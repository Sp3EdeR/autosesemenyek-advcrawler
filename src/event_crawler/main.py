from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import os
import traceback
from pathlib import Path
from typing import Any

from camoufox.async_api import AsyncCamoufox

from event_crawler.crawler_base import BaseCrawler, CrawlerResult
from event_crawler.euroring_crawler import EuroringCrawler
from event_crawler.m_ring_crawler import MRingCrawler

from playwright.async_api import Error as PlaywrightError, TimeoutError as PlaywrightTimeoutError

CRAWL_RETRIES = 3
RETRY_TIMEOUT_S = 1
CLOSE_TIMEOUT_S = 5

class CrawlOrchestrator:
    """Coordinate all crawler executions and write the merged JSON output."""

    def __init__(self, crawler_filter: str | None = None, jobs: int = 4) -> None:
        self._crawler_filter = re.compile(crawler_filter) if crawler_filter else None
        self._jobs = jobs
        self._crawlers = self._build_crawler_registry()

    @staticmethod
    def _build_crawler_registry() -> dict[str, BaseCrawler[Any]]:
        return {
            "m-ring": MRingCrawler(),
            "euroring": EuroringCrawler(),
        }

    @classmethod
    def available_crawler_ids(cls) -> list[str]:
        return sorted(cls._build_crawler_registry().keys())

    def selected_crawler_ids(self) -> list[str]:
        ids = self.available_crawler_ids()
        if not self._crawler_filter:
            return ids
        return [crawler_id for crawler_id in ids if self._crawler_filter.search(crawler_id)]

    async def _run_single_crawler(self, browser: Any, crawler_id: str) -> tuple[str, CrawlerResult]:
        crawler = self._crawlers[crawler_id]

        for i in range(1, CRAWL_RETRIES + 1):
            context = None
            try:
                print(f"[{crawler_id}] Creating browser context, attempt {i}/{CRAWL_RETRIES}...")
                context = await browser.new_context()
                print(f"[{crawler_id}] Creating page, attempt {i}/{CRAWL_RETRIES}...")
                page = await context.new_page()

                print(f"[{crawler_id}] Starting crawl, attempt {i}/{CRAWL_RETRIES}...")
                result = await crawler.crawl(page)
                print(f"[{crawler_id}] Finished crawl, found {len(result)} items.")
                return crawler_id, result
            except Exception as exc:
                # The crawler ran into an exception. If the exception marks a recoverable error,
                # retry the crawl. Otherwise, propagate the exception to terminate the process.
                print(
                    f"[{crawler_id}] Crawl attempt {i}/{CRAWL_RETRIES} failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                if CRAWL_RETRIES <= i:
                    raise

                if isinstance(exc, PlaywrightTimeoutError):
                    print(f"[{crawler_id}] Got timeout, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(RETRY_TIMEOUT_S)
                elif isinstance(exc, PlaywrightError) and "NS_ERROR_ABORT" in exc.message:
                    print(f"[{crawler_id}] Failed to load page, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(RETRY_TIMEOUT_S)
                else:
                    raise
            finally:
                if context is not None:
                    print(
                        f"[{crawler_id}] Closing browser context, attempt {i}/{CRAWL_RETRIES}..."
                    )
                    try:
                        await asyncio.wait_for(context.close(), timeout=CLOSE_TIMEOUT_S)
                    except asyncio.TimeoutError:
                        raise TimeoutError(f"[{crawler_id}] Timeout while trying to close context.")

        raise RuntimeError(f"[{crawler_id}] Crawl exited retry loop unexpectedly.")

    async def run(self) -> CrawlerResult:
        """Run both source crawlers and persist a flattened output payload."""
        selected_ids = self.selected_crawler_ids()
        if not selected_ids:
            return []

        if self._jobs == 1:
            print("Spawning single browser instance...")
            async with AsyncCamoufox(headless=True) as browser:
                results: list[tuple[str, CrawlerResult]] = []
                for crawler_id in selected_ids:
                    results.append(await self._run_single_crawler(browser, crawler_id))
        else:
            # WARN: Camoufox is not reentrant. Multiple browser instances must be created to run
            # crawlers concurrently. Do not try to share a single browser instance with multiple
            # pages, as it will lead to sporadic problems.
            semaphore = asyncio.Semaphore(self._jobs)

            async def crawl_one_async(crawler_id: str) -> tuple[str, CrawlerResult]:
                async with semaphore:
                    try:
                        print(f"[{crawler_id}] Spawning browser instance...")
                        browser_cm = AsyncCamoufox(headless=True)
                        browser = await browser_cm.__aenter__()
                        result = await self._run_single_crawler(browser, crawler_id)
                        # Don't try to clean Camoufox up on error, just kill the process.
                        await browser_cm.__aexit__(None, None, None)
                        print(f"[{crawler_id}] Finished successfully.")
                        return result
                    except BaseException as exc:
                        # The browser connection may have become unstable, so don't try to exit
                        # cleanly. Just terminate the process.
                        print(f"[{crawler_id}] Uncaught exception, terminating process.")
                        traceback.print_exception(type(exc), exc, exc.__traceback__)
                        sys.stdout.flush()
                        sys.stderr.flush()
                        os._exit(1)

            results = await asyncio.gather(
                *[crawl_one_async(crawler_id) for crawler_id in selected_ids]
            )

        # Merge results, and ensure stable sorted output
        payload: CrawlerResult = []
        for crawler_id, crawler_rows in results:
            for row in crawler_rows:
                prefixed_row = { f"{crawler_id}_{key}": value for key, value in sorted(
                        row.items(), key=lambda item: item[0]
                ) }
                payload.append(prefixed_row)

        payload.sort(key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False, default=str))
        return payload

def main() -> None:
    """CLI entrypoint for running the crawler orchestrator."""
    available_ids = CrawlOrchestrator.available_crawler_ids()

    parser = argparse.ArgumentParser(description="Run event crawlers and write JSON output.")
    parser.add_argument(
        "--output",
        default="crawled.json",
        help="File path for output JSON (default: crawled.json).",
    )
    parser.add_argument(
        "--crawler-filter",
        default=".*",
        help=(
            "Regex to select crawler IDs for this run. "
            f"Available IDs: {', '.join(available_ids)}"
        ),
    )
    def parse_jobs(value: str) -> int:
        try:
            jobs = int(value)
        except ValueError as exc:
            raise argparse.ArgumentTypeError("jobs must be a positive integer") from exc
        if jobs <= 0:
            raise argparse.ArgumentTypeError("jobs must be a positive integer")
        return jobs
    parser.add_argument(
        "-j",
        "--jobs",
        type=parse_jobs,
        default=4,
        help="Maximum number of crawler jobs to run in parallel. Use 1 for sequential mode. (default: 4).",
    )
    args = parser.parse_args()

    try:
        re.compile(args.crawler_filter)
    except re.error as exc:
        parser.error(f"Invalid --crawler-filter regex: {exc}")


    orchestrator = CrawlOrchestrator(crawler_filter=args.crawler_filter, jobs=args.jobs)

    result = asyncio.run(orchestrator.run())

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
