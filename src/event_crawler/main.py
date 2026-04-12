from __future__ import annotations

import argparse
import asyncio
import json
import re
from pathlib import Path
from typing import Any

from camoufox.async_api import AsyncCamoufox

from event_crawler.crawler_base import BaseCrawler, CrawlerResult
from event_crawler.euroring_crawler import EuroringCrawler
from event_crawler.m_ring_crawler import MRingCrawler

from playwright.async_api import TimeoutError as PlaywrightTimeoutError

CRAWL_RETRIES = 3

class CrawlOrchestrator:
    """Coordinate all crawler executions and write the merged JSON output."""

    def __init__(self, output: Path, crawler_filter: str | None = None) -> None:
        self.output = output
        self._crawler_filter = re.compile(crawler_filter) if crawler_filter else None
        self._crawlers = self._build_crawler_registry()
        self._page_creation_lock = asyncio.Lock()

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
                async with self._page_creation_lock:
                    print(
                        f"[{crawler_id}] Creating browser context, attempt {i}/{CRAWL_RETRIES}..."
                    )
                    context = await browser.new_context()
                    # Camoufox can hang intermittently on concurrent page creation.
                    # Serialize context/page startup while keeping crawl work parallel.
                    print(f"[{crawler_id}] Creating page, attempt {i}/{CRAWL_RETRIES}...")
                    page = await context.new_page()
                    print(f"[{crawler_id}] Releasing startup lock.")

                print(f"[{crawler_id}] Starting crawl, attempt {i}/{CRAWL_RETRIES}...")
                result = await crawler.crawl(page)
                print(f"[{crawler_id}] Finished crawl, found {len(result)} items.")
                return crawler_id, result
            except Exception as exc:
                print(
                    f"[{crawler_id}] Crawl attempt {i}/{CRAWL_RETRIES} failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                if isinstance(exc, PlaywrightTimeoutError) and i < CRAWL_RETRIES:
                    print(f"[{crawler_id}] Got timeout, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(1)
                else:
                    raise
            finally:
                if context is not None:
                    print(
                        f"[{crawler_id}] Closing browser context, attempt {i}/{CRAWL_RETRIES}..."
                    )
                    await context.close()

        raise RuntimeError(f"[{crawler_id}] Crawl exited retry loop unexpectedly.")

    async def run(self) -> None:
        """Run both source crawlers and persist a flattened output payload."""
        selected_ids = self.selected_crawler_ids()
        if not selected_ids:
            self.output.write_text(json.dumps([]), encoding="utf-8")
            return

        async with AsyncCamoufox(headless=True) as browser:
            results = await asyncio.gather(
                *[self._run_single_crawler(browser, crawler_id) for crawler_id in selected_ids],
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

        self.output.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def main() -> None:
    """CLI entrypoint for running the crawler orchestrator."""
    available_ids = CrawlOrchestrator.available_crawler_ids()

    parser = argparse.ArgumentParser(description="Run event crawlers and write JSON output.")
    parser.add_argument(
        "--output",
        default="track_events.json",
        help="File path for output JSON (default: crawled/track_events.json).",
    )
    parser.add_argument(
        "--crawler-filter",
        default=".*",
        help=(
            "Regex to select crawler IDs for this run. "
            f"Available IDs: {', '.join(available_ids)}"
        ),
    )
    args = parser.parse_args()

    try:
        re.compile(args.crawler_filter)
    except re.error as exc:
        parser.error(f"Invalid --crawler-filter regex: {exc}")

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    orchestrator = CrawlOrchestrator(output=output, crawler_filter=args.crawler_filter)

    asyncio.run(orchestrator.run())


if __name__ == "__main__":
    main()
