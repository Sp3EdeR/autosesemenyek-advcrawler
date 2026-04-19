from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import os
import pkgutil
import re
import sys
import traceback
from collections.abc import Callable
from pathlib import Path
from typing import Annotated, Any, ClassVar, cast

from camoufox.async_api import AsyncCamoufox
from playwright.async_api import Error as PlaywrightError
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from event_crawler.crawler_base import CrawlerBase
from event_crawler.downloader_base import DownloaderBase
from event_crawler.parser_base import ParserBase

CRAWL_RETRIES = 3
RETRY_TIMEOUT_S = 1
CLOSE_TIMEOUT_S = 5


class CrawlOrchestrator:
    """Coordinate all crawler executions and write the merged JSON output."""

    class _RegistryBootstrap:
        @staticmethod
        def import_modules(suffix: str) -> None:
            """Import all modules in the current package that end with the given suffix."""
            package_name = __package__ or "event_crawler"
            package_dir = Path(__file__).resolve().parent
            for module_info in pkgutil.iter_modules([str(package_dir)]):
                module_name = module_info.name
                if module_name.endswith(suffix):
                    importlib.import_module(f"{package_name}.{module_name}")

        @classmethod
        def build_crawler_registry(cls) -> dict[str, CrawlerBase]:
            """Import all crawler modules and build the crawler registry."""
            cls.import_modules("_crawler")
            return {
                parser_id: cast(Callable[[], CrawlerBase], crawler_cls)()
                for parser_id, crawler_cls in sorted(CrawlerBase.get_registry().items())
            }

        @classmethod
        def build_downloader_registry(cls) -> dict[str, DownloaderBase]:
            """Import all downloader modules and build the downloader registry."""
            cls.import_modules("_downloader")
            return {
                parser_id: cast(Callable[[], DownloaderBase], downloader_cls)()
                for parser_id, downloader_cls in sorted(DownloaderBase.get_registry().items())
            }

    _crawler_registry: Annotated[
        ClassVar[dict[str, CrawlerBase]],
        "Registry of all available crawler implementations, keyed by their 'id' attribute."
    ] = _RegistryBootstrap.build_crawler_registry()
    _downloader_registry: Annotated[
        ClassVar[dict[str, DownloaderBase]],
        "Registry of all available downloader implementations, keyed by their 'id' attribute."
    ] = _RegistryBootstrap.build_downloader_registry()
    del _RegistryBootstrap

    available_ids: ClassVar[list[str]] = sorted(
        [*_crawler_registry.keys(), *_downloader_registry.keys()]
    )

    @property
    def _crawlers(self) -> dict[str, CrawlerBase]:
        return type(self)._crawler_registry

    @property
    def _downloaders(self) -> dict[str, DownloaderBase]:
        return type(self)._downloader_registry

    def __init__(
        self,
        crawler_filter: str | None = None,
        jobs: int = 4,
        headless: bool = True,
    ) -> None:
        self._crawler_filter = re.compile(crawler_filter or ".*")
        self._jobs = jobs
        self._headless = headless

    async def _run_single_crawler(self, browser: Any, id: str) -> tuple[str, ParserBase.Result]:
        crawler = self._crawlers[id]

        for i in range(1, CRAWL_RETRIES + 1):
            context = None
            try:
                print(f"[{id}] Creating browser context, attempt {i}/{CRAWL_RETRIES}...")
                context = await browser.new_context()
                print(f"[{id}] Creating page, attempt {i}/{CRAWL_RETRIES}...")
                page = await context.new_page()

                print(f"[{id}] Starting crawl, attempt {i}/{CRAWL_RETRIES}...")
                result = await crawler.crawl(page)
                print(f"[{id}] Finished crawl, found {len(result)} items.")
                return id, result
            except Exception as exc:
                # The crawler ran into an exception. If the exception marks a recoverable error,
                # retry the crawl. Otherwise, propagate the exception to terminate the process.
                print(
                    f"[{id}] Crawl attempt {i}/{CRAWL_RETRIES} failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                if CRAWL_RETRIES <= i:
                    raise

                if isinstance(exc, PlaywrightTimeoutError):
                    print(f"[{id}] Got timeout, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(RETRY_TIMEOUT_S)
                elif isinstance(exc, PlaywrightError) and "NS_ERROR_ABORT" in exc.message:
                    print(f"[{id}] Failed to load page, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(RETRY_TIMEOUT_S)
                else:
                    raise
            finally:
                if context is not None:
                    print(
                        f"[{id}] Closing browser context, attempt {i}/{CRAWL_RETRIES}..."
                    )
                    try:
                        await asyncio.wait_for(context.close(), timeout=CLOSE_TIMEOUT_S)
                    except TimeoutError as e:
                        raise TimeoutError(f"[{id}] Timeout while trying to close context.") from e

        raise RuntimeError(f"[{id}] Crawl exited retry loop unexpectedly.")
    
    async def _run_single_downloader(self, id: str) -> tuple[str, ParserBase.Result]:
        print(f"[{id}] Starting downloader...")
        downloader = self._downloaders[id]
        content_bin: bytes = b""
        for i in range(1, CRAWL_RETRIES + 1):
            try:
                content_bin = await downloader.download()
                break
            except Exception as exc:
                print(
                    f"[{id}] Download attempt {i}/{CRAWL_RETRIES} failed: "
                    f"{type(exc).__name__}: {exc}"
                )
                if CRAWL_RETRIES <= i:
                    raise

                if isinstance(exc, DownloaderBase.Error) and exc.is_recoverable:
                    print(f"[{id}] Recoverable error, attempt {i}/{CRAWL_RETRIES}, retrying...")
                    await asyncio.sleep(RETRY_TIMEOUT_S)
                else:
                    raise

        content = downloader.decode_content(content_bin)
        print(f"[{id}] Extracting data...")
        result = await downloader.extract_data(content)
        return id, result

    async def run(self) -> ParserBase.Result:
        """Run source crawlers and persist a flattened output payload."""
        selected_crawler_ids = [
            id for id in self._crawlers.keys() if self._crawler_filter.search(id)
        ]
        selected_downloader_ids = [
            id for id in self._downloaders.keys() if self._crawler_filter.search(id)
        ]
        if not [*selected_crawler_ids, *selected_downloader_ids]:
            print("No crawlers selected with the given filter.")
            return []

        if self._jobs == 1:
            results: list[tuple[str, ParserBase.Result]] = []
            if selected_downloader_ids:
                for id in selected_downloader_ids:
                    results.append(await self._run_single_downloader(id))
            if selected_crawler_ids:
                print("Spawning single browser instance...")
                async with AsyncCamoufox(headless=self._headless) as browser:
                    for id in selected_crawler_ids:
                        results.append(await self._run_single_crawler(browser, id))
        else:
            # WARN: Camoufox is not reentrant. Multiple browser instances must be created to run
            # crawlers concurrently. Do not try to share a single browser instance with multiple
            # pages, as it will lead to sporadic problems.
            semaphore = asyncio.Semaphore(self._jobs)

            async def crawl_one_async(id: str) -> tuple[str, ParserBase.Result]:
                async with semaphore:
                    try:
                        print(f"[{id}] Spawning browser instance...")
                        browser_cm = AsyncCamoufox(headless=self._headless)
                        browser = await browser_cm.__aenter__()
                        result = await self._run_single_crawler(browser, id)
                        # Don't try to clean Camoufox up on error, just kill the process.
                        await browser_cm.__aexit__(None, None, None)
                        print(f"[{id}] Finished successfully.")
                        return result
                    except BaseException as exc:
                        # The browser connection may have become unstable, so don't try to exit
                        # cleanly. Just terminate the process.
                        print(f"[{id}] Uncaught exception, terminating process.")
                        traceback.print_exception(type(exc), exc, exc.__traceback__)
                        sys.stdout.flush()
                        sys.stderr.flush()
                        os._exit(1)

            async def download_one_async(id: str) -> tuple[str, ParserBase.Result]:
                async with semaphore:
                    try:
                        result = await self._run_single_downloader(id)
                        print(f"[{id}] Finished successfully.")
                        return result
                    except BaseException as exc:
                        print(f"[{id}] Uncaught exception in downloader, terminating process.")
                        traceback.print_exception(type(exc), exc, exc.__traceback__)
                        sys.stdout.flush()
                        sys.stderr.flush()
                        os._exit(1)

            results = await asyncio.gather(
                *[crawl_one_async(id) for id in selected_crawler_ids],
                *[download_one_async(id) for id in selected_downloader_ids],
            )

        # Merge results, and ensure stable sorted output
        payload: ParserBase.Result = []
        for id, crawler_rows in results:
            for row in crawler_rows:
                prefixed_row = {f"{id}_{key}": value for key, value in sorted(
                    row.items(), key=lambda item: item[0]
                )}
                payload.append(prefixed_row)

        payload.sort(key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False, default=str))
        return payload

def main() -> None:
    """CLI entrypoint for running the crawler orchestrator."""
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
            f"Available IDs: {', '.join(CrawlOrchestrator.available_ids)}"
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
        help="Maximum number of crawler jobs to run in parallel. Use 1 for sequential mode. "
             "(default: 4).",
    )
    def parse_bool(value: str) -> bool:
        normalized = value.strip().lower()
        if normalized in {"1", "true", "t", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "f", "no", "n", "off"}:
            return False
        raise argparse.ArgumentTypeError(
            "expected a truthy or falsy value: true/false, yes/no, on/off, 1/0"
        )
    parser.add_argument(
        "--headless",
        type=parse_bool,
        default=True,
        help="Run Camoufox headless. Accepts true/false. (default: true).",
    )
    args = parser.parse_args()

    try:
        re.compile(args.crawler_filter)
    except re.error as exc:
        parser.error(f"Invalid --crawler-filter regex: {exc}")


    orchestrator = CrawlOrchestrator(
        crawler_filter=args.crawler_filter,
        jobs=args.jobs,
        headless=args.headless,
    )

    result = asyncio.run(orchestrator.run())

    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, sort_keys=True), encoding="utf-8")


if __name__ == "__main__":
    main()
