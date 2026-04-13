from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from abc import ABC, abstractmethod
from datetime import date
from typing import Any, Generic, TypeVar

from playwright.async_api import Locator, Page, TimeoutError as PlaywrightTimeoutError

TPageData = TypeVar("TPageData")
CrawlerRow = dict[str, Any]
CrawlerResult = list[CrawlerRow]

HUNGARIAN_MONTHS = {
    "januar": 1,
    "februar": 2,
    "marcius": 3,
    "aprilis": 4,
    "majus": 5,
    "junius": 6,
    "julius": 7,
    "augusztus": 8,
    "szeptember": 9,
    "oktober": 10,
    "november": 11,
    "december": 12,
}

CONTENT_TIMEOUT_MS = 15000
PAGE_RETRIES = 2

class BaseCrawler(ABC, Generic[TPageData]):
    """Abstract base class for calendar event crawlers."""

    def __init__(self, url: str, crawler_id: str) -> None:
        self.url = url
        self.crawler_id = crawler_id
        self.max_pages = 30

    @property
    @abstractmethod
    def next_selectors(self) -> list[str]:
        raise NotImplementedError

    @property
    @abstractmethod
    def page_content_selectors(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    async def wait_until_ready(self, page: Page) -> None:
        raise NotImplementedError

    @abstractmethod
    async def is_page_empty(self, page: Page) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def extract_page_data(self, page: Page) -> TPageData:
        raise NotImplementedError

    @abstractmethod
    def build_initial_result(self) -> CrawlerResult:
        raise NotImplementedError

    @abstractmethod
    def append_page_data(self, aggregate: CrawlerResult, page_data: TPageData) -> None:
        raise NotImplementedError

    def finalize_result(self, aggregate: CrawlerResult) -> CrawlerResult:
        """Finalize aggregate data before returning it from crawl()."""
        return aggregate

    async def crawl(self, page: Page) -> CrawlerResult:
        """Run the full crawl loop: load, extract, paginate, and aggregate."""
        print(f"[{self.crawler_id}] Navigating to {self.url}...")
        await page.goto(self.url, wait_until="commit", timeout=CONTENT_TIMEOUT_MS)
        print(f"[{self.crawler_id}] Page committed, waiting for it to be ready...")
        await self.wait_until_ready(page)
        print(f"[{self.crawler_id}] Initial page marked ready.")
        aggregate = self.build_initial_result()
        page_number = 1
        for _ in range(self.max_pages):
            print(f"[{self.crawler_id}] Processing page {page_number}...")
            if await self.is_page_empty(page):
                print(f"[{self.crawler_id}] Page {page_number} is empty; stopping crawl.")
                break

            page_data = await self.extract_page_data(page)
            self.append_page_data(aggregate, page_data)

            moved = await self._activate_next_page(page, preferred_selectors=self.next_selectors)
            if not moved:
                print(f"[{self.crawler_id}] No further pages detected after page {page_number}.")
                break

            page_number += 1
            print(f"[{self.crawler_id}] Loaded page {page_number}.")

        return self.finalize_result(aggregate)

    @staticmethod
    def _normalize_text_for_match(text: str) -> str:
        lowered = text.casefold()
        normalized = unicodedata.normalize("NFD", lowered)
        return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")

    @staticmethod
    def _collapse_whitespace(text: str) -> str:
        return " ".join((text or "").replace("\xa0", " ").split())

    @staticmethod
    def _extract_iso_date_from_caption(caption: str, day: int) -> str | None:
        """Extract an ISO date from a month caption and day number."""
        caption_norm = BaseCrawler._normalize_text_for_match(caption)
        month = None
        for month_name, month_number in HUNGARIAN_MONTHS.items():
            if month_name in caption_norm:
                month = month_number
                break

        if month is None:
            return None

        year_match = re.search(r"\b(\d{4})\b", caption_norm)
        if not year_match:
            return None

        year = int(year_match.group(1))
        try:
            return date(year, month, day).isoformat()
        except ValueError:
            return None

    @staticmethod
    def _dedupe(rows: list[str]) -> list[str]:
        """Return rows in original order with duplicates removed."""
        seen: set[str] = set()
        unique_rows: list[str] = []
        for row in rows:
            marker = json.dumps(row, sort_keys=True, ensure_ascii=False)
            if marker in seen:
                continue
            seen.add(marker)
            unique_rows.append(row)
        return unique_rows

    async def _get_calendar_signature(self, page: Page) -> str:
        """Build a stable signature hash from crawler-specific selector content."""
        signature_parts: list[str] = []
        for selector in self.page_content_selectors:
            locator = page.locator(selector)
            count = await locator.count()
            for idx in range(count):
                text = self._collapse_whitespace(await locator.nth(idx).inner_text())[:2000]
                signature_parts.append(text)

        joined = "".join(signature_parts)
        return hashlib.sha256(joined.encode("utf-8")).hexdigest()

    async def _find_next_page_activator(
        self,
        page: Page,
        preferred_selectors: list[str],
    ) -> Locator | None:
        """Find the first visible and enabled next-page control."""
        for selector in reversed(preferred_selectors):
            locator = page.locator(selector)
            buttons = await locator.all()
            for target in buttons:
                try:
                    if await target.is_visible() and await target.is_enabled():
                        return target
                except Exception:
                    continue

        return None

    async def _activate_next_page(self, page: Page, preferred_selectors: list[str]) -> bool:
        """Click next and wait until the calendar signature changes."""
        next_button = await self._find_next_page_activator(page, preferred_selectors)
        if not next_button:
            print(f"[{self.crawler_id}] Next-page control not found or not enabled.")
            return False

        signature_before = await self._get_calendar_signature(page)
        print(f"[{self.crawler_id}] Clicking next-page control...")
        await next_button.click()

        check_interval_ms = 100
        for _ in range(int(CONTENT_TIMEOUT_MS / check_interval_ms)):
            await page.wait_for_timeout(check_interval_ms)
            signature_after = await self._get_calendar_signature(page)
            if signature_after != signature_before:
                return True

        print(f"[{self.crawler_id}] Timed out waiting for next page content to change.")
        raise PlaywrightTimeoutError(f"Next page did not load in time for {self.crawler_id}.")
