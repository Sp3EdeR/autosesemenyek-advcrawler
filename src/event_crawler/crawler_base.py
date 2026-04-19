from __future__ import annotations

import hashlib
from abc import ABC, abstractmethod
from typing import Annotated, Any, ClassVar

from playwright.async_api import Locator, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from event_crawler.parser_base import ParserBase

CONTENT_TIMEOUT_MS = 15000
PAGE_RETRIES = 2

class CrawlerBase(ABC, ParserBase):
    """Abstract base class for calendar event crawlers."""

    url: Annotated[ClassVar[str],
        "URL of the crawler's target page. Override in each subclass."]

    max_pages: Annotated[ClassVar[int],
        "Maximum number of pages to crawl. Override in a subclass if needed."] = 30

    _registry: Annotated[ClassVar[dict[str, type[CrawlerBase]]], # pyright: ignore[reportIncompatibleVariableOverride]
        "Registry of all crawler implementations. Automatically populated."] = {}

    async def wait_until_ready(self, page: Page) -> None:
        """Wait until the page is ready for interaction after navigation or pagination.

        By default, this waits for the page's ``DOMContentLoaded`` load state. Override if needed.
        """
        await page.wait_for_load_state("domcontentloaded", timeout=CONTENT_TIMEOUT_MS)

    async def is_page_empty(self, page: Page) -> bool:
        """Returns whether the currently visible calendar page has no events.
        
        When returning True, the crawler will stop loading new pages. Override to stop on empty
        pages, and when the next button is always active.
        """
        return False

    @property
    @abstractmethod
    def next_selectors(self) -> list[str]:
        """Return a list of CSS selectors to find the next-page control, in order of preference."""
        raise NotImplementedError

    @property
    @abstractmethod
    def page_content_selectors(self) -> list[str]:
        """Return a list of CSS selectors whose content should be used to detect page changes."""
        raise NotImplementedError

    @abstractmethod
    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        """Extract event data from the current page and return it as a list of dicts."""
        raise NotImplementedError

    def finalize_result(self, aggregate: ParserBase.Result) -> ParserBase.Result:
        """Finalize aggregate data before returning it from crawl()."""
        return aggregate

    async def crawl(self, page: Page) -> ParserBase.Result:
        """Run the full crawl loop: load, extract, paginate, and aggregate."""
        print(f"[{self.id}] Navigating to {type(self).url}...")
        await page.goto(type(self).url, wait_until="commit", timeout=CONTENT_TIMEOUT_MS)
        print(f"[{self.id}] Page committed, waiting for it to be ready...")
        await self.wait_until_ready(page)
        print(f"[{self.id}] Initial page marked ready.")
        aggregate = []
        page_number = 1
        for _ in range(type(self).max_pages):
            print(f"[{self.id}] Processing page {page_number}...")
            if await self.is_page_empty(page):
                print(f"[{self.id}] Page {page_number} is empty; stopping crawl.")
                break

            page_data = await self.extract_page_data(page)
            aggregate.extend(page_data)

            moved = await self._activate_next_page(page, preferred_selectors=self.next_selectors)
            if not moved:
                print(f"[{self.id}] No further pages detected after page {page_number}.")
                break

            page_number += 1
            print(f"[{self.id}] Loaded page {page_number}.")

        return self.finalize_result(aggregate)

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
        """Find the first visible and enabled next-page control.

        Args:
            page: Playwright page instance to operate on.
            preferred_selectors: List of CSS selectors to find the next-page control, in order of
                preference.

        Returns:
            Locator for the next-page control, or None if not found or not enabled.
        """
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

    async def _activate_next_page(
            self,
            page: Page,
            preferred_selectors: list[str],
            click_options: dict[str, Any] | None = None,
    ) -> bool:
        """Click next and wait until the calendar signature changes.

        Args:
            page: Playwright page instance to operate on.
            preferred_selectors: List of CSS selectors to find the next-page control.
            click_options: Locator.click arguments to customize the click behavior.

        Returns:
            True if the next-page control was clicked and the calendar content changed;
            False if no visible, enabled next-page control was found.
        """
        next_button = await self._find_next_page_activator(page, preferred_selectors)
        if not next_button:
            print(f"[{self.id}] Next-page control not found or not enabled.")
            return False

        signature_before = await self._get_calendar_signature(page)
        print(f"[{self.id}] Clicking next-page control...")
        await next_button.click(**(click_options or {}))

        check_interval_ms = 100
        for _ in range(int(CONTENT_TIMEOUT_MS / check_interval_ms)):
            await page.wait_for_timeout(check_interval_ms)
            signature_after = await self._get_calendar_signature(page)
            if signature_after != signature_before:
                return True

        print(f"[{self.id}] Timed out waiting for next page content to change.")
        raise PlaywrightTimeoutError(f"Next page did not load in time for {self.id}.")

class SinglePageCrawlerBase(CrawlerBase):
    """Base crawler for single-page calendars that don't require pagination."""

    @property
    def next_selectors(self) -> list[str]:
        return []

    @property
    def page_content_selectors(self) -> list[str]:
        return []
