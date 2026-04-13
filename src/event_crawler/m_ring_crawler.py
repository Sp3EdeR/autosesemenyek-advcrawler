from __future__ import annotations

from playwright.async_api import Page

from event_crawler.crawler_base import BaseCrawler, CrawlerResult


class MRingCrawler(BaseCrawler[CrawlerResult]):
    """Crawler implementation for extracting M-Ring event dates."""

    crawler_id = "m-ring"
    url = "https://m-ring.hu/"

    @property
    def next_selectors(self) -> list[str]:
        return [".nav-link-next"]

    @property
    def page_content_selectors(self) -> list[str]:
        return [ ".jet-calendar" ]

    async def wait_until_ready(self, page: Page) -> None:
        await page.wait_for_selector(".jet-calendar .jet-calendar-week__day-date", timeout=15000)
        await page.wait_for_timeout(1000)

    async def is_page_empty(self, page: Page) -> bool:
        """Check whether the currently displayed calendar month has any events."""
        has_event_cells = await page.locator("td.has-events").count() > 0
        has_event_like_nodes = await page.locator(
            ".jet-calendar-week__event, .jet-calendar-list__event, .has-events"
        ).count() > 0
        return not (has_event_cells or has_event_like_nodes)

    def build_initial_result(self) -> CrawlerResult:
        return []

    def append_page_data(self, aggregate: CrawlerResult, page_data: CrawlerResult) -> None:
        """Merge one calendar page worth of rows into aggregate."""
        aggregate.extend(page_data)

    async def extract_page_data(self, page: Page) -> CrawlerResult:
        """Extract M-Ring trackday and race dates from the active month."""
        caption = ""
        caption_candidates = [
            ".jet-calendar-caption__name",
            ".jet-calendar-nav__month",
            ".jet-calendar-header__month-name",
            ".jet-calendar__month-name",
        ]
        for selector in caption_candidates:
            locator = page.locator(selector)
            if await locator.count() > 0:
                text = self._collapse_whitespace(await locator.first.inner_text())
                if text:
                    caption = text
                    break

        race_rows: list[str] = []
        trackday_rows: list[str] = []

        day_cells = page.locator("td.jet-calendar-week__day:not(.day-pad)")
        for idx in range(await day_cells.count()):
            cell = day_cells.nth(idx)
            day_node = cell.locator(".jet-calendar-week__day-date")
            if await day_node.count() == 0:
                continue

            day_text = self._collapse_whitespace(await day_node.first.inner_text())
            if not day_text.isdigit():
                continue

            day = int(day_text)
            iso_date = self._extract_iso_date_from_caption(caption, day)
            if not iso_date:
                continue

            headings = await cell.locator("h2").all_inner_texts()
            for heading_text in headings:
                summary = self._collapse_whitespace(heading_text)
                if not summary:
                    continue

                summary_norm = self._normalize_text_for_match(summary.strip())

                if "nyilt" in summary_norm and "auto" in summary_norm:
                    trackday_rows.append(iso_date)

                if (
                    "m-ring cup" in summary_norm
                    or "m ring cup" in summary_norm
                    or "m-ring kupa" in summary_norm
                    or "m ring kupa" in summary_norm
                ):
                    race_rows.append(iso_date)

        page_rows: CrawlerResult = []
        page_rows.extend({"trackday": value} for value in self._dedupe(trackday_rows))
        page_rows.extend({"race": value} for value in self._dedupe(race_rows))
        return page_rows
