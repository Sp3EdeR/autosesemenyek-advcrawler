from __future__ import annotations

import json

from playwright.async_api import Page

from event_crawler.crawler_base import CrawlerBase, ParserBase


class EuroringCrawler(CrawlerBase):
    """Crawler implementation for extracting Euroring event dates."""

    id = "euroring"
    url = "https://euroring.hu/esemenynaptar2/"

    @property
    def next_selectors(self) -> list[str]:
        return [".mec-next-month > a.mec-load-month-link"]

    @property
    def page_content_selectors(self) -> list[str]:
        return [ ".mec-calendar" ]

    async def is_page_empty(self, page: Page) -> bool:
        """Check whether the visible Euroring month container has event markers."""
        month_nodes = page.locator(".mec-calendar-table .mec-month-container")
        month_count = await month_nodes.count()
        if month_count == 0:
            return True

        active_month = []
        for idx in range(month_count):
            node = month_nodes.nth(idx)
            style = ((await node.get_attribute("style")) or "").replace(" ", "").lower()
            if "display:none" not in style:
                active_month.append(node)

        if len(active_month) != 1:
            active_month = [month_nodes.nth(month_count - 1)]

        event_markers = await active_month[0].locator(
            ".mec-calendar-day script[type='application/ld+json'], "
            ".event-single-link-novel, "
            ".mec-single-event-novel"
        ).count() > 0
        return not event_markers

    def finalize_result(self, aggregate: ParserBase.Result) -> ParserBase.Result:
        # Deduplicate by full row value
        return [row for idx, row in enumerate(aggregate) if row not in aggregate[:idx]]

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        """Parse JSON-LD event entries and map them to known Euroring categories."""
        car_rows: list[str] = []
        motor_rows: list[str] = []
        trackangel_rows: list[str] = []

        scripts = page.locator(".mec-calendar-day script")
        for script_idx in range(await scripts.count()):
            script = scripts.nth(script_idx)
            payload = ((await script.text_content()) or "").strip()
            if not payload:
                continue

            try:
                data = json.loads(payload)
            except Exception:
                continue

            if not isinstance(data, dict):
                continue

            start_date = str(data.get("startDate", "")).strip()
            event_url = self._normalize_text_for_match(str(data.get("url", "")).strip())

            if "autos-nyiltnap" in event_url:
                car_rows.append(start_date)
            elif "motoros-nyiltnap" in event_url:
                motor_rows.append(start_date)
            elif "track-angel" in event_url:
                trackangel_rows.append(start_date)

        page_rows: ParserBase.Result = []
        page_rows.extend({"trackday": value} for value in self._dedupe(car_rows))
        page_rows.extend({"motor_trackday": value} for value in self._dedupe(motor_rows))
        page_rows.extend({"trackangel_trackday": value} for value in self._dedupe(trackangel_rows))
        return page_rows
