from __future__ import annotations

import calendar
import re
from datetime import date, datetime, time
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from playwright.async_api import Page

from event_crawler.crawler_base import CONTENT_TIMEOUT_MS, CrawlerBase, ParserBase
from event_crawler.parser_base import HUNGARIAN_MONTHS


class CsajokCrawler(CrawlerBase):
    """Extract the AJAX event listing from Csajok a Motoron."""

    id = "csajok"
    url = "https://csajokamotoron.hu/motoros-esemenynaptar/"

    _VIEW_SELECTOR = "#em-view-1"
    _LIST_SELECTOR = "#em-events-list-1"
    _TIMEZONE = ZoneInfo("Europe/Budapest")

    def __init__(self) -> None:
        now = datetime.now(self._TIMEZONE)
        next_year = now.year + 1
        day = min(now.day, calendar.monthrange(next_year, now.month)[1])
        self._cutoff = now.replace(year=next_year, day=day)
        self._reached_cutoff = False

    @property
    def next_selectors(self) -> list[str]:
        if self._reached_cutoff:
            return []
        return [f"{self._VIEW_SELECTOR} .em-pagination a.next"]

    @property
    def page_content_selectors(self) -> list[str]:
        return [f"{self._VIEW_SELECTOR} .em-pagination .current"]

    async def wait_until_ready(self, page: Page) -> None:
        await super().wait_until_ready(page)
        await page.add_style_tag(content="#ez-cmpv2-container { display: none !important; }")
        await page.locator(self._LIST_SELECTOR).wait_for(
            state="attached", timeout=CONTENT_TIMEOUT_MS
        )

    async def is_page_empty(self, page: Page) -> bool:
        return await page.locator(f"{self._LIST_SELECTOR} > .em-event").count() == 0

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        rows: ParserBase.Result = []
        items = page.locator(f"{self._LIST_SELECTOR} > .em-event")
        for item in await items.all():
            start, end = self._parse_dates(
                await item.locator(".em-event-date").inner_text(),
                await item.locator(".em-event-time").inner_text(),
            )
            if (
                self._cutoff < start if isinstance(start, datetime)
                else self._cutoff.date() < start
            ):
                self._reached_cutoff = True
                break

            title = item.locator(".em-item-title a")
            summary = self._collapse_whitespace(await title.inner_text())
            href = await title.get_attribute("href")
            if not summary or not href:
                raise ValueError(f"[{self.id}] Event title or details URL is missing.")

            event: ParserBase.Row = {
                "summary": summary,
                "dtstart": start.isoformat(),
                "url": urljoin(self.url, href),
            }
            if end is not None:
                event["dtend"] = end.isoformat()
            for field, selector in (
                ("location", ".em-event-location"),
                ("description", ".em-item-desc"),
            ):
                element = item.locator(selector)
                if await element.count():
                    text = self._collapse_whitespace(await element.inner_text())
                    if text:
                        event[field] = text

            categories = [
                self._collapse_whitespace(text)
                for text in await item.locator(".em-event-categories li").all_inner_texts()
                if text.strip()
            ]
            if categories:
                event["categories"] = ", ".join(categories)
            rows.append({"event": event})

        return rows

    @classmethod
    def _parse_dates(
        cls, date_text: str, time_text: str
    ) -> tuple[date | datetime, date | datetime | None]:
        parsed_dates: list[date] = [
            date(int(y), HUNGARIAN_MONTHS[m], int(d)) for y, m, d in re.findall(
                r"\b(\d{4})\s+(\w+)\s+(\d{1,2})\b", cls._normalize_text_for_match(date_text)
            )[:2]
        ]
        if not parsed_dates:
            raise ValueError(f"[{cls.id}] No or unexpected event date: {date_text!r}")
        start_date = parsed_dates[0]
        end_date = parsed_dates[1] if len(parsed_dates) == 2 else None

        times = [
            time(int(h), int(m)) for h, m in re.findall(
                r"\b(\d{1,2})\s*:\s*(\d{2})\b", time_text
            )[:2]
        ]
        if not times:
            return start_date, end_date

        start = datetime.combine(start_date, times[0], tzinfo=cls._TIMEZONE)
        end: date | datetime | None = end_date
        if len(times) == 2:
            end = datetime.combine(end_date or start_date, times[1], tzinfo=cls._TIMEZONE)
        return start, end
