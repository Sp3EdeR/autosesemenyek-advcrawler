from __future__ import annotations

import re
from datetime import datetime
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

from playwright.async_api import Page

from event_crawler.crawler_base import CONTENT_TIMEOUT_MS, ParserBase, SinglePageCrawlerBase
from event_crawler.parser_base import HUNGARIAN_MONTHS


class TuningeventCrawler(SinglePageCrawlerBase):
    """Extract event cards from the single-page TuningEvent listing."""

    id = "tuningevent"
    url = "https://tuningevent.hu/esemenyek"

    _CARD_SELECTOR = 'main article.eventCard'
    _TIMEZONE = ZoneInfo("Europe/Budapest")

    async def wait_until_ready(self, page: Page) -> None:
        await super().wait_until_ready(page)
        await page.locator(self._CARD_SELECTOR).first.wait_for(
            state="attached", timeout=CONTENT_TIMEOUT_MS
        )
        await page.locator("footer").first.wait_for(
            state="attached", timeout=CONTENT_TIMEOUT_MS
        )

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        rows: ParserBase.Result = []
        for card in await page.locator(self._CARD_SELECTOR).all():
            summary = self._collapse_whitespace(await card.locator("h2").inner_text())
            href = await card.locator('a.eventDetailsButton[href]').first.get_attribute("href")
            if not summary or not href:
                raise ValueError(f"[{self.id}] Event title or details URL is missing.")

            datetimes = self._parse_datetimes(await card.locator(".eventDateLine").inner_text())
            event: ParserBase.Row = {
                "summary": summary,
                "location": self._collapse_whitespace(
                    await card.locator(".eventLocation").inner_text()
                ),
                "dtstart": datetimes[0],
                "description": self._collapse_whitespace(
                    await card.locator(".eventCardDescription").inner_text()
                ),
                "url": urljoin(self.url, href),
            }
            if len(datetimes) > 1:
                event["dtend"] = datetimes[1]

            maps = card.locator('a[href^="https://www.google.com/maps/"]')
            if await maps.count():
                maps_url = (await maps.first.get_attribute("href")) or ""
                coordinates = re.search(
                    r"destination=(-?\d+(?:\.\d+)?)%2C(-?\d+(?:\.\d+)?)",
                    maps_url,
                    re.IGNORECASE
                )
                if coordinates:
                    event["geo"] = f"{coordinates[1]};{coordinates[2]}"
            rows.append({"event": event})
        return rows

    @classmethod
    def _parse_datetimes(cls, text: str) -> list[str]:
        normalized = cls._normalize_text_for_match(text)
        matches = list(re.finditer(
            r"(\d{4})\.\s+(\w+)\s+(\d{1,2})\.\s+(\d{1,2}):(\d{2})", normalized
        ))
        if not matches:
            raise ValueError(f"[{cls.id}] Unexpected event date: {text!r}")
        datetimes = []
        for index, match in enumerate(matches):
            try:
                datetimes.append(datetime(
                    int(match[1]), HUNGARIAN_MONTHS[match[2]], int(match[3]),
                    int(match[4]), int(match[5]), tzinfo=cls._TIMEZONE,
                ).isoformat())
            except (KeyError, ValueError) as exc:
                if index == 0:
                    raise ValueError(f"[{cls.id}] Unexpected event date: {text!r}") from exc
        return datetimes