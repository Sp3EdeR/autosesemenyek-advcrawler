from __future__ import annotations

import re
import urllib.parse
from typing import Any

from playwright.async_api import Locator, Page

from event_crawler.crawler_base import CrawlerBase, ParserBase


class ProgramturizmusCrawler(CrawlerBase):
    id = "programturizmus"
    url = "https://www.programturizmus.hu/kategoria-autos-motoros-talalkozo.html"

    reloading_pager = True

    _date_regex = re.compile(r"\d{4}\.\d{2}\.\d{2}\.")

    @property
    def next_selectors(self) -> list[str]:
        return ["div[class*=pagination] a[class*=active] + a"]

    @property
    def page_content_selectors(self) -> list[str]:
        return ["div[class*=table]:has(>div[class*=content])"]

    @property
    def next_click_options(self) -> dict[str, Any] | None:
        """Use force=True to handle the page loader intercepting clicks."""
        return {"force": True}

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        page_rows: ParserBase.Result = []
        events = page.locator(".d-desktop div[class*=descriptionWrapper]")

        cnt = await events.count()
        for idx in range(cnt):
            event = events.nth(idx)
            dates = self._date_regex.findall(
                await event.locator("p[class*=menu-button]").first.inner_text()
            )
            evt_data = {
                "summary": await self._get_summary(event),
                "dtstart": self._to_iso_date(dates[0]),
            }
            if len(dates) > 1:
                evt_data["dtend"] = self._to_iso_date(dates[1])
            if href := await self._get_href(event, "div[class*=titleContainer] a[href]"):
                evt_data["url"] = urllib.parse.urljoin(self.url, href)
            if location := await self._get_text(
                event, "div[class*=locationContainer] a:last-of-type"
            ):
                evt_data["location"] = location
            if description := await self._get_text(event, "p[class*=body]"):
                evt_data["description"] = description
            page_rows.append({"event": evt_data})

        return page_rows

    async def _get_summary(self, event: Locator) -> str:
        label = event.locator("p[class*=label]")
        if await label.count():
            text = self._collapse_whitespace(await label.first.inner_text())
            if text:
                return text
        return self._collapse_whitespace(await event.locator("h2").first.inner_text())

    async def _get_text(self, event: Locator, selector: str) -> str:
        locator = event.locator(selector)
        if not await locator.count():
            return ""
        return self._collapse_whitespace(await locator.first.inner_text())

    async def _get_href(self, event: Locator, selector: str) -> str:
        locator = event.locator(selector)
        if not await locator.count():
            return ""
        return (await locator.first.get_attribute("href")) or ""

    @staticmethod
    def _to_iso_date(value: str) -> str:
        return value.rstrip(".").replace(".", "-")
