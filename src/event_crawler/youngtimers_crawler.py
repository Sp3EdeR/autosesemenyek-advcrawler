from __future__ import annotations

import re
from collections import defaultdict
from datetime import date, timedelta
from typing import TypedDict
from urllib.parse import urljoin, urlparse

from playwright.async_api import Page

from event_crawler.crawler_base import CrawlerBase, ParserBase


class _ParsedRow(TypedDict):
    date: date
    summary: str
    url: str | None
    categories: list[str] | None


class _MergedRow(TypedDict):
    summary: str
    dtstart: date
    dtend: date
    url: str | None
    categories: list[str] | None


class YoungtimersCrawler(CrawlerBase):
    """Crawler implementation for extracting event listings from Youngtimers Club Hungary."""

    id = "youngtimersclub"
    url = "https://youngtimers-club.hu/esemenyek"

    @property
    def next_selectors(self) -> list[str]:
        return [".eventControl .next"]

    @property
    def page_content_selectors(self) -> list[str]:
        return [".eventlist"]

    async def is_page_empty(self, page: Page) -> bool:
        """Check whether the currently displayed calendar month has any events."""
        return await page.locator(".eventlist tr:not(:has(td[colspan]))").count() == 0

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        rows = page.locator("table.eventlist tr")

        today = date.today()
        grouped_rows: dict[str, list[_ParsedRow]] = defaultdict(list)
        for idx in range(await rows.count()):
            row = rows.nth(idx)
            cells = row.locator("td")
            if await cells.count() != 2:
                raise ValueError(
                    "Unexpected row structure, expected exactly 2 cells: "
                    f"{await row.inner_html()}"
                )

            dtstart = self._collapse_whitespace(await cells.nth(0).inner_text())
            dtstart = date.fromisoformat(dtstart)

            url = None
            link_elem = cells.nth(1).locator("a")
            if await link_elem.count() == 0:
                summary = self._collapse_whitespace(await cells.nth(1).inner_text())
            else:
                summary = self._collapse_whitespace(await link_elem.first.inner_text())
                url = (await link_elem.first.get_attribute("href")) or None
                url = urljoin(self.url, url) if url else None

            cat_class = (await cells.nth(0).get_attribute("class")) or ""
            categories = self._collapse_whitespace(cat_class).split() if cat_class else None

            merge_key = self._build_merge_key(summary, url)
            grouped_rows[merge_key].append(
                {
                    "date": dtstart,
                    "summary": summary,
                    "url": url,
                    "categories": categories,
                }
            )

        page_rows: ParserBase.Result = []
        for grouped in grouped_rows.values():
            grouped.sort(key=lambda item: item["date"])
            merged_events = self._merge_event_rows(grouped)
            for merged in merged_events:
                dtend = merged["dtend"]
                if dtend < today:
                    continue

                event = {
                    "summary": str(merged["summary"]),
                    "dtstart": merged["dtstart"].isoformat(),
                    "dtend": dtend.isoformat(),
                }

                categories = merged.get("categories")
                if categories:
                    event["description"] = f"Kategoriak: {', '.join(categories)}"

                event_url = merged.get("url")
                if event_url:
                    event["url"] = str(event_url)

                page_rows.append({"event": event})

        return page_rows

    def _build_merge_key(self, summary: str, event_url: str | None) -> str:
        if event_url:
            path = urlparse(event_url).path.strip("/")
            if path:
                return re.sub(r"-\d+$", "", path).casefold()

        return self._normalize_text_for_match(summary)

    def _merge_event_rows(
        self,
        rows: list[_ParsedRow],
    ) -> list[_MergedRow]:
        merged: list[_MergedRow] = []

        for row in rows:
            event_date = row["date"]
            current_end = merged[-1]["dtend"] if merged else None

            if current_end and event_date == current_end + timedelta(days=1):
                merged[-1]["dtend"] = event_date
                continue

            merged.append({
                "summary": row["summary"],
                "dtstart": event_date,
                "dtend": event_date,
                "url": row["url"],
                "categories": row["categories"],
            })

        return merged
