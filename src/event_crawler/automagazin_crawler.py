from __future__ import annotations

import html
import json
from datetime import datetime
from typing import Any

from playwright.async_api import Page

from event_crawler.crawler_base import CrawlerBase, ParserBase


class AutomagazinCrawler(CrawlerBase):
    """Crawler implementation for extracting Automagazin veteran car event dates."""

    id = "automagazin"
    url = "https://automagazinonline.hu/programok/lista/"

    @property
    def next_selectors(self) -> list[str]:
        return [
            "section[class*=tribe] a[rel=next]"
        ]

    @property
    def page_content_selectors(self) -> list[str]:
        # Using the main calendar container or entire DOM to track changes
        return [
            "ul[class*=calendar-list]"
        ]

    @property
    def next_click_options(self) -> dict[str, Any] | None:
        """Use force=True to handle the page loader intercepting clicks."""
        return {"force": True}

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        """Extract veteran car event dates from the calendar JSON-LD schema."""
        page_rows: ParserBase.Result = []

        scripts = page.locator("section[class*=tribe] script[type='application/ld+json']")
        for idx in range(await scripts.count()):
            script = scripts.nth(idx)
            text = (await script.text_content()) or ""
            if "Event" not in text:
                continue

            try:
                data = json.loads(text)
            except Exception:
                continue

            for item in data:
                title = html.unescape(self._collapse_whitespace(item["name"]))
                url = item.get("url", "")
                description = html.unescape(item.get("description", "")).replace("\\n", "\n")
                start_date = datetime.fromisoformat(item["startDate"]).date().isoformat()
                end_date = item.get("endDate")
                if end_date:
                    end_date = datetime.fromisoformat(end_date).date().isoformat()

                loc_data = item.get("location")
                location_parts = []
                if isinstance(loc_data, dict):
                    loc_name = loc_data.get("name", "")
                    if loc_name:
                        location_parts.append(loc_name)

                    address = loc_data.get("address", {})
                    if isinstance(address, dict):
                        city = address.get("addressLocality")
                        street = address.get("streetAddress")
                        current_loc_str = " ".join(location_parts)
                        if city and city not in current_loc_str:
                            location_parts.insert(0, city)
                        if street and street not in current_loc_str:
                            location_parts.insert(0, street)

                location = html.unescape(", ".join(location_parts))

                # Return structure that matches other crawlers
                evt_data = {"summary": title, "dtstart": start_date}
                if end_date:
                    evt_data["dtend"] = end_date
                if url:
                    evt_data["url"] = url
                if location:
                    evt_data["location"] = location
                if description:
                    evt_data["description"] = description

                page_rows.append({"event": evt_data})

        return self._dedupe(page_rows)
