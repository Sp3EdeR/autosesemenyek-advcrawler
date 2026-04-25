from __future__ import annotations

import asyncio
import json
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta

from event_crawler.downloader_base import DownloaderBase, ParserBase


class AutosnaptarDownloader(DownloaderBase):
    """Downloader + parser implementation for extracting autosnaptar.hu event dates."""

    id = "autosnaptar"
    url = "https://autosnaptar.hu/site/search"

    async def download(self) -> bytes:
        today = datetime.today()

        req = urllib.request.Request(self.url)
        req.add_header("Accept", "application/json, text/javascript, */*; q=0.01")
        req.add_header("Accept-Language", "hu-HU,hu;q=0.9,en-US;q=0.8,en;q=0.7")
        req.add_header("Host", "autosnaptar.hu")
        req.add_header("Origin", "https://autosnaptar.hu")
        req.add_header(
            "Referer", f"https://autosnaptar.hu/?mode=list&year={today.year}&month={today.month}"
        )
        req.add_header(
            "User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            "(KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
        )
        req.add_header("X-Requested-With", "XMLHttpRequest")
        data = {
            "startDate": today.strftime("%Y-%m-%d"),
            "endDate": (today + timedelta(days=365)).strftime("%Y-%m-%d"),
            "car_select": "",
            "mode": "list"
        }
        req.data = bytes(urllib.parse.urlencode(data), encoding="utf-8")
        return await asyncio.to_thread(lambda: self._fetch_with_error_handling(req))

    async def extract_data(self, content: str) -> ParserBase.Result:
        """Extract future event dates from the calendar."""

        page_rows: ParserBase.Result = []

        try:
            payload = json.loads(content)
            if not isinstance(payload, list):
                raise ValueError()
        except Exception as exc:
            raise ValueError(f"[{self.id}] Failed to parse content as JSON data.") from exc

        today = date.today()
        for raw_event in payload:
            start_date = date.fromisoformat(raw_event["date_start"])
            raw_end_date = raw_event.get("date_end", "")
            end_date = date.fromisoformat(raw_end_date) if raw_end_date else None
            if (end_date or start_date) < today:
                continue

            summary = raw_event["title"]
            evt_data = {
                "summary": summary,
                "dtstart": start_date.isoformat(),
            }
            if end_date:
                evt_data["dtend"] = end_date.isoformat()

            city = raw_event.get("city") or ""
            if city:
                evt_data["location"] = city

            event_id = raw_event.get("id")
            slug = str(raw_event.get("link_rewrite") or "")
            if event_id not in (None, "") and slug:
                evt_data["url"] = f"https://autosnaptar.hu/{event_id}-{slug}"

            detail_parts: list[str] = []

            description_short = str(raw_event.get("description_short") or "")
            if description_short:
                detail_parts.append(description_short)

            type_title = str(raw_event.get("type_title") or "")
            if type_title:
                detail_parts.append(f"Tipus: {type_title}")

            organizer = str(raw_event.get("organizer") or "")
            if organizer:
                detail_parts.append(f"Szervezo: {organizer}")

            flags_raw = str(raw_event.get("flags") or "").strip()
            flags = [flag for flag in flags_raw.split("|") if flag.strip()]
            if flags:
                detail_parts.append(f"Jellemzok: {', '.join(flags)}")

            if detail_parts:
                evt_data["description"] = "\n".join(detail_parts)

            page_rows.append({"event": evt_data})

        return page_rows
