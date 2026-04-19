from __future__ import annotations

import re
from datetime import date, datetime, timedelta

from icalendar import STATUS, Calendar

from event_crawler.downloader_base import DownloaderBase, ParserBase


class OldtimerDownloader(DownloaderBase):
    """Downloader + parser implementation for extracting oldtimernaptar.hu event dates."""

    id = "oldtimernaptar"
    url = "https://calendar.google.com/calendar/ical/oldtimernaptar%40gmail.com/public/basic.ics"

    url_regex = re.compile(
        r"\s*(?:Esem\u00e9ny\s*:\s*)?(?:(?:https://(?:www\.)?facebook\.com)?"
        r"\/?events\/(?:s\/[^\/]*\/)?|fb:\/\/event\/)(\d{8,})[^\\\"\n]*\s*",
        re.IGNORECASE
    )

    async def extract_data(self, content: str) -> ParserBase.Result:
        """Extract future event dates from the calendar."""

        page_rows: ParserBase.Result = []

        try:
            calendar = Calendar.from_ical(content)
        except Exception as exc:
            raise ValueError(f"[{self.id}] Failed to parse content as iCalendar data.") from exc

        for event in calendar.events:
            if event.status == STATUS.CANCELLED:
                continue

            if event.summary is None or event.DTSTART is None:
                print(f"[{self.id}] Invalid event received: {event.uid}")
                continue

            # Only get future events
            def is_old(dt):
                return (
                    isinstance(dt, datetime) and dt < datetime.now(tz=dt.tzinfo) or
                    not isinstance(dt, datetime) and dt < date.today()
                )
            if event.DTEND and is_old(event.DTEND) or not event.DTEND and is_old(event.DTSTART):
                continue

            evt_data = {}
            evt_data["summary"] = str(event.summary)
            evt_data["dtstart"] = event.DTSTART.isoformat() # pyright: ignore[reportOptionalMemberAccess]
            if event.DTEND:
                dtend = event.DTEND
                if not isinstance(event.DTEND, datetime):
                    dtend = event.DTEND - timedelta(days=1)
                evt_data["dtend"] = dtend.isoformat()
            if event.description:
                desc = str(event.description)
                if match := self.url_regex.search(desc):
                    evt_data["url"] = f"https://www.facebook.com/events/{match.group(1)}"
                    start, end = match.span()
                    desc = desc[:start] + desc[end:]
                evt_data["description"] = desc
            if event.location:
                evt_data["location"] = str(event.location)
            page_rows.append({"event": evt_data})

        return page_rows
