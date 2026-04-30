from __future__ import annotations

import asyncio
import re
from datetime import date, datetime, timedelta

import icalendar

from event_crawler.downloader_base import DownloaderBase, ParserBase


class AutosesemenyekDownloader(DownloaderBase):
    """Downloader + parser implementation for extracting autosesemenyek event dates."""

    id = "autosesemenyek"
    url = "https://sp3eder.github.io/autosesemenyek/"

    async def extract_data(self, content: str) -> ParserBase.Result:
        """Extracts autosesemenyek calendar IDs, and downloads & parses ICS files"""
        pattern = re.compile(
            r"""{\s*['\"]id['\"]\s*:\s*['\"]([^'\"]+)['\"]\s*,"""
            r"""\s*['\"]clr['\"]\s*:\s*['\"]#[0-9A-Fa-f]+['\"]\s*}"""
        )
        rows: ParserBase.Result = []
        for m in pattern.finditer(content):
            cal_id = m.group(1)
            cal_ics = await self.download_calendar(cal_id)

            output_id_match = re.match(r"[0-9A-Fa-f]+", cal_id)
            if output_id_match is None:
                raise ValueError(f"[{self.id}] Invalid calendar ID format: {cal_id}")
            output_id = output_id_match.group(0)
            rows.extend({output_id: evt} for evt in self.get_calendar_events(cal_ics))
        return rows

    async def download_calendar(self, cal_id: str) -> str:
        """Download iCal data from a Google Calendar public URL."""
        url = f"https://calendar.google.com/calendar/ical/{cal_id}/public/basic.ics"
        content_bin = await asyncio.to_thread(
            lambda url=url: self.fetch_with_error_handling(url)
        )
        return self.decode_content(content_bin)

    def get_calendar_events(self, ics: str):
        """Parse iCal data and extract needed event information."""
        def is_old(dt: date | datetime) -> bool:
            return (
                isinstance(dt, datetime) and dt < datetime.now(tz=dt.tzinfo) or
                not isinstance(dt, datetime) and dt < date.today()
            )

        try:
            cal = icalendar.Calendar.from_ical(ics)
        except Exception as exc:
            raise ValueError(f"[{self.id}] Failed to parse content as iCalendar data.") from exc

        for component in cal.walk():
            if component.name == "VEVENT":
                dtstart = component["dtstart"].dt
                dtend_prop = component.get("dtend")
                dtend = dtend_prop.dt if dtend_prop is not None else None
                # Only get future events
                if dtend and is_old(dtend) or not dtend and is_old(dtstart):
                    continue

                evt_data = {
                    "summary": str(component["summary"]),
                    "dtstart": dtstart.isoformat(),
                }
                if dtend is not None:
                    if not isinstance(dtend, datetime):
                        # All-day events are an open interval
                        dtend = dtend - timedelta(days=1)
                    evt_data["dtend"] = dtend.isoformat()

                location = component.get("location")
                if location:
                    evt_data["location"] = str(location)

                description = component.get("description")
                if description:
                    evt_data["description"] = str(description)

                yield evt_data
