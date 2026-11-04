from __future__ import annotations

import csv
import re
from datetime import datetime, time, timedelta
from io import StringIO

from event_crawler.downloader_base import DownloaderBase, ParserBase


class AutosesemenyekDownloader(DownloaderBase):
    """Download the current autosesemenyek events from the spreadsheet CSV export."""

    id = "autosesemenyek"
    url = (
        "https://docs.google.com/spreadsheets/d/"
        "1wXqLusMxEXWKGjQV1Q37wzJHGz9MSNXo-8J99XXEiy4/gviz/tq"
        "?tqx=out:csv&headers=1&sheet=events"
    )

    async def extract_data(self, content: str) -> ParserBase.Result:
        reader = csv.DictReader(StringIO(content, newline=""))
        required = {"start", "summary", "calid"}
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f"[{self.id}] Missing required CSV columns: {sorted(required)}")

        rows: ParserBase.Result = []
        for row in reader:
            start = datetime.fromisoformat(row["start"])
            summary = row["summary"].strip()
            calendar_id = re.match(r"[0-9A-Fa-f]+", row["calid"])
            if not summary or calendar_id is None:
                raise ValueError(
                    f"[{self.id}] Invalid summary or calendar ID at line {reader.line_num}"
                )

            event = {"summary": summary, "dtstart": start.isoformat()}
            if row.get("end"):
                end = datetime.fromisoformat(row["end"])
                # The spreadsheet represents all-day dates as UTC-midnight pairs.
                if all(
                    dt.time() == time.min and dt.utcoffset() == timedelta(0)
                    for dt in (start, end)
                ):
                    event["dtstart"] = start.date().isoformat()
                    event["dtend"] = (end.date() - timedelta(days=1)).isoformat()
                else:
                    event["dtend"] = end.isoformat()
            for field in ("location", "description"):
                if row.get(field):
                    event[field] = row[field]
            rows.append({calendar_id.group(0): event})
        return rows
