from __future__ import annotations

import csv
from io import StringIO

from event_crawler.downloader_base import DownloaderBase, ParserBase


class GoogleDownloader(DownloaderBase):
    """Download URL-first Google spreadsheet events without requiring optional metadata."""

    id = "google"
    url = (
        "https://docs.google.com/spreadsheets/d/"
        "1JJci7hawy5Hc6fvQWmY4WjgNOi_oPIMaalM_-WZFI7I/gviz/tq"
        "?tqx=out:csv&headers=1&sheet=events"
    )

    async def extract_data(self, content: str) -> ParserBase.Result:
        reader = csv.DictReader(StringIO(content, newline=""))
        if "url" not in (reader.fieldnames or []):
            raise ValueError(f"[{self.id}] Missing required CSV column: url")

        rows: ParserBase.Result = []
        for row in reader:
            url = row["url"].strip()
            if not url:
                raise ValueError(f"[{self.id}] Missing URL at line {reader.line_num}")
            event = {"url": url}
            for field in ("start", "summary", "location", "description"):
                value = row.get(field)
                if value and value.strip():
                    event["dtstart" if field == "start" else field] = value
            rows.append({"evt": event})
        return rows
