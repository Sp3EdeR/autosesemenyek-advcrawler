from __future__ import annotations

import asyncio
import re
import sys
from datetime import date
from pathlib import Path
from typing import Any

import cv2
import numpy as np
from playwright.async_api import Page

from event_crawler.crawler_base import ParserBase, SinglePageCrawlerBase
from event_crawler.parser_base import ACCENTED_HUNGARIAN_MONTHS

# Maps track lengths to their canonical names. This is the most reliable way 
# to identify tracks because the OCR detects length numbers consistently, 
# whereas text-based track names often get split across multiple bounding boxes.
_LENGTH_TO_TRACK: dict[str, str] = {
    "1200m": "teljes nyomvonal",
    "810m": "2. nyomvonal",
    "964m": "3. nyomvonal",
    "660m": "4. nyomvonal",
    "782m": "5. nyomvonal",
}

class LzgpCrawler(SinglePageCrawlerBase):
    """Crawler for the LZGP Gokart Championship race calendar on lzgp.hu.

    Since LZGP publishes their calendar as a single image, this crawler:
    1. Loads lzgp.hu using Playwright.
    2. Locates and downloads the calendar image.
    3. Runs text recognition on the image using OCREngine.
    4. Parses the Hungarian text to build structured calendar events.
    """

    id = "lzgp"
    url: str = "https://lzgp.hu/"

    _LOCATION = "Palócring, Patvarc"
    _CALENDAR_LINK_SELECTOR = "img[src*=versenynaptar], img[data-src-fg*=versenynaptar]"
    _IMAGE_FILENAME = "versenynaptar-palocra.webp"

    _LENGTH_RE = re.compile(r"(\d+)\s*m\b")

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        """Extract race calendar events from the lzgp.hu page."""

        # Locate the calendar image element on the page
        link = page.locator(self._CALENDAR_LINK_SELECTOR)
        if await link.count() == 0:
            print(f"[{self.id}] WARNING: No calendar image link found on page.")
            return []

        img_url = await link.first.get_attribute("data-src-fg")
        if not img_url or img_url.startswith("data:"):
            img_url = await link.first.get_attribute("src")

        if not img_url or img_url.startswith("data:"):
            print(f"[{self.id}] WARNING: Calendar image URL could not be resolved from data-src-fg or src.")
            return []

        print(f"[{self.id}] Calendar image URL: {img_url}")
        # In extract_page_data:
        response = await page.request.get(img_url)
        if response.status != 200:
            print(f"[{self.id}] ERROR: Failed to read calendar image from {img_url}")
            return []
        img_data = await response.body()
        img_buffer = np.frombuffer(img_data, dtype=np.uint8)
        img_array = cv2.imdecode(img_buffer, cv2.IMREAD_COLOR)

        if img_array is None:
            print(f"[{self.id}] ERROR: Failed to read calendar image from {img_url}")
            return []

        # Run text recognition on the downloaded image
        text_boxes = await asyncio.to_thread(self._run_ocr, img_array)
        print(f"[{self.id}] OCR extracted {len(text_boxes)} text regions.")

        # Parse the recognized text into structured event objects
        events = self._parse_events(text_boxes)
        print(f"[{self.id}] Parsed {len(events)} race events.")
        return events

    def _run_ocr(self, img_data: str) -> list[dict[str, Any]]:
        """Runs text recognition on the image and returns bounding box details."""
        # OCREngine is in the project root, which isn't in Python's default import path.
        # We append the project root to sys.path and perform a lazy import to prevent
        # import issues during orchestration module discovery.
        project_root = str(Path(__file__).resolve().parent.parent.parent)
        if project_root not in sys.path:
            sys.path.insert(0, project_root)

        from ocr.models.text_ocr import TextOCREngine
        engine = TextOCREngine(lang="hu", enable_mkldnn=False)
        return engine.process(img_data, log_id=self.id)


    @staticmethod
    def _cluster_rows(
        boxes: list[dict[str, Any]],
        y_threshold: float = 25.0,
    ) -> list[list[dict[str, Any]]]:
        """Groups bounding boxes into horizontal rows based on y coordinate proximity."""
        if not boxes:
            return []

        sorted_boxes = sorted(boxes, key=lambda b: b["y"])
        rows: list[list[dict[str, Any]]] = [[sorted_boxes[0]]]

        for box in sorted_boxes[1:]:
            avg_y = sum(b["y"] for b in rows[-1]) / len(rows[-1])
            if abs(box["y"] - avg_y) <= y_threshold:
                rows[-1].append(box)
            else:
                rows.append([box])

        # Sort text boxes in each row from left to right
        for row in rows:
            row.sort(key=lambda b: b["x"])
        return rows

    @staticmethod
    def _detect_year(rows: list[list[dict[str, Any]]]) -> int:
        """Extracts the calendar year from the text, falling back to the current year."""
        for row in rows:
            for box in row:
                # Look for a standard year format like "2026"
                match = re.search(r"20(\d{2})", box["text"])
                if match:
                    return 2000 + int(match.group(1))
                # Fallback for OCR errors/artifacts like "026|"
                match = re.search(r"0(\d{2})\|", box["text"])
                if match:
                    suffix = int(match.group(1))
                    if 20 <= suffix <= 40:
                        return 2000 + suffix
        return date.today().year

    def _parse_events(self, text_boxes: list[dict[str, Any]]) -> ParserBase.Result:
        """Parses recognized text blocks into structured race events.

        Algorithm:
        1. Group text boxes into horizontal rows.
        2. Find where the month names are positioned, and identify data rows
           by checking for track length patterns (e.g., '1200m').
        3. For each data row, associate it with the closest preceding month header,
           extract the day of the month, and determine the track name from the length.
        """
        rows = self._cluster_rows(text_boxes)
        year = self._detect_year(rows)

        # Pass 1: Identify Y-positions for months and active race rows
        month_entries: list[tuple[float, int]] = []   # (y_center, month_num)
        data_rows: list[tuple[float, list[dict], str]] = []  # (y, row, length)

        for row in rows:
            row_text = " ".join(b["text"] for b in row).lower()
            avg_y = sum(b["y"] for b in row) / len(row)

            # Match Hungarian month names
            for month_name, month_num in ACCENTED_HUNGARIAN_MONTHS.items():
                if month_name in row_text:
                    month_entries.append((avg_y, month_num))
                    break

            # Find data rows containing a track length
            for box in row:
                m = self._LENGTH_RE.search(box["text"])
                if m:
                    data_rows.append((avg_y, row, f"{m.group(1)}m"))
                    break

        month_entries.sort(key=lambda e: e[0])

        # Pass 2: Build event dictionaries
        events: ParserBase.Result = []

        for data_y, row_boxes, track_length in data_rows:
            # Link each row to the closest month situated above it
            assigned_month: int | None = None
            for m_y, m_num in month_entries:
                if m_y <= data_y + 30:
                    assigned_month = m_num

            if assigned_month is None:
                print(f"[{self.id}] Could not assign month for row at y={data_y:.0f}")
                continue

            # Parse the day number
            day = self._extract_day(row_boxes)
            if day is None:
                print(f"[{self.id}] Could not extract day for row at y={data_y:.0f}")
                continue

            # Map the track length to its canonical name
            track_name = _LENGTH_TO_TRACK.get(track_length, f"nyomvonal ({track_length})")

            try:
                event_date = date(year, assigned_month, day).isoformat()
            except ValueError:
                print(
                    f"[{self.id}] Invalid date: {year}-{assigned_month}-{day}, skipping."
                )
                continue

            description = f"{track_name} ({track_length})"

            events.append({
                "lzgp": {
                    "date": event_date,
                    "description": description,
                    "location": self._LOCATION,
                }
            })

        return self._dedupe(events)

    @staticmethod
    def _extract_day(row_boxes: list[dict[str, Any]]) -> int | None:
        """Extracts the day number from a data row.

        A typical row is formatted left-to-right as: round number, month, day,
        optional Sunday/weekend indicator '(V)', track info, and track length.
        We gather all numeric candidates, sort them horizontally, skip the
        leftmost one (which represents the round number), and return the actual day.
        """
        candidates: list[tuple[int, float]] = []   # (value, x_pos)

        for box in row_boxes:
            # Extract digits and check if they form a plausible day number (1-31)
            cleaned = re.sub(r"[^0-9]", "", box["text"])
            if not cleaned:
                continue
            val = int(cleaned)
            if 1 <= val <= 31:
                candidates.append((val, box["x"]))

        if not candidates:
            return None

        # Sort by x position, left to right 
        candidates.sort(key=lambda c: c[1])

        if len(candidates) >= 2:
            # Since the first number is usually the round, prefer the second one.
            # However, if the first number is > 12, it is likely the day itself.
            if candidates[0][0] > 12:
                return candidates[0][0]
            return candidates[1][0]

        # Single candidate, return it
        return candidates[0][0]
