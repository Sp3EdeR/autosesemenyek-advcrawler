from __future__ import annotations

import asyncio
import re
from datetime import date
from typing import Any

import cv2
import numpy as np
from playwright.async_api import Page

from event_crawler.crawler_base import ParserBase, SinglePageCrawlerBase
from event_crawler.parser_base import HUNGARIAN_MONTHS

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

# Vertical proximity threshold for line segmentation algorithms.
# Controls bounding box aggregation along the Y-axis.
# Value impact:
#   - High: merges adjacent rows into single horizontal strings.
#   - Low: breaks individual sentences into distinct vertical fragments.
Y_THRESHOLD: float = 25.0

class LzgpCrawler(SinglePageCrawlerBase):
    """Crawler for the LZGP Gokart Championship race calendar on lzgp.hu.

    Since LZGP publishes their calendar as a single image, this crawler:
    1. Loads lzgp.hu using Playwright.
    2. Locates and downloads the calendar image.
    3. Runs text recognition on the image using OCREngine.
    4. Parses the Hungarian text to build structured calendar events.
    """

    id = "lzgp"
    url = "https://lzgp.hu/"

    _LOCATION = "Palócring, Patvarc"
    _CALENDAR_LINK_SELECTOR = "img[src*=versenynaptar], img[data-src-fg*=versenynaptar]"
    _IMAGE_FILENAME = "versenynaptar-palocra.webp"

    _LENGTH_RE = re.compile(r"(\d+)\s*m\b")

    # --- OCR-related configuration ---
    _OCR_LANG = "hu"
    _OCR_CONFIDENCE_THRESHOLD = 0.7


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

    def _run_ocr(self, img_data: cv2.typing.MatLike) -> list[dict[str, Any]]:
        """Runs text recognition on the image and returns bounding box details."""
        from ocr.models.text_ocr import TextOCREngine
        engine = TextOCREngine(lang=self._OCR_LANG, enable_mkldnn=False)
        return engine.process(
            img_data,
            log_id=self.id,
            confidence_threshold=self._OCR_CONFIDENCE_THRESHOLD,
        )


    @staticmethod
    def _cluster_rows(
        boxes: list[dict[str, Any]]
    ) -> list[list[dict[str, Any]]]:
        """Groups bounding boxes into horizontal rows based on y coordinate proximity."""
        if not boxes:
            return []

        sorted_boxes = sorted(boxes, key=lambda b: b["y"])
        rows: list[list[dict[str, Any]]] = [[sorted_boxes[0]]]

        for box in sorted_boxes[1:]:
            avg_y = sum(b["y"] for b in rows[-1]) / len(rows[-1])
            if abs(box["y"] - avg_y) <= Y_THRESHOLD:
                rows[-1].append(box)
            else:
                rows.append([box])

        # Sort text boxes in each row from left to right
        for row in rows:
            row.sort(key=lambda b: b["x"])
        return rows

    @staticmethod
    def _detect_year(rows: list[list[dict[str, Any]]]) -> int:
        """Extracts the calendar year from the text.

        Raises:
            RuntimeError: If no plausible year can be detected from the OCR
                text, notifies callers.
        """
        current_year = date.today().year
        for row in rows:
            for box in row:
                # Look for a standard year format like "2026"
                match = re.search(r"20\d{2}", box["text"])
                if match and current_year -1 <= int(match.group(0)) <= current_year + 1 :
                    return int(match.group(0))

        raise RuntimeError(
            f"[{LzgpCrawler.id}] Could not detect the calendar year from OCR "
            "text."
        )

    def _parse_events(self, text_boxes: list[dict[str, Any]]) -> ParserBase.Result:
        """Parses recognized text blocks into structured race events.

        Each data row carries the month, day, and track length together:
        1. Group text boxes into horizontal rows.
        2. Identify data rows by the track length at row[3].
        3. Extract the month from row[1] and the day from the row, then build the event.
        """
        rows = self._cluster_rows(text_boxes)
        year = self._detect_year(rows)

        events: ParserBase.Result = []

        for row in rows:
            if len(row) < 4:
                continue

            # Track length always sits at row[3]
            m = self._LENGTH_RE.search(row[3]["text"])
            if not m:
                continue
            track_length = f"{m.group(1)}m"

            avg_y = sum(b["y"] for b in row) / len(row)

            # Month is in the same row at row[1]
            normalized_month_data = self._normalize_text_for_match(row[1]["text"])
            assigned_month = next(
                (month_num for month_name, month_num in HUNGARIAN_MONTHS.items()
                 if month_name in normalized_month_data),
                None,
            )
            if assigned_month is None:
                print(f"[{self.id}] Could not assign month for row at y={avg_y:.0f}")
                continue

            # Parse the day number
            day = self._extract_day(row)
            if day is None:
                print(f"[{self.id}] Could not extract day for row at y={avg_y:.0f}")
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
