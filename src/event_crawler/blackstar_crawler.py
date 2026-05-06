from __future__ import annotations

from datetime import date
import re
from typing import Any

from playwright.async_api import Locator, Page
from playwright.async_api import TimeoutError as PlaywrightTimeoutError

from event_crawler.crawler_base import CONTENT_TIMEOUT_MS, CrawlerBase, ParserBase
from event_crawler.parser_base import HUNGARIAN_MONTHS


DYNAMIC_CONTENT_TIMEOUT_MS = 5000

class BlackstarCrawler(CrawlerBase):
    """Crawler implementation for extracting Black Star Visonta trackday dates."""

    id = "blackstar"
    url = "https://www.blackstarvisonta.hu/naptar"
    reloading_pager = True

    datepicker_button_selector = (
        "#wix-events-widget[data-hydrated='true'] "
        "button[data-hook='calendar-date-picker-button']"
    )
    datepicker_next_selector = (
        "#wix-events-widget[data-hydrated='true'] "
        "button[data-hook='datepicker-right-arrow']"
    )
    month_cell_selector = (
        "#wix-events-widget[data-hydrated='true'] "
        "[role='grid'] [data-hook^='calendar-cell-']"
    )
    popup_details_selector = "[data-hook='calendar-event-details']"

    popup_month_aliases = {
        "jan": 1,
        "januar": 1,
        "febr": 2,
        "februar": 2,
        "marc": 3,
        "marcius": 3,
        "apr": 4,
        "aprilis": 4,
        "maj": 5,
        "majus": 5,
        "jun": 6,
        "junius": 6,
        "jul": 7,
        "julius": 7,
        "aug": 8,
        "augusztus": 8,
        "szept": 9,
        "szeptember": 9,
        "okt": 10,
        "oktober": 10,
        "nov": 11,
        "november": 11,
        "dec": 12,
        "december": 12,
    }

    @property
    def next_selectors(self) -> list[str]:
        return [self.datepicker_next_selector]

    @property
    def page_content_selectors(self) -> list[str]:
        return ["#wix-events-widget [role='grid']"]

    async def wait_until_ready(self, page: Page) -> None:
        await super().wait_until_ready(page)
        # Ensure post-load calendar "hydration" event, and datepicker button visibility
        await page.locator(self.datepicker_button_selector).wait_for(
            state="visible",
            timeout=CONTENT_TIMEOUT_MS,
        )
        await self._ensure_datepicker_open(page)

    async def is_page_empty(self, page: Page) -> bool:
        await self._ensure_datepicker_open(page)
        visible_month = await self._get_visible_month_parts(page)

        # Check cells only for the current month, ignore spillover days from adjacent months
        month_cells = page.locator(self.month_cell_selector)
        for idx in range(await month_cells.count()):
            cell = month_cells.nth(idx)
            aria_label_attr = await cell.get_attribute("aria-label")
            if aria_label_attr is None:
                raise ValueError(f"[{self.id}] Calendar cell {idx} is missing aria-label.")

            aria_label = self._collapse_whitespace(aria_label_attr)
            if not self._cell_matches_visible_month(aria_label, visible_month[1]):
                continue

            if "no events" not in self._normalize_text_for_match(aria_label):
                return False

        return True

    def finalize_result(self, aggregate: ParserBase.Result) -> ParserBase.Result:
        return self._dedupe(aggregate)

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        visible_year, visible_month_number, visible_caption = await self._get_visible_month_parts(
            page
        )
        rows = await self._extract_dom_rows(
            page,
            visible_year,
            visible_month_number,
            visible_caption,
        )
        await self._ensure_datepicker_open(page)
        return rows

    async def _ensure_datepicker_open(self, page: Page) -> None:
        # If the next arrow exists, then the datepicker is already open
        next_arrow = page.locator(self.datepicker_next_selector)
        if await next_arrow.first.is_visible():
            return

        button = page.locator(self.datepicker_button_selector)
        await button.click(force=True)
        await next_arrow.first.wait_for(state="visible", timeout=DYNAMIC_CONTENT_TIMEOUT_MS)

    async def _activate_next_page(
        self,
        page: Page,
        preferred_selectors: list[str],
        click_options: dict[str, Any] | None = None,
    ) -> bool:
        # Override the next page activator to use the month label change instead of a generic
        # calendar content change.
        await self._ensure_datepicker_open(page)

        visible_month_before = await self._get_visible_month_parts(page)
        next_button = await self._find_next_page_activator(page, preferred_selectors)
        if not next_button:
            print(f"[{self.id}] Next-page control not found or not enabled.")
            return False

        print(f"[{self.id}] Clicking next-page control...")
        await next_button.click(**(click_options or {}))

        check_interval_ms = 100
        for _ in range(int(CONTENT_TIMEOUT_MS / check_interval_ms)):
            await page.wait_for_timeout(check_interval_ms)
            visible_month_after = await self._get_visible_month_parts(page)
            if visible_month_after and visible_month_after != visible_month_before:
                print(f"[{self.id}] Content changed, waiting for the page to be ready...")
                await self.wait_until_ready(page)
                print(f"[{self.id}] New page marked ready.")
                return True

        print(f"[{self.id}] Timed out waiting for next page content to change.")
        raise PlaywrightTimeoutError(f"Next page did not load in time for {self.id}.")

    async def _get_visible_month_parts(self, page: Page) -> tuple[int, int, str]:
        # Gets the month and year from the date picker dialog's pickers
        caption = self._collapse_whitespace(
            await page.locator(self.datepicker_button_selector).inner_text()
        )
        if not caption:
            raise ValueError(f"[{self.id}] Datepicker caption is empty.")

        caption_norm = self._normalize_text_for_match(caption)
        year_match = re.search(r"\b(\d{4})\b", caption_norm)
        if not year_match:
            raise ValueError(
                f"[{self.id}] Could not parse year from datepicker caption: {caption!r}"
            )

        month_number = None
        for month_name, candidate in HUNGARIAN_MONTHS.items():
            if month_name in caption_norm:
                month_number = candidate
                break

        if month_number is None:
            raise ValueError(
                f"[{self.id}] Could not parse month from datepicker caption: {caption!r}"
            )

        return int(year_match.group(1)), month_number, caption

    async def _extract_dom_rows(
        self,
        page: Page,
        visible_year: int,
        visible_month_number: int,
        visible_caption: str,
    ) -> ParserBase.Result:
        month_cells = page.locator(self.month_cell_selector)
        rows: ParserBase.Result = []
        visible_month_cells = 0

        for idx in range(await month_cells.count()):
            cell = month_cells.nth(idx)
            aria_label_attr = await cell.get_attribute("aria-label")
            if aria_label_attr is None:
                raise ValueError(f"[{self.id}] Calendar cell {idx} is missing aria-label.")

            aria_label = self._collapse_whitespace(aria_label_attr)
            if not self._cell_matches_visible_month(aria_label, visible_month_number):
                continue

            visible_month_cells += 1
            day_number = await self._extract_cell_day(cell, aria_label)
            iso_date = self._build_visible_month_iso_date(
                visible_year,
                visible_month_number,
                day_number,
                visible_caption,
                aria_label,
            )

            if "no events" in self._normalize_text_for_match(aria_label):
                continue

            rows.extend(await self._extract_popup_rows(cell, iso_date))

        if visible_month_cells == 0:
            raise ValueError(
                f"[{self.id}] No calendar cells matched the visible month {visible_caption!r}."
            )

        return self._dedupe(rows)

    async def _extract_popup_rows(
        self,
        cell: Locator,
        fallback_date: str,
    ) -> ParserBase.Result:
        container = cell.locator("xpath=..")
        await cell.dispatch_event("click")

        try:
            event_list = container.locator("[data-hook='calendar-event-list']").first
            event_details = container.locator(self.popup_details_selector).first
            await event_list.or_(event_details).first.wait_for(
                state="visible",
                timeout=DYNAMIC_CONTENT_TIMEOUT_MS
            )

            if await event_list.is_visible():
                items = event_list.locator("ul > li")
                rows: ParserBase.Result = []
                for idx in range(await items.count()):
                    await items.nth(idx).click(force=True)

                    row = await self._extract_popup_detail_row(container, fallback_date)
                    if row:
                        rows.append(row)

                    back_button = container.locator("[data-hook='calendar-popup-back-button']")
                    await back_button.first.click(force=True)
                    await event_list.wait_for(state="visible", timeout=DYNAMIC_CONTENT_TIMEOUT_MS)
                return rows
            elif await event_details.is_visible():
                row = await self._extract_popup_detail_row(container, fallback_date)
                return [row] if row else []
            else:
                raise RuntimeError(f"[{self.id}] Popup appeared and vanished")
        except PlaywrightTimeoutError:
            print(f"[{self.id}] No popup appeared for a cell in time")
            raise

    async def _extract_popup_detail_row(
        self,
        popup_container: Locator,
        fallback_date: str,
    ) -> ParserBase.Row | None:
        popup = popup_container.locator(self.popup_details_selector).first
        await popup.wait_for(state="visible", timeout=DYNAMIC_CONTENT_TIMEOUT_MS)

        summary = self._collapse_whitespace(
            await popup.locator("[data-hook='title']").first.inner_text()
        )
        category = self._classify_title(summary)
        if not category:
            return None

        date_text = self._collapse_whitespace(
            await popup.locator("[data-hook='date']").first.inner_text()
        )

        dtstart, dtend = self._parse_popup_datetimes(date_text, fallback_date)

        description = None
        description_locator = popup.locator("[data-hook='description']")
        if await description_locator.count() > 0:
            description = self._clean_description(
                await description_locator.first.inner_text()
            ) or None

        url = await self._extract_popup_url(popup)

        return self._build_event_row(
            category=category,
            summary=summary,
            dtstart=dtstart,
            dtend=dtend,
            description=description,
            url=url,
        )

    async def _extract_cell_day(
        self,
        cell: Locator,
        aria_label: str,
    ) -> int:
        # The cell structure is only marked up with random classes, so we look for leaf divs
        # and use the first one as the day number.
        cell_dates = await cell.locator("div:not(:has(> *))").first.all_inner_texts()
        if not cell_dates:
            raise ValueError(
                f"[{self.id}] No leaf div texts found for calendar cell {aria_label!r}."
            )

        cell_date = self._collapse_whitespace(cell_dates[0])
        if not cell_date.isdigit():
            raise ValueError(
                f"[{self.id}] First leaf div is not a day number for cell {aria_label!r}: "
                f"{cell_date!r}"
            )

        return int(cell_date)

    def _build_visible_month_iso_date(
        self,
        year: int,
        month_number: int,
        day: int,
        visible_caption: str,
        aria_label: str,
    ) -> str:
        try:
            return date(year, month_number, day).isoformat()
        except ValueError as exc:
            raise ValueError(
                f"[{self.id}] Invalid day {day} for visible month {visible_caption!r} "
                f"while parsing {aria_label!r}."
            ) from exc

    async def _extract_popup_url(self, popup: Locator) -> str | None:
        link = popup.locator("[data-hook='title'] a[href]")
        if await link.count() == 0:
            return None

        href = self._collapse_whitespace(await link.first.get_attribute("href") or "")
        return href if href else None

    def _build_event_row(
        self,
        *,
        category: str,
        summary: str,
        dtstart: str,
        dtend: str | None = None,
        description: str | None = None,
        url: str | None = None,
    ) -> ParserBase.Row:
        event_data: ParserBase.Row = {
            "summary": summary,
            "dtstart": dtstart,
        }
        if dtend:
            event_data["dtend"] = dtend
        if description:
            event_data["description"] = description
        if url:
            event_data["url"] = url
        return {category: event_data}

    def _parse_popup_datetimes(
        self,
        date_text: str,
        fallback_date: str,
    ) -> tuple[str, str | None]:
        normalized = self._normalize_text_for_match(self._collapse_whitespace(date_text))
        normalized = normalized.replace("–", "-").replace("—", "-").replace("−", "-")

        explicit_dates: list[tuple[str, int]] = []
        for match in re.finditer(r"(\d{4})\.\s*([a-z]+)\.?\s*(\d{1,2})\.", normalized):
            iso_date = self._build_iso_date(
                int(match.group(1)),
                match.group(2),
                int(match.group(3)),
            )
            if iso_date:
                explicit_dates.append((iso_date, match.start()))

        times: list[tuple[str, int]] = []
        for match in re.finditer(r"\b(\d{1,2}):(\d{2})\b", normalized):
            times.append((f"{int(match.group(1)):02d}:{match.group(2)}", match.start()))

        start_date = explicit_dates[0][0] if explicit_dates else fallback_date
        start_time = times[0][0] if times else ""
        dtstart = self._combine_date_and_time(start_date, start_time)

        dtend = None
        if len(times) >= 2:
            end_date = start_date
            if len(explicit_dates) >= 2 and explicit_dates[1][1] > times[0][1]:
                end_date = explicit_dates[1][0]
            dtend = self._combine_date_and_time(end_date, times[1][0])

        return dtstart, dtend

    def _build_iso_date(self, year: int, month_name: str, day: int) -> str:
        month_number = self.popup_month_aliases.get(month_name.rstrip("."))
        if month_number is None:
            raise ValueError(f"[{self.id}] Unsupported popup month name: {month_name!r}")

        try:
            return date(year, month_number, day).isoformat()
        except ValueError as exc:
            raise ValueError(
                f"[{self.id}] Invalid popup date components: {year}-{month_name!r}-{day}"
            ) from exc

    def _combine_date_and_time(self, iso_date: str, time_text: str) -> str:
        time_match = re.search(r"\b(\d{1,2}):(\d{2})\b", time_text)
        if not time_match:
            return iso_date

        hour = int(time_match.group(1))
        minute = int(time_match.group(2))
        return f"{iso_date}T{hour:02d}:{minute:02d}:00"

    def _clean_description(self, description: str) -> str:
        lines = [self._collapse_whitespace(line) for line in description.splitlines()]
        return "\n".join(line for line in lines if line).strip()

    def _classify_title(self, title: str) -> str | None:
        title_norm = self._normalize_text_for_match(title)
        if "nyilt" in title_norm and "auto" in title_norm:
            return "trackday"
        if (
            "nyilt" in title_norm and ("moto" in title_norm or "robog" in title_norm) or
            "supermoto" in title_norm
        ):
            return "motor_trackday"
        return None

    def _cell_matches_visible_month(self, aria_label: str, visible_month_number: int) -> bool:
        label_norm = self._normalize_text_for_match(aria_label)
        for month_name, month_number in HUNGARIAN_MONTHS.items():
            if month_name in label_norm:
                return month_number == visible_month_number
        raise ValueError(f"[{self.id}] Could not parse month from cell aria-label: {aria_label!r}")
