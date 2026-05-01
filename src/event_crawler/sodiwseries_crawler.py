from __future__ import annotations

import importlib
import os
from datetime import date
from typing import Any

from playwright.async_api import Locator, Page

from event_crawler.crawler_base import CONTENT_TIMEOUT_MS, CrawlerBase, ParserBase

MAXIMUM_PAGINATION_COUNT = 10


class SodiWSeriesCrawler(CrawlerBase):
    """Crawler implementation for extracting Sodi W Series race listings."""

    id = "sodiwseries"
    url = f"https://www.sodiwseries.com/en-gb/races/{date.today():%Y/%m}"
    max_pages = 12
    reloading_pager = True

    recaptcha_form_selector = "form#form-recaptcha"
    country_hungary_option_selector = 'select#country option[value="143"]'
    country_hungary_search_selector = "#search-race-form .country-flag-hu"
    listing_rows_selector = "#race-listing-table tbody tr[data-rowlink]"
    pagination_selector = ".pagination[data-current-page]"
    pagination_more_selector = ".pagination li:last-child:not(.disabled)"

    def build_camoufox_launch_options(self) -> dict[str, Any]:
        from playwright_captcha.utils.camoufox_add_init_script.add_init_script import (
            get_addon_path,
        )

        addon_path = os.path.abspath(get_addon_path())
        return {
            "i_know_what_im_doing": True,
            "main_world_eval": True,
            "addons": [addon_path],
        }

    @property
    def next_selectors(self) -> list[str]:
        return ["#race-search-date a:has(.fa-chevron-right)"]

    @property
    def page_content_selectors(self) -> list[str]:
        return ["#race-listing-table", self.recaptcha_form_selector]

    @property
    def next_click_options(self) -> dict[str, Any] | None:
        """Use force=True to handle the page loader intercepting clicks."""
        return {"force": True}

    async def wait_until_ready(self, page: Page) -> None:
        await super().wait_until_ready(page)

        for i in range(3):
            try:
                if await self._solve_recaptcha_if_present(page):
                    await super().wait_until_ready(page)
                    break
            except RuntimeError as exc:
                print(f"[{self.id}] Failed to solve reCAPTCHA attempt {i + 1}/3: {exc}")
                if 2 <= i:
                    raise

        if await self._apply_hungary_filter_if_needed(page):
            await super().wait_until_ready(page)

        await self._load_all_listing_pages(page)

    async def extract_page_data(self, page: Page) -> ParserBase.Result:
        rows = page.locator(self.listing_rows_selector)
        page_rows: ParserBase.Result = []
        today = date.today()

        for idx in range(await rows.count()):
            row = rows.nth(idx)
            cells = row.locator("td")
            if await cells.count() != 9:
                raise ValueError(
                    f"[{self.id}] Unexpected Sodi table structure: {await row.inner_html()}"
                )

            dtstart = date.fromisoformat(
                self._collapse_whitespace(await cells.nth(1).inner_text())
            )
            if dtstart < today:
                continue

            summary = self._collapse_whitespace(
                await cells.nth(7).locator("a[href]").first.inner_text()
            )

            event_data: ParserBase.Row = {
                "summary": summary,
                "dtstart": dtstart.isoformat(),
            }

            url = await row.get_attribute("data-rowlink")
            if url:
                event_data["url"] = url

            [track, town] = await self._extract_location_text(cells.nth(4))
            if track:
                event_data["track"] = track
            if town:
                event_data["town"] = town

            kart_model = self._collapse_whitespace(await cells.nth(6).inner_text())
            if kart_model:
                event_data["kart_model"] = kart_model

            page_rows.append({"event": event_data})

        return page_rows

    def finalize_result(self, aggregate: ParserBase.Result) -> ParserBase.Result:
        # TODO: Unify same-day same-track events
        return self._dedupe(aggregate)

    async def _solve_recaptcha_if_present(self, page: Page) -> bool:
        if await page.locator(self.recaptcha_form_selector).count() == 0:
            return False

        api_key = os.getenv("TEN_CAPTCHA_API_KEY")
        if not api_key:
            raise RuntimeError(
                f"[{self.id}] Encountered a reCAPTCHA page but TEN_CAPTCHA_API_KEY is not set."
            )

        playwright_captcha = importlib.import_module("playwright_captcha")
        tencaptcha_solver = importlib.import_module(
            "playwright_captcha.solvers.api.tencaptcha.tencaptcha_solver"
        )
        tencaptcha_async_solver = importlib.import_module(
            "playwright_captcha.solvers.api.tencaptcha.tencaptcha.async_solver"
        )
        CaptchaType = getattr(playwright_captcha, "CaptchaType")
        FrameworkType = getattr(playwright_captcha, "FrameworkType")
        TenCaptchaSolver = getattr(tencaptcha_solver, "TenCaptchaSolver")
        AsyncTenCaptcha = getattr(tencaptcha_async_solver, "AsyncTenCaptcha")

        print(f"[{self.id}] Solving reCAPTCHA challenge...")
        captcha_client = AsyncTenCaptcha(api_key)
        async with TenCaptchaSolver(
            framework=FrameworkType.CAMOUFOX,
            page=page,
            async_ten_captcha_client=captcha_client,
        ) as solver:
            result = await solver.solve_captcha(
                captcha_container=page.locator(".g-recaptcha").first,
                captcha_type=CaptchaType.RECAPTCHA_V2,
            )
            if not isinstance(result, str) or not result:
                raise RuntimeError(f"[{self.id}] Failed to solve reCAPTCHA challenge.")
            print(f"[{self.id}] reCAPTCHA solved, submitting verification form...")

        await self._submit_captcha_form(page)

        await page.wait_for_selector(
            self.recaptcha_form_selector,
            state="detached",
            timeout=CONTENT_TIMEOUT_MS,
        )

        return True

    async def _submit_captcha_form(self, page: Page) -> None:
        script = """
            (formSelector) => {
                const form = document.querySelector(formSelector);
                const captcha = document.querySelector('.g-recaptcha');
                const tokenField = document.querySelector('#g-recaptcha-response');
                if (!(form instanceof HTMLFormElement) || !(captcha instanceof HTMLElement)) {
                    return false;
                }

                const token = tokenField instanceof HTMLTextAreaElement
                    ? tokenField.value.trim()
                    : '';
                if (!token) {
                    return false;
                }

                const callbackName = captcha.dataset.callback;
                if (callbackName) {
                    const callback = callbackName
                        .split('.')
                        .reduce((current, key) => current?.[key], window);
                    if (typeof callback === 'function') {
                        callback(token);
                        return true;
                    }
                }

                if (typeof form.requestSubmit === 'function') {
                    form.requestSubmit();
                    return true;
                }

                form.submit();
                return true;
            }
        """
        submitted = await page.evaluate(script, self.recaptcha_form_selector)
        if not submitted:
            raise RuntimeError(f"[{self.id}] Token solved, but form was not submitted.")

    async def _apply_hungary_filter_if_needed(self, page: Page) -> bool:
        hu = page.locator(self.country_hungary_option_selector + ":not([selected])")
        if await hu.count() == 0:
            return False

        await hu.first.evaluate("option => option.setAttribute('selected', 'selected')")

        submit = page.locator("button#search-race-submit")
        if await submit.count() != 1:
            raise ValueError(f"[{self.id}] Could not find the search form submit button.")
        await submit.first.click()

        # On page reload, the flag appears in the form
        await page.wait_for_selector(
            self.country_hungary_search_selector,
            timeout=CONTENT_TIMEOUT_MS,
        )

        return True

    async def _load_all_listing_pages(self, page: Page) -> None:
        pagination = page.locator(self.pagination_selector)
        if await pagination.count() == 0:
            return

        pagination = pagination.first
        for _ in range(MAXIMUM_PAGINATION_COUNT):
            if await page.locator(self.pagination_more_selector).count() == 0:
                return

            current_page = await pagination.get_attribute("data-current-page")
            if not current_page:
                raise ValueError(f"[{self.id}] Cannot find current page number in pagination.")

            print(f"[{self.id}] Loading additional Sodi listing page {current_page}...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

            await page.wait_for_selector(
                self.pagination_selector + f':not([data-current-page="{current_page}"])',
                timeout=CONTENT_TIMEOUT_MS,
            )

    async def _extract_location_text(self, cell: Locator) -> list[str]:
        text = self._collapse_whitespace(await cell.inner_text())

        subtext_elem = cell.locator("small")
        subtext = ""
        if await subtext_elem.count() > 0:
            subtext = self._collapse_whitespace(await subtext_elem.first.inner_text())
        if subtext and text.endswith(subtext):
            text = self._collapse_whitespace(text[: -len(subtext)])

        return [text, subtext]
