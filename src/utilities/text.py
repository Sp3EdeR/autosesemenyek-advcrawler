from __future__ import annotations

import unicodedata


def normalize_text_for_match(text: str) -> str:
    """Normalize text for matching by casefolding and removing diacritics."""
    normalized_text, _ = _normalize_text_with_index(text)
    return normalized_text


def _normalize_text_with_index(text: str) -> tuple[str, list[int]]:
    """Normalize text and keep the original character index for each output character."""
    normalized_chars: list[str] = []
    normalized_to_original: list[int] = []

    for index, char in enumerate(text):
        lowered = char.casefold()
        normalized = unicodedata.normalize("NFD", lowered)
        for normalized_char in normalized:
            if unicodedata.category(normalized_char) == "Mn":
                continue
            normalized_chars.append(normalized_char)
            normalized_to_original.append(index)

    return "".join(normalized_chars), normalized_to_original