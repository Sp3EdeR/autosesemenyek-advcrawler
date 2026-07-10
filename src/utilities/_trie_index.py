from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Generator, Iterable

import marisa_trie

from .text import _normalize_text_with_index, normalize_text_for_match

_WORD_START_RE = re.compile(r"(?<!\w)\w")


@dataclass(frozen=True, slots=True)
class TrieMatch:
    matched_key: str
    start: int
    end: int


def build_trie_index(strings: Iterable[str]) -> marisa_trie.Trie:
    """Build a trie over normalized string keys."""
    normalized_keys = (normalize_text_for_match(value) for value in strings if value)
    return marisa_trie.Trie(normalized_keys)


def find_prefix_matches(text: str, trie: marisa_trie.Trie) -> Generator[TrieMatch, None, None]:
    """Find trie matches that start at word boundaries in the original string."""
    normalized_text, normalized_to_original = _normalize_text_with_index(text)

    for word_start in _WORD_START_RE.finditer(normalized_text):
        normalized_start = word_start.start()
        for matched_key in trie.prefixes(normalized_text[normalized_start:]):
            normalized_end = normalized_start + len(matched_key) - 1
            start = normalized_to_original[normalized_start]
            end = normalized_to_original[normalized_end] + 1
            yield TrieMatch(matched_key=matched_key, start=start, end=end)
