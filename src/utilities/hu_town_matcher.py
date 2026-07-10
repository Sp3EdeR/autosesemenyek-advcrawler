from __future__ import annotations

import os
import re

from ._trie_index import build_trie_index, find_prefix_matches

HU_TOWN_POSTFIXES_RE = re.compile(r"^(?:n|en|on|ön|i|ban|ben)?(?=\W|$)", re.IGNORECASE)
HU_TOWN_POSTFIXES_LEN = 3
HU_TOWNS_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), "hu-towns", "towns.txt")

_TRIE = None


def _get_towns_trie():
    """Return a trie built from the Hungarian towns file."""
    def load_towns():
        with open(HU_TOWNS_FILE, 'r', encoding='UTF-8') as file:
            for line in file:
                yield line.rstrip()

    global _TRIE
    if _TRIE is None:
        _TRIE = build_trie_index(load_towns())
    return _TRIE


def get_first_town_norm(text: str) -> str | None:
    """Return the first town name matched in the text, already normalized."""
    matches = find_prefix_matches(text, _get_towns_trie())
    for match in matches:
        # Only accept full word matches with Hungarian inflections
        word_postfix = text[match.end : match.end + HU_TOWN_POSTFIXES_LEN + 1]
        if HU_TOWN_POSTFIXES_RE.match(word_postfix):
            return match.matched_key

    return None
