from __future__ import annotations

import os
import re

import marisa_trie

# Hungarian inflection suffixes until word boundary, excluding street suffixes
_HU_TOWN_POSTFIXES_RE = re.compile(r"(?:n|en|on|ön|ban|ben|i(?!\s+(?:út|utca|u\.|köz)))?(?=\W|$)")
_HU_TOWNS_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "towns")
_WORD_ITERATOR_RE = re.compile(r"\w+")

_TOWNS: marisa_trie.Trie | None = None
_TOWN_MAP: dict[str, str] | None = None


def _get_towns_trie() -> marisa_trie.Trie:
    """Return a trie built from the Hungarian towns file."""
    def load_towns():
        for town_file in os.listdir(_HU_TOWNS_DIR):
            if not town_file.endswith("towns.txt"):
                continue
            with open(os.path.join(_HU_TOWNS_DIR, town_file), 'r', encoding='UTF-8') as file:
                for line in file:
                    town = line.rstrip().lower()
                    if town:
                        yield town

    global _TOWNS
    if _TOWNS is None:
        _TOWNS = marisa_trie.Trie(load_towns())
    return _TOWNS


def _get_town_map() -> dict[str, str]:
    """Return a dictionary of town name replacements."""
    def load_replacers() -> dict[str, str]:
        replacers: dict[str, str] = {}
        replacements_file = os.path.join(_HU_TOWNS_DIR, "explicit-map.csv")
        with open(replacements_file, 'r', encoding='UTF-8') as file:
            for line in file:
                parts = line.rstrip().lower().split(";", 1)
                replacers[parts[0]] = parts[1]
        return replacers

    global _TOWN_MAP
    if _TOWN_MAP is None:
        _TOWN_MAP = load_replacers()
    return _TOWN_MAP


def _get_hu_special_inflections(word: str) -> list[str]:
    search_words = [ word ]
    last_char = word[-1:]
    if last_char in ("i", "n"):
        # Find Ja`noshalmi -> Ja`noshalma
        if last_char == "i":
            search_words.append(word[:-1] + "a")
        # Find Galgama`csa`n -> Galgama`csa
        elif word.endswith("án"):
            search_words.append(word[:-2] + "a")
        # Find Abd`aban -> Abda
        elif word.endswith("ában"):
            search_words.append(word[:-4] + "a")
    return search_words


def get_first_town_norm(text: str) -> str | None:
    """Return the first found town name in the given text."""
    text = text.lower()
    for match in _WORD_ITERATOR_RE.finditer(text):
        word = match.group(0)
        # If the word is a non-standard town name, return its normalized name
        replacement = _get_town_map().get(word)
        if replacement is not None:
            return replacement

        # Check if the word is a town's name with Hungarian inflection
        search_words = _get_hu_special_inflections(word)
        for search in search_words:
            for matched_key in _get_towns_trie().prefixes(search):
                if _HU_TOWN_POSTFIXES_RE.match(text, match.start() + len(matched_key)):
                    return matched_key

    return None
