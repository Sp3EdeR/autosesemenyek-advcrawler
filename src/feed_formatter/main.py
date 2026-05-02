import argparse
import json
import re
from collections.abc import Iterable
from datetime import datetime
from html import escape as esc_html
from typing import TextIO
from urllib.parse import quote_plus

def load_events(path: str) -> Iterable[dict]:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)

    return (
        {**payload, "src_id": src_key}
        for item in raw
        for src_key, payload in item.items()
    )

def filter_events(events: Iterable[dict]) -> Iterable[dict]:
    invalid = []
    for ev in events:
        if ev.get("dtstart") and ev.get("summary"):
            yield ev
        else:
            invalid.append(ev)

    if invalid:
        print("Events missing dtstart or summary:")
        for ev in invalid:
            print(f"  - {ev}")

def write_markdown(strm: TextIO, events: list[dict]) -> int:
    def fmt_dt(value: str | None) -> str:
        if not value:
            return ""

        stripped = value.strip()

        if "T" not in stripped and " " not in stripped:
            return stripped

        try:
            parsed = datetime.fromisoformat(stripped.replace("Z", "+00:00"))
        except ValueError:
            return stripped

        return parsed.strftime("%Y-%m-%d %H:%M")

    def fmt_loc(value: str | None) -> str:
        if not value:
            return ""

        location = value.strip()
        maps_url = f"https://www.google.com/maps?q={quote_plus(location)}"
        location = esc_html(location)
        return f'<a href="{maps_url}" target="_blank" rel="noopener noreferrer">{location}</a>'

    strm.write("# Aut\u00f3s Esem\u00e9nyek Feed\n\n")
    strm.write(
        "Ez a k\u00fcl\u00f6nb\u00f6z\u0151 aut\u00f3s napt\u00e1rakb\u00f3l gy\u0171jt\u00f6tt, "
        "egyes\u00edtett esem\u00e9ny lista.\n\n"
    )

    is_html = re.compile(r"<(?:p|br)\b[^>]*?/?>")

    for ev in events:
        summary = esc_html(ev["summary"])
        url = ev.get("url")

        if url:
            strm.write(
                f'## <a href="{url}" target="_blank" rel="noopener noreferrer">{summary}</a>\n\n'
            )
        else:
            strm.write(f"## {summary}\n\n")

        strm.write("| | | |\n|---|---|---|\n")
        strm.write(
            f"| {fmt_dt(ev['dtstart'])} | {fmt_dt(ev.get('dtend'))} | "
            f"{fmt_loc(ev.get('location'))} |\n\n"
        )

        skip = {"summary", "url", "dtstart", "dtend", "location"}
        for key, value in ev.items():
            if key in skip or not value:
                continue
            key_f = key.replace('_', ' ').capitalize()
            val_f = value.strip(" \n\t")
            if not is_html.search(val_f):
                val_f = esc_html(val_f).replace("\n", "<br>\n")

            if is_html.search(val_f):
                strm.write(f"<details><summary><b>{key_f}</b></summary>\n{val_f}\n</details>\n")
            else:
                strm.write(f"<details><summary><b>{key_f}</b>: {val_f}</summary>\n</details>\n")

        strm.write("\n---\n\n")

    return len(events)

def main() -> None:
    parser = argparse.ArgumentParser(description="Format crawled events into Markdown")
    parser.add_argument("input", help="Path to the input events JSON file")
    parser.add_argument("output", help="Path to the output Markdown file")
    args = parser.parse_args()

    events = load_events(args.input)
    events = filter_events(events)
    events = sorted(events, key=lambda e: e["dtstart"])

    with open(args.output, "w", encoding="utf-8") as f:
        count = write_markdown(f, events)
        print(f"Wrote {count} events to {args.output}")


if __name__ == "__main__":
    main()
