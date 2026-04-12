# autosesemenyek-advcrawler

Crawler project for extracting event data from:

- https://m-ring.hu/
- https://euroring.hu/esemenynaptar2/

The crawler uses:

- Camoufox (async API)

## Requirements

- Python 3.12+
- Poetry

## Setup

Recommended in-source installation setting in the project directory:

```bash
poetry config virtualenvs.in-project true --local
```

Create the virtual env and add needed dependencies to it:

```bash
poetry install
poetry run python -m camoufox fetch
```

## Run

```bash
poetry run crawl
```

or

```bash
poetry run python -m autosesemenyek_advcrawler.main
```

Output files are generated in the project root:

- `m-ring_race.json`
- `m-ring.json`
- `euroring.json`

## Notes

- The crawler advances page-by-page using each calendar's right-arrow next button.
- It stops when a loaded page is fully empty (no visible event markers/content), not just when a specific extraction has no matches.
- In CI, the GitHub Actions workflow runs on `ubuntu-latest` with Python 3.12 and pushes the generated JSON to the `crawled` branch instead of uploading build artifacts.
