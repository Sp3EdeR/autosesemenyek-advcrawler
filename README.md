# autosesemenyek-advcrawler

Crawler project for extracting event data from various sources for the
[Autós Események](https://github.com/Sp3EdeR/autosesemenyek) project.

The crawler uses:

- Camoufox (async API)

## Requirements

- Python 3.12+
- Poetry

## Setting up and running the crawler

If you want to develop or debug this project, it is recommended to follow the directions below, in
the [Developing and debugging section](#developing-and-debugging).

Create the virtual env and add needed dependencies to it:

```bash
poetry install
poetry run python -m camoufox fetch
```

Run the crawler:

```bash
poetry run crawl
```

or

```bash
poetry run python -m event_crawler.main
```

By default, the output is generated at `$CWD/crawled.json`.

For more information about available command-line options, run:

```bash
poetry run crawl --help
```

## Developing and debugging

First, set the created virtual environment's location to be within the project folder with the
following command. Run this command inside the `autosesemenyek-advcrawler` directory.

```bash
poetry config virtualenvs.in-project true --local
```

After this, you can install Poetry using the process defined in the
[Setting up and running the crawler](#setting-up-and-running-the-crawler). There is no need to run
the crawler.

Use the following Visual Studio Code launch configuration if you use this IDE, or convert this
launch configuration to your favourite IDE's configuration. This configuration creates its output
under the `crawled` subdirectory.

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Debug Event Crawler",
      "type": "debugpy",
      "request": "launch",
      "python": "${workspaceFolder}\\.venv\\Scripts\\python.exe",
      "module": "event_crawler.main",
      "cwd": "${workspaceFolder}",
      "env": {
        "PYTHONPATH": "${workspaceFolder}/src"
      },
      "args": [
        "--output",
        "${workspaceFolder}/crawled/events.json",
      ],
      "console": "integratedTerminal",
      "justMyCode": true
    }
  ]
}
```

## Project outputs

This project has an automatic GitHub actions job set up to run crawling jobs regularly. See the
latest data here:

* https://raw.githubusercontent.com/Sp3EdeR/autosesemenyek-advcrawler/refs/heads/crawled/events.json
