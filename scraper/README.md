# Aircraft Tracker

Extracts exhaustive, high-detail data on commercial aviation accidents from Wikipedia for the period **1919–2026**. Covers ~2 000 incidents with infobox fields, full section text (investigation, causes, technical details, specs), geographic coordinates, and all images with captions.

---

## Features

- Async scraper with configurable concurrency and polite rate-limiting
- Parses every Wikipedia accident article: infobox, all section text, images
- **Latitude / longitude** extracted from Wikipedia's geo microformats and GeoHack links
- Dedicated extraction of investigation findings, probable causes, aircraft specs, technical details
- Full-resolution image URLs for every thumbnail/figure on the page
- Rich terminal UI — progress bar, stats panel, summary table
- Exports to **JSON**, **JSONL**, and **CSV** (split into `accidents.csv` + `images.csv`)
- CLI commands: `scrape`, `list-index`, `show`, `export`, `stats`

---

## Requirements

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) (package manager)

---

## Installation

```bash
# Clone / enter project
cd aircraft-tracking

# Create virtual environment and install all dependencies
uv venv --python 3.11
uv pip install -e .

# Activate (optional – all commands below use .venv/bin/ directly)
source .venv/bin/activate
```

---

## Quick Start

```bash
# Scrape one year, all formats
aircraft-tracker scrape --start-year 1977 --end-year 1977 --out ./data

# Scrape full 1919–2026 dataset (takes ~30–60 min depending on concurrency)
aircraft-tracker scrape --out ./data

# Browse the index without fetching articles
aircraft-tracker list-index --year 1977

# Pretty-print a single accident
aircraft-tracker show "https://en.wikipedia.org/wiki/Tenerife_airport_disaster"

# Re-export an existing JSON dump to CSV
aircraft-tracker export --input ./data/accidents.json --out ./data --format csv

# Dataset statistics from an existing dump
aircraft-tracker stats --input ./data/accidents.json
```

---

## CLI Reference

```
Usage: aircraft-tracker [OPTIONS] COMMAND [ARGS]...

 Aircraft Tracker – extract exhaustive data on commercial aviation accidents
 from Wikipedia (1919–2026).

 Options
   --install-completion   Install completion for the current shell.
   --show-completion      Show completion for the current shell.
   --help                 Show this message and exit.

 Commands
   scrape      Scrape Wikipedia for commercial aviation accidents (1919–2026).
   list-index  List accident entries from the Wikipedia index without fetching articles.
   show        Show detailed information for a single accident article.
   export      Export an existing accidents.json dump to CSV / JSONL.
   stats       Stats – show dataset statistics from an existing JSON dump.
```

---

### `scrape`

```
Usage: aircraft-tracker scrape [OPTIONS]

 Scrape Wikipedia for commercial aviation accidents (1919–2026).
 Fetches the main index, then visits each individual accident article to
 extract infobox data, all section text (investigation, causes, findings,
 specs), and images.

 Options
   --out          -o   PATH              Output directory for exported files.         [default: data]
   --start-year   -s   INTEGER           First year to include (e.g. 1970).
   --end-year     -e   INTEGER           Last year to include (e.g. 1979).
   --concurrency  -c   INTEGER [1..20]   Max simultaneous HTTP requests.              [default: 5]
   --delay        -d   FLOAT  [0.1..10]  Polite delay (seconds) between requests.     [default: 0.4]
   --format       -f   TEXT              Output format: json | jsonl | csv | all      [default: all]
   --pretty / --compact                  Pretty-print JSON output.                    [default: pretty]
   --no-summary                          Skip printing per-record summary table.
   --help                                Show this message and exit.
```

```bash
# Scrape a single decade with higher concurrency
aircraft-tracker scrape --start-year 1990 --end-year 1999 --concurrency 8 --out ./data/1990s

# Full dataset, compact JSON
aircraft-tracker scrape --out ./data --compact
```


---

### `list-index`

```
Usage: aircraft-tracker list-index [OPTIONS]

 List accident entries from the Wikipedia index without fetching articles.

 Options
   --year   -y   INTEGER   Filter to a specific year.
   --decade -d   TEXT      Filter by decade label (e.g. '1970s').
   --limit  -l   INTEGER   Max rows to display (0 = all).   [default: 0]
   --help                  Show this message and exit.
```

```bash
aircraft-tracker list-index --decade "1980s" --limit 50
aircraft-tracker list-index --year 1977
```

---

### `show`

```
Usage: aircraft-tracker show [OPTIONS] URL

 Show detailed information for a single accident article.

 Arguments
   url   TEXT   Full Wikipedia URL of the accident article.   [required]

 Options
   --help   Show this message and exit.
```

```bash
aircraft-tracker show "https://en.wikipedia.org/wiki/Air_India_Flight_182"
aircraft-tracker show "https://en.wikipedia.org/wiki/Tenerife_airport_disaster"
```

---

### `export`

```
Usage: aircraft-tracker export [OPTIONS]

 Export an existing accidents.json dump to CSV / JSONL.

 Options
   --input  -i   PATH   Path to existing accidents.json dump.         [required]
   --out    -o   PATH   Output directory for re-exported files.       [default: data]
   --format -f   TEXT   Output format: json | jsonl | csv | all       [default: all]
   --pretty / --compact                                                [default: pretty]
   --help               Show this message and exit.
```

```bash
aircraft-tracker export --input ./data/accidents.json --format csv --out ./data
```

---

### `stats`

```
Usage: aircraft-tracker stats [OPTIONS]

 Stats – show dataset statistics from an existing JSON dump.

 Options
   --input  -i   PATH   Path to accidents.json dump.   [required]
   --help               Show this message and exit.
```

```bash
aircraft-tracker stats --input ./data/accidents.json
```

---

## Output Schema

### `accidents.json` / `accidents.jsonl`

Each record is a JSON object with these top-level keys:

| Key | Type | Description |
|---|---|---|
| `page_title` | string | Wikipedia article title |
| `wikipedia_url` | string | Full article URL |
| `decade` | string | Index section heading (e.g. `"1970s"`) |
| `year` | string | Accident year |
| `date` | string | Full date from infobox |
| `summary_infobox` | string | One-line cause summary from infobox |
| `site` | string | Location from infobox |
| `aircraft_type` | string | Aircraft model |
| `aircraft_name` | string | Named registration / tail name |
| `operator` | string | Airline or operator |
| `iata_flight` | string | IATA flight number |
| `icao_flight` | string | ICAO flight number |
| `call_sign` | string | Radio call sign |
| `registration` | string | Aircraft registration |
| `flight_origin` | string | Departure airport |
| `destination` | string | Destination airport |
| `stopover` | string | Intermediate stop |
| `occupants` | string | Total people on board |
| `passengers` | string | Passenger count |
| `crew` | string | Crew count |
| `fatalities` | string | On-board fatalities |
| `injuries` | string | On-board injuries |
| `survivors` | string | On-board survivors |
| `ground_fatalities` | string | Ground fatalities (third parties) |
| `ground_injuries` | string | Ground injuries (third parties) |
| **`latitude`** | float\|null | Decimal latitude of accident site |
| **`longitude`** | float\|null | Decimal longitude of accident site |
| `coordinates_raw` | string | Original coordinate string from page |
| `investigation_text` | string | Full text of investigation/findings sections |
| `cause_text` | string | Full text of cause/probable cause sections |
| `aircraft_specs_text` | string | Full text of aircraft description sections |
| `technical_details_text` | string | Full text of technical detail sections |
| `accident_description` | string | Full text of the accident narrative sections |
| `sections` | object | All article sections keyed by heading |
| `infobox_extra` | object | Any infobox rows not mapped to known fields |
| **`aircraft_list`** | array | Per-aircraft breakdown for multi-aircraft incidents (see below) |
| `images` | array | See below |
| `index_summary` | string | Raw summary sentence from the index page |
| `scrape_error` | string | Set if the article could not be fetched |

#### `aircraft_list` entries

Populated for incidents involving more than one aircraft (mid-air collisions, runway collisions). Each entry mirrors the top-level aircraft fields scoped to that specific aircraft. When an article provides separate infobox sections labelled *First aircraft*, *Second aircraft*, etc., the scraper captures each independently and auto-sums totals (fatalities, occupants, passengers, crew, injuries, survivors) onto the parent record.

| Key | Description |
|---|---|
| `aircraft_type` | Aircraft model |
| `aircraft_name` | Named registration / tail name |
| `operator` | Airline or operator |
| `iata_flight` | IATA flight number |
| `icao_flight` | ICAO flight number |
| `call_sign` | Radio call sign |
| `registration` | Aircraft registration |
| `flight_origin` | Departure airport |
| `destination` | Destination airport |
| `stopover` | Intermediate stop |
| `occupants` | People on board this aircraft |
| `passengers` | Passengers on this aircraft |
| `crew` | Crew on this aircraft |
| `fatalities` | Fatalities on this aircraft |
| `injuries` | Injuries on this aircraft |
| `survivors` | Survivors from this aircraft |

#### `images` array entries

| Key | Description |
|---|---|
| `src` | Thumbnail URL |
| `full_src` | Full-resolution Wikimedia URL |
| `alt` | Alt text |
| `caption` | Human-readable caption |

---

### `accidents.csv`

Flat table — one row per accident. Contains all scalar fields from the schema above (including `latitude`, `longitude`, `coordinates_raw`) plus `image_count`.

### `images.csv`

One row per image. Columns: `wikipedia_url`, `page_title`, `year`, `image_index`, `src`, `full_src`, `alt`, `caption`.

---

## Project Layout

```
aircraft-tracking/
├── LICENSE
├── pyproject.toml                  # uv/hatch project config, dependency pins
├── README.md
├── tests/
│   └── smoke_test.py               # Import + sanity tests (run with pytest)
└── src/
    └── aircraft_tracker/
        ├── __init__.py
        ├── models.py               # AccidentRecord, AircraftEntry, AccidentImage dataclasses
        ├── scraper.py              # Async HTTP + BeautifulSoup parsing; coordinate extraction
        ├── exporter.py             # JSON / JSONL / CSV writers
        └── cli.py                  # Typer CLI with Rich UI
```

---

## Notes

- Wikipedia does not provide an API for full article HTML, so scraping uses the public web pages. The user-agent header identifies this tool for transparency.
- Coordinates are extracted from geo microformats (`<span class="geo">`), split latitude/longitude spans, and GeoHack URL parameters — covering the vast majority of geotagged articles. Articles without coordinates will have `latitude: null`.
- Investigation and cause text coverage depends on Wikipedia article quality; older accidents (pre-1950s) typically have shorter articles.
- The `--concurrency` default of 5 and `--delay` of 0.4 s stay well within Wikipedia's [rate-limit guidelines](https://www.mediawiki.org/wiki/API:Etiquette).

---

## Contributing

1. Fork the repository and create a feature branch.
2. Install dev dependencies: `uv pip install -e '.[dev]'` (or `uv pip install -e . && uv pip install pytest ruff`).
3. Run the smoke tests before submitting: `pytest tests/`.
4. Format and lint with [Ruff](https://docs.astral.sh/ruff/): `ruff check . && ruff format .`.
5. Open a pull request with a clear description of the change.

Please do not commit generated data files — the `data/` directory is gitignored.

---

## License

[MIT](LICENSE) © 2026 akshayt
