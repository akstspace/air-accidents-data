# Aircraft Crashes Data

This dataset contains information about aircraft accidents, extracted from Wikipedia.

## Data Format

The data is stored in JSON Lines format (`.jsonl`), where each line is a valid JSON object representing an individual accident record.

`last_updated.json` is generated alongside the dataset and records when the latest successful refresh completed, how many records were written, and the scrape configuration used.

## Usage

You can process this file using any JSON Lines parser in your preferred programming language.

## Automated Refresh

The repository includes a GitHub Actions workflow at [.github/workflows/weekly-refresh.yml](.github/workflows/weekly-refresh.yml) that runs weekly and on manual dispatch. It uses the scraper in [scraper](scraper) to:

- rebuild `accidents.jsonl`
- replace the root dataset file only after a successful scrape
- write `last_updated.json`
- commit and push those refreshed files back to `main`

## Credits

This dataset is derived from Wikipedia content, primarily the page [List of accidents and incidents involving commercial aircraft](https://en.wikipedia.org/wiki/List_of_accidents_and_incidents_involving_commercial_aircraft), with linked accident articles as sources.
