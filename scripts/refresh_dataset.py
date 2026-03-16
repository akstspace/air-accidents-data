from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from aircraft_tracker.exporter import export_jsonl
from aircraft_tracker.scraper import fetch_index_entries, scrape_all


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refresh the root accidents.jsonl dataset and write last_updated.json.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("accidents.jsonl"),
        help="Path to the root JSONL dataset file.",
    )
    parser.add_argument(
        "--metadata-output",
        type=Path,
        default=Path("last_updated.json"),
        help="Path to the metadata file written after a successful refresh.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Maximum number of concurrent scrape requests.",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.4,
        help="Delay between requests in seconds.",
    )
    return parser.parse_args()


async def scrape_records(concurrency: int, delay: float):
    entries = await fetch_index_entries()
    return await scrape_all(entries, concurrency=concurrency, delay=delay)


def write_metadata(
    metadata_path: Path,
    dataset_path: Path,
    record_count: int,
    started_at: datetime,
    completed_at: datetime,
    concurrency: int,
    delay: float,
) -> None:
    payload = {
        "dataset": dataset_path.name,
        "source": "Wikipedia: List of accidents and incidents involving commercial aircraft",
        "source_url": "https://en.wikipedia.org/wiki/List_of_accidents_and_incidents_involving_commercial_aircraft",
        "status": "success",
        "record_count": record_count,
        "generated_at": completed_at.isoformat().replace("+00:00", "Z"),
        "started_at": started_at.isoformat().replace("+00:00", "Z"),
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
        "duration_seconds": round((completed_at - started_at).total_seconds(), 2),
        "scrape_config": {
            "concurrency": concurrency,
            "delay_seconds": delay,
        },
    }
    metadata_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    output_path = args.output.resolve()
    metadata_path = args.metadata_output.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.parent.mkdir(parents=True, exist_ok=True)

    started_at = datetime.now(UTC)
    records = asyncio.run(scrape_records(args.concurrency, args.delay))
    completed_at = datetime.now(UTC)

    # Write to a temp file first so the root dataset is only replaced on success.
    with TemporaryDirectory() as tmpdir:
        tmp_output = Path(tmpdir) / output_path.name
        export_jsonl(records, tmp_output)
        output_path.write_bytes(tmp_output.read_bytes())

    write_metadata(
        metadata_path=metadata_path,
        dataset_path=output_path,
        record_count=len(records),
        started_at=started_at,
        completed_at=completed_at,
        concurrency=args.concurrency,
        delay=args.delay,
    )


if __name__ == "__main__":
    main()
