"""
exporter.py – Export AccidentRecord lists to JSON and CSV.

JSON  → orjson (compact or pretty-printed), one file.
CSV   → two files: accidents.csv (flat fields) + images.csv (one row per image).
"""
import csv
import dataclasses
from collections.abc import Iterable
from pathlib import Path

import orjson

from .models import AccidentRecord, AccidentImage, AircraftEntry

# ──────────────────────────────────────────────────────────────────────────────
# Serialisation helpers
# ──────────────────────────────────────────────────────────────────────────────

def _record_to_dict(rec: AccidentRecord) -> dict:
    """Convert an AccidentRecord (dataclass) to a plain dict with nested images."""
    d = {
        "page_title": rec.page_title,
        "wikipedia_url": rec.wikipedia_url,
        "decade": rec.decade,
        "year": rec.year,
        "index_summary": rec.index_summary,
        "date": rec.date,
        "summary_infobox": rec.summary_infobox,
        "site": rec.site,
        "aircraft_type": rec.aircraft_type,
        "aircraft_name": rec.aircraft_name,
        "operator": rec.operator,
        "iata_flight": rec.iata_flight,
        "icao_flight": rec.icao_flight,
        "call_sign": rec.call_sign,
        "registration": rec.registration,
        "flight_origin": rec.flight_origin,
        "destination": rec.destination,
        "stopover": rec.stopover,
        "occupants": rec.occupants,
        "passengers": rec.passengers,
        "crew": rec.crew,
        "fatalities": rec.fatalities,
        "injuries": rec.injuries,
        "survivors": rec.survivors,
        "ground_fatalities": rec.ground_fatalities,
        "ground_injuries": rec.ground_injuries,
        "latitude": rec.latitude,
        "longitude": rec.longitude,
        "coordinates_raw": rec.coordinates_raw,
        "investigation_text": rec.investigation_text,
        "cause_text": rec.cause_text,
        "aircraft_specs_text": rec.aircraft_specs_text,
        "technical_details_text": rec.technical_details_text,
        "accident_description": rec.accident_description,
        "sections": rec.sections,
        "infobox_extra": rec.infobox_extra,
        "aircraft_list": [
            {
                "aircraft_type": ac.aircraft_type,
                "aircraft_name": ac.aircraft_name,
                "operator": ac.operator,
                "iata_flight": ac.iata_flight,
                "icao_flight": ac.icao_flight,
                "call_sign": ac.call_sign,
                "registration": ac.registration,
                "flight_origin": ac.flight_origin,
                "destination": ac.destination,
                "stopover": ac.stopover,
                "occupants": ac.occupants,
                "passengers": ac.passengers,
                "crew": ac.crew,
                "fatalities": ac.fatalities,
                "injuries": ac.injuries,
                "survivors": ac.survivors,
                **ac.extra,
            }
            for ac in rec.aircraft_list
        ],
        "images": [
            {
                "src": img.src,
                "full_src": img.full_src,
                "alt": img.alt,
                "caption": img.caption,
            }
            for img in rec.images
        ],
        "scrape_error": rec.scrape_error,
    }
    return d


# ──────────────────────────────────────────────────────────────────────────────
# JSON export
# ──────────────────────────────────────────────────────────────────────────────

# CSV flat field order (for accidents.csv)
CSV_ACCIDENT_FIELDS = [
    "page_title",
    "wikipedia_url",
    "decade",
    "year",
    "date",
    "summary_infobox",
    "site",
    "aircraft_type",
    "aircraft_name",
    "operator",
    "iata_flight",
    "icao_flight",
    "call_sign",
    "registration",
    "flight_origin",
    "destination",
    "stopover",
    "occupants",
    "passengers",
    "crew",
    "fatalities",
    "injuries",
    "survivors",
    "ground_fatalities",
    "ground_injuries",
    "latitude",
    "longitude",
    "coordinates_raw",
    "investigation_text",
    "cause_text",
    "aircraft_specs_text",
    "technical_details_text",
    "accident_description",
    "index_summary",
    "image_count",
    "aircraft_list",
    "scrape_error",
]


def export_json(
    records: list[AccidentRecord],
    output_path: Path,
    pretty: bool = True,
) -> Path:
    """Write all records to a single JSON file."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = [_record_to_dict(r) for r in records]

    option = orjson.OPT_INDENT_2 if pretty else 0
    raw = orjson.dumps(data, option=option)
    output_path.write_bytes(raw)
    return output_path


def export_jsonl(
    records: list[AccidentRecord],
    output_path: Path,
) -> Path:
    """Write records as newline-delimited JSON (one object per line)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("wb") as f:
        for rec in records:
            f.write(orjson.dumps(_record_to_dict(rec)))
            f.write(b"\n")
    return output_path


# ──────────────────────────────────────────────────────────────────────────────
# CSV export
# ──────────────────────────────────────────────────────────────────────────────

def export_csv(
    records: list[AccidentRecord],
    output_dir: Path,
) -> tuple[Path, Path]:
    """
    Export two CSV files:
      accidents.csv  – one row per accident (flat fields)
      images.csv     – one row per image, keyed by wikipedia_url

    Returns (accidents_path, images_path).
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    accidents_path = output_dir / "accidents.csv"
    images_path = output_dir / "images.csv"

    # ── accidents.csv ────────────────────────────────────────────────────────
    with accidents_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_ACCIDENT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            d = _record_to_dict(rec)
            d["image_count"] = len(rec.images)
            # CSV cannot represent None; use empty string for absent values.
            # Lists/dicts (e.g. aircraft_list) are JSON-serialised inline.
            def _csv_val(v):
                if v is None:
                    return ""
                if isinstance(v, (list, dict)):
                    return orjson.dumps(v).decode()
                return v
            writer.writerow({k: _csv_val(v) for k, v in d.items()})

    # ── images.csv ───────────────────────────────────────────────────────────
    image_fields = ["wikipedia_url", "page_title", "year", "image_index", "src", "full_src", "alt", "caption"]
    with images_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=image_fields, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            for idx, img in enumerate(rec.images):
                writer.writerow(
                    {
                        "wikipedia_url": rec.wikipedia_url,
                        "page_title": rec.page_title,
                        "year": rec.year,
                        "image_index": idx,
                        "src": img.src,
                        "full_src": img.full_src,
                        "alt": img.alt,
                        "caption": img.caption,
                    }
                )

    return accidents_path, images_path
