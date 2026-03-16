"""
models.py – Pydantic-free dataclasses for accident records.
Using plain dataclasses + slots for performance at scale (~2000+ records).
"""
from dataclasses import dataclass, field


@dataclass(slots=True)
class AccidentImage:
    src: str
    alt: str
    caption: str
    full_src: str | None = None  # link to full-res file page


@dataclass(slots=True)
class AircraftEntry:
    """Infobox fields for one aircraft in a multi-aircraft incident."""
    aircraft_type: str | None = None
    aircraft_name: str | None = None
    operator: str | None = None
    iata_flight: str | None = None
    icao_flight: str | None = None
    call_sign: str | None = None
    registration: str | None = None
    flight_origin: str | None = None
    destination: str | None = None
    stopover: str | None = None
    occupants: str | None = None
    passengers: str | None = None
    crew: str | None = None
    fatalities: str | None = None
    injuries: str | None = None
    survivors: str | None = None
    # Any infobox rows not covered by the fields above
    extra: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class AccidentRecord:
    # ── Index-page fields ─────────────────────────────────────────────────────
    decade: str
    year: str
    index_summary: str   # raw sentence from the index list
    wikipedia_url: str

    # ── Infobox fields (keyed exactly as Wikipedia labels them) ───────────────
    date: str | None = None
    summary_infobox: str | None = None
    site: str | None = None
    aircraft_type: str | None = None
    aircraft_name: str | None = None
    operator: str | None = None
    iata_flight: str | None = None
    icao_flight: str | None = None
    call_sign: str | None = None
    registration: str | None = None
    flight_origin: str | None = None
    destination: str | None = None
    stopover: str | None = None
    occupants: str | None = None
    passengers: str | None = None
    crew: str | None = None
    fatalities: str | None = None
    injuries: str | None = None
    survivors: str | None = None
    ground_fatalities: str | None = None
    ground_injuries: str | None = None
    # Geographic coordinates of the accident site
    latitude: float | None = None
    longitude: float | None = None
    coordinates_raw: str | None = None  # original string as found on the page
    # Catch-all for any other infobox rows
    infobox_extra: dict = field(default_factory=dict)

    # ── Full article text by section ──────────────────────────────────────────
    sections: dict[str, str] = field(default_factory=dict)
    # Specific high-value sections extracted separately
    investigation_text: str | None = None
    cause_text: str | None = None
    aircraft_specs_text: str | None = None
    technical_details_text: str | None = None
    accident_description: str | None = None

    # ── Images ────────────────────────────────────────────────────────────────
    images: list[AccidentImage] = field(default_factory=list)

    # ── Per-aircraft breakdowns (multi-aircraft incidents) ─────────────────────
    # Each element holds infobox fields for one aircraft (e.g. mid-air
    # collisions with "First aircraft" / "Second aircraft" infobox sections).
    # Empty list for single-aircraft accidents.
    aircraft_list: list["AircraftEntry"] = field(default_factory=list)

    # ── Meta ──────────────────────────────────────────────────────────────────
    page_title: str | None = None
    scrape_error: str | None = None
