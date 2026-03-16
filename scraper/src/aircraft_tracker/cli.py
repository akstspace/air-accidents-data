"""
cli.py – Typer-based CLI with Rich UI for aircraft-tracker.

Commands
────────
  aircraft-tracker scrape        Full scrape (all 1919-2026), optional year range
  aircraft-tracker list-index    Print index entries without fetching articles
  aircraft-tracker show          Pretty-print a single accident by URL
  aircraft-tracker export        Re-export an existing JSON dump to CSV / JSONL
  aircraft-tracker stats         Show statistics from an existing JSON dump

Usage examples
──────────────
  aircraft-tracker scrape --start-year 1970 --end-year 1979 --out ./data
  aircraft-tracker scrape --concurrency 8 --out ./data
  aircraft-tracker list-index --year 1977
  aircraft-tracker show --url https://en.wikipedia.org/wiki/Tenerife_airport_disaster
  aircraft-tracker export --input ./data/accidents.json --out ./data
  aircraft-tracker stats --input ./data/accidents.json
"""
import asyncio
import csv
import dataclasses
from pathlib import Path

import typer
from rich import box
from rich.columns import Columns
from rich.console import Console
from rich.live import Live
from rich.markdown import Markdown
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.rule import Rule
from rich.table import Table
from rich.text import Text

from .exporter import export_csv, export_json, export_jsonl
from .models import AccidentRecord
from .scraper import fetch_index_entries, scrape_all

app = typer.Typer(
    name="aircraft-tracker",
    help="[bold cyan]Aircraft Tracker[/bold cyan] – extract exhaustive data on "
    "commercial aviation accidents from Wikipedia (1919–2026).",
    rich_markup_mode="rich",
    add_completion=True,
)

console = Console(stderr=False)

# Core fields checked during audit
_AUDIT_CORE_FIELDS = [
    "date", "aircraft_type", "operator", "fatalities",
    "site", "registration", "flight_origin", "destination",
    "latitude", "longitude", "summary_infobox",
]


def _missing(v) -> bool:
    """True when a field value is absent (None, empty string, or empty list)."""
    return v is None or v == "" or v == []


def _is_irrelevant_record(r: dict) -> bool:
    """
    Return True if a record has no aviation-accident signals at all —
    i.e. it was scraped from a non-accident Wikipedia page such as a list,
    a person biography, or a natural-disaster article.
    """
    _aviation_extra = {"aircraft type", "aircraft name", "registration",
                       "flight origin", "destination", "type"}
    has_direct = any(
        r.get(f) for f in ("aircraft_type", "operator", "date", "site",
                           "registration", "fatalities")
    )
    extra_keys = {k.lower() for k in r.get("infobox_extra", {}).keys()}
    return not has_direct and not (_aviation_extra & extra_keys)


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _progress_bar() -> Progress:
    return Progress(
        SpinnerColumn(),
        TextColumn("[bold blue]{task.description}"),
        BarColumn(bar_width=40),
        MofNCompleteColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
        transient=False,
    )


def _make_summary_table(records: list[AccidentRecord]) -> Table:
    table = Table(
        title=f"[bold]Scraped {len(records)} Accident Records[/bold]",
        box=box.ROUNDED,
        show_lines=False,
        highlight=True,
        expand=True,
    )
    table.add_column("Year", style="cyan", no_wrap=True, min_width=6)
    table.add_column("Title", style="bold white", ratio=3)
    table.add_column("Aircraft Type", style="yellow", ratio=2)
    table.add_column("Operator", style="green", ratio=2)
    table.add_column("Fatalities", style="red", justify="right", min_width=10)
    table.add_column("Images", style="magenta", justify="right", min_width=7)
    table.add_column("Errors", style="dim red", justify="center", min_width=6)

    for rec in records:
        fatal = rec.fatalities or "–"
        err = "✗" if rec.scrape_error else ""
        table.add_row(
            rec.year,
            rec.page_title or rec.index_summary[:60],
            rec.aircraft_type or "–",
            rec.operator or "–",
            fatal,
            str(len(rec.images)),
            err,
        )
    return table


def _make_stats_panel(records: list[AccidentRecord]) -> Panel:
    total = len(records)
    errors = sum(1 for r in records if r.scrape_error)
    total_images = sum(len(r.images) for r in records)
    with_investigation = sum(1 for r in records if r.investigation_text)
    with_cause = sum(1 for r in records if r.cause_text)

    # Year distribution
    year_counts: dict[str, int] = {}
    for r in records:
        year_counts[r.decade] = year_counts.get(r.decade, 0) + 1

    # Fatal counts
    fatal_values: list[int] = []
    for r in records:
        raw = r.fatalities
        try:
            fatal_values.append(int(raw.replace(",", "").split()[0]))
        except Exception:
            pass

    total_fatalities = sum(fatal_values)
    max_fatal = max(fatal_values, default=0)

    lines = [
        f"  [bold cyan]Total records[/bold cyan]         : [white]{total:,}[/white]",
        f"  [bold cyan]Total images[/bold cyan]          : [white]{total_images:,}[/white]",
        f"  [bold cyan]Scrape errors[/bold cyan]         : [red]{errors}[/red]",
        f"  [bold cyan]With investigation text[/bold cyan]: [white]{with_investigation:,}[/white]",
        f"  [bold cyan]With cause text[/bold cyan]       : [white]{with_cause:,}[/white]",
        f"  [bold cyan]Total fatalities (parsed)[/bold cyan]: [red]{total_fatalities:,}[/red]",
        f"  [bold cyan]Deadliest single event[/bold cyan]: [red]{max_fatal:,}[/red]",
        "",
        "  [bold]Decade distribution:[/bold]",
    ]
    for decade, count in sorted(year_counts.items()):
        bar = "█" * min(count // 5, 40)
        lines.append(f"    [dim]{decade:<20}[/dim] {bar} [cyan]{count}[/cyan]")

    return Panel(
        "\n".join(lines),
        title="[bold green]Dataset Statistics[/bold green]",
        border_style="green",
        padding=(1, 2),
    )


def _record_detail_panel(rec: AccidentRecord) -> None:
    """Pretty-print a single accident record to the console."""
    console.print(Rule(f"[bold cyan]{rec.page_title or rec.wikipedia_url}[/bold cyan]"))

    # Infobox-style table
    info = Table(box=box.SIMPLE, show_header=False, padding=(0, 1), expand=False)
    info.add_column("Field", style="bold cyan", min_width=22)
    info.add_column("Value", style="white")

    def row(label: str, value: str) -> None:
        if value:
            info.add_row(label, value)

    row("URL", rec.wikipedia_url)
    row("Date", rec.date)
    row("Year", rec.year)
    row("Latitude", str(rec.latitude) if rec.latitude is not None else "")
    row("Longitude", str(rec.longitude) if rec.longitude is not None else "")
    row("Coordinates (raw)", rec.coordinates_raw)
    row("Site", rec.site)
    row("Aircraft Type", rec.aircraft_type)
    row("Aircraft Name", rec.aircraft_name)
    row("Operator", rec.operator)
    row("IATA Flight", rec.iata_flight)
    row("ICAO Flight", rec.icao_flight)
    row("Call Sign", rec.call_sign)
    row("Registration", rec.registration)
    row("Origin", rec.flight_origin)
    row("Destination", rec.destination)
    row("Stopover", rec.stopover)
    row("Occupants", rec.occupants)
    row("Passengers", rec.passengers)
    row("Crew", rec.crew)
    row("Fatalities", rec.fatalities)
    row("Injuries", rec.injuries)
    row("Survivors", rec.survivors)
    row("Ground Fatalities", rec.ground_fatalities)
    row("Ground Injuries", rec.ground_injuries)

    console.print(Panel(info, title="[bold]Infobox[/bold]", border_style="blue"))

    for title, text in [
        ("Summary (Index)", rec.index_summary),
        ("Accident Description", rec.accident_description[:2000] if rec.accident_description else ""),
        ("Cause", rec.cause_text[:2000] if rec.cause_text else ""),
        ("Investigation Findings", rec.investigation_text[:2000] if rec.investigation_text else ""),
        ("Aircraft Specifications", rec.aircraft_specs_text[:1000] if rec.aircraft_specs_text else ""),
        ("Technical Details", rec.technical_details_text[:1000] if rec.technical_details_text else ""),
    ]:
        if text:
            console.print(Panel(text, title=f"[bold]{title}[/bold]", border_style="dim"))

    if rec.images:
        img_table = Table(
            title=f"Images ({len(rec.images)})",
            box=box.SIMPLE,
            show_header=True,
        )
        img_table.add_column("#", style="cyan", min_width=3)
        img_table.add_column("Caption", style="white", ratio=4)
        img_table.add_column("Full-res URL", style="dim blue", ratio=5)
        for idx, img in enumerate(rec.images):
            img_table.add_row(str(idx + 1), img.caption or img.alt or "–", img.full_src or img.src)
        console.print(img_table)

    if rec.infobox_extra:
        extra = Table(box=box.SIMPLE, show_header=False)
        extra.add_column("Field", style="dim cyan", min_width=22)
        extra.add_column("Value", style="dim white")
        for k, v in rec.infobox_extra.items():
            extra.add_row(k, v[:200])
        console.print(Panel(extra, title="[dim]Additional Infobox Fields[/dim]", border_style="dim"))

    if rec.scrape_error:
        console.print(f"[bold red]Scrape Error:[/bold red] {rec.scrape_error}")


# ──────────────────────────────────────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────────────────────────────────────

@app.command()
def scrape(
    out: Path = typer.Option(
        Path("./data"),
        "--out", "-o",
        help="Output directory for exported files.",
        show_default=True,
    ),
    start_year: int | None = typer.Option(
        None, "--start-year", "-s",
        help="First year to include (e.g. 1970). Defaults to 1919.",
    ),
    end_year: int | None = typer.Option(
        None, "--end-year", "-e",
        help="Last year to include (e.g. 1979). Defaults to 2026.",
    ),
    concurrency: int = typer.Option(
        5, "--concurrency", "-c",
        help="Max simultaneous HTTP requests.",
        min=1, max=20,
    ),
    delay: float = typer.Option(
        0.4, "--delay", "-d",
        help="Polite delay (seconds) between requests.",
        min=0.1, max=10.0,
    ),
    fmt: str = typer.Option(
        "all", "--format", "-f",
        help="Output format: json | jsonl | csv | all",
    ),
    pretty: bool = typer.Option(
        True, "--pretty/--compact",
        help="Pretty-print JSON output.",
    ),
    no_summary: bool = typer.Option(
        False, "--no-summary",
        help="Skip printing per-record summary table.",
    ),
) -> None:
    """
    [bold green]Scrape[/bold green] Wikipedia for commercial aviation accidents (1919–2026).

    Fetches the main index, then visits each individual accident article to extract
    infobox data, all section text (investigation, causes, findings, specs), and images.
    """
    year_range = None
    if start_year or end_year:
        year_range = (start_year or 1919, end_year or 2026)

    console.print(
        Panel(
            f"[bold cyan]Aircraft Tracker[/bold cyan] – Wikipedia Scraper\n"
            f"  Range    : [yellow]{year_range[0] if year_range else 1919}[/yellow] → [yellow]{year_range[1] if year_range else 2026}[/yellow]\n"
            f"  Workers  : [green]{concurrency}[/green]\n"
            f"  Delay    : [green]{delay}s[/green]\n"
            f"  Output   : [cyan]{out.resolve()}[/cyan]\n"
            f"  Format   : [magenta]{fmt}[/magenta]",
            title="Configuration",
            border_style="cyan",
        )
    )

    # Step 1: fetch index
    with console.status("[bold blue]Fetching accident index from Wikipedia…", spinner="dots"):
        entries = asyncio.run(fetch_index_entries())

    if year_range:
        entries = [
            e for e in entries
            if e["year"].isdigit() and year_range[0] <= int(e["year"]) <= year_range[1]
        ]

    console.print(f"[green]✓[/green] Index loaded — [bold]{len(entries):,}[/bold] entries to scrape.")

    # Step 2: scrape articles with live progress
    records: list[AccidentRecord] = []
    with _progress_bar() as progress:
        task = progress.add_task("[cyan]Scraping articles…", total=len(entries))
        last_title: list[str] = [""]

        def on_progress(completed: int, total: int, rec: AccidentRecord) -> None:
            last_title[0] = rec.page_title or rec.wikipedia_url.split("/")[-1]
            progress.update(
                task,
                completed=completed,
                description=f"[cyan]Scraping… [dim]{last_title[0][:55]}[/dim]",
            )

        records = asyncio.run(
            scrape_all(
                entries,
                concurrency=concurrency,
                delay=delay,
                progress_callback=on_progress,
            )
        )

    console.print(f"[green]✓[/green] Scraped [bold]{len(records):,}[/bold] records.")

    # Step 3: export
    out.mkdir(parents=True, exist_ok=True)
    exported_files: list[Path] = []

    with console.status("[bold blue]Exporting data…", spinner="dots"):
        if fmt in ("json", "all"):
            p = export_json(records, out / "accidents.json", pretty=pretty)
            exported_files.append(p)
        if fmt in ("jsonl", "all"):
            p = export_jsonl(records, out / "accidents.jsonl")
            exported_files.append(p)
        if fmt in ("csv", "all"):
            acc_p, img_p = export_csv(records, out)
            exported_files.extend([acc_p, img_p])

    console.print("\n[bold green]Exported files:[/bold green]")
    for fp in exported_files:
        size_kb = fp.stat().st_size / 1024
        console.print(f"  [cyan]{fp}[/cyan]  ([green]{size_kb:,.0f} KB[/green])")

    # Step 4: show statistics
    console.print()
    console.print(_make_stats_panel(records))

    # Step 5: optional summary table
    if not no_summary:
        console.print()
        console.print(_make_summary_table(records))


@app.command("list-index")
def list_index(
    year: int | None = typer.Option(
        None, "--year", "-y",
        help="Filter to a specific year.",
    ),
    decade: str | None = typer.Option(
        None, "--decade", "-d",
        help="Filter by decade label (e.g. '1970s').",
    ),
    limit: int = typer.Option(
        0, "--limit", "-l",
        help="Max rows to display (0 = all).",
    ),
) -> None:
    """
    [bold green]List[/bold green] accident entries from the Wikipedia index without fetching articles.
    """
    with console.status("[bold blue]Fetching index…", spinner="dots"):
        entries = asyncio.run(fetch_index_entries())

    if year:
        entries = [e for e in entries if e["year"] == str(year)]
    if decade:
        entries = [e for e in entries if decade.lower() in e["decade"].lower()]
    if limit:
        entries = entries[:limit]

    table = Table(
        title=f"[bold]Aviation Accident Index ({len(entries)} entries)[/bold]",
        box=box.ROUNDED,
        highlight=True,
        expand=True,
    )
    table.add_column("Year", style="cyan", no_wrap=True, min_width=6)
    table.add_column("Decade", style="dim", min_width=14)
    table.add_column("Summary", style="white", ratio=5)
    table.add_column("URL", style="dim blue", ratio=3)

    for e in entries:
        table.add_row(
            e["year"],
            e["decade"],
            e["index_summary"][:120],
            e["wikipedia_url"],
        )

    console.print(table)
    console.print(f"[dim]Total: {len(entries):,} entries[/dim]")


@app.command()
def show(
    url: str = typer.Argument(
        ...,
        help="Full Wikipedia URL of the accident article.",
    ),
) -> None:
    """
    [bold green]Show[/bold green] detailed information for a single accident article.
    """
    import aiohttp
    from .scraper import _fetch, parse_article

    meta = {
        "decade": "",
        "year": "",
        "index_summary": "",
        "wikipedia_url": url,
    }

    async def _go() -> AccidentRecord:
        async with aiohttp.ClientSession() as session:
            html = await _fetch(session, url)
        return parse_article(html, url, meta)

    with console.status(f"[bold blue]Fetching {url}…", spinner="dots"):
        rec = asyncio.run(_go())

    _record_detail_panel(rec)


@app.command()
def export(
    input: Path = typer.Option(
        ..., "--input", "-i",
        help="Path to existing accidents.json dump.",
    ),
    out: Path = typer.Option(
        Path("./data"),
        "--out", "-o",
        help="Output directory for re-exported files.",
    ),
    fmt: str = typer.Option(
        "all", "--format", "-f",
        help="Output format: json | jsonl | csv | all",
    ),
    pretty: bool = typer.Option(True, "--pretty/--compact"),
) -> None:
    """
    [bold green]Export[/bold green] an existing accidents.json dump to CSV / JSONL.
    """
    import orjson
    from .models import AccidentRecord, AccidentImage

    console.print(f"[cyan]Loading[/cyan] {input}…")
    raw = orjson.loads(input.read_bytes())

    _fields = {f.name for f in dataclasses.fields(AccidentRecord)}
    records: list[AccidentRecord] = []
    for d in raw:
        imgs = [AccidentImage(**img) for img in d.pop("images", [])]
        sections = d.pop("sections", {})
        infobox_extra = d.pop("infobox_extra", {})
        r = AccidentRecord(**{k: v for k, v in d.items() if k in _fields})
        r.images = imgs
        r.sections = sections
        r.infobox_extra = infobox_extra
        records.append(r)

    console.print(f"[green]✓[/green] Loaded [bold]{len(records):,}[/bold] records.")

    out.mkdir(parents=True, exist_ok=True)
    with console.status("[bold blue]Exporting…", spinner="dots"):
        exported_files: list[Path] = []
        if fmt in ("json", "all"):
            p = export_json(records, out / "accidents.json", pretty=pretty)
            exported_files.append(p)
        if fmt in ("jsonl", "all"):
            p = export_jsonl(records, out / "accidents.jsonl")
            exported_files.append(p)
        if fmt in ("csv", "all"):
            acc_p, img_p = export_csv(records, out)
            exported_files.extend([acc_p, img_p])

    for fp in exported_files:
        size_kb = fp.stat().st_size / 1024
        console.print(f"  [cyan]{fp}[/cyan]  ([green]{size_kb:,.0f} KB[/green])")


@app.command()
def stats(
    input: Path = typer.Option(
        ..., "--input", "-i",
        help="Path to accidents.json dump.",
    ),
) -> None:
    """
    [bold green]Stats[/bold green] – show dataset statistics from an existing JSON dump.
    """
    import orjson
    from .models import AccidentRecord, AccidentImage

    raw = orjson.loads(input.read_bytes())
    _fields = {f.name for f in dataclasses.fields(AccidentRecord)}
    records: list[AccidentRecord] = []
    for d in raw:
        imgs = [AccidentImage(**img) for img in d.pop("images", [])]
        sections = d.pop("sections", {})
        infobox_extra = d.pop("infobox_extra", {})
        r = AccidentRecord(**{k: v for k, v in d.items() if k in _fields})
        r.images = imgs
        r.sections = sections
        r.infobox_extra = infobox_extra
        records.append(r)

    console.print(_make_stats_panel(records))
    console.print(_make_summary_table(records))


@app.command()
def audit(
    input: Path = typer.Option(
        ..., "--input", "-i",
        help="Path to accidents.json or accidents.jsonl dump.",
    ),
    out: Path | None = typer.Option(
        None, "--out", "-o",
        help="Directory to write audit_coverage.csv and audit_issues.csv.",
    ),
    threshold: int = typer.Option(
        3, "--threshold", "-t",
        help="Min missing core fields to flag a record as 'missing core data'.",
        min=1, max=5,
    ),
) -> None:
    """
    [bold green]Audit[/bold green] data-quality of an existing dump.

    Reports per-field coverage, flags records with no aviation signals
    (irrelevant pages), and shows records missing multiple core fields.
    Optionally exports two CSVs to [cyan]--out[/cyan]:

      [bold]audit_coverage.csv[/bold]  — one row per record, presence flags per field\n
      [bold]audit_issues.csv[/bold]    — records flagged as irrelevant or missing core data
    """
    import orjson

    suffix = input.suffix.lower()
    if suffix == ".jsonl":
        raw: list[dict] = [orjson.loads(line) for line in input.open("rb")]
    else:
        raw = orjson.loads(input.read_bytes())

    total = len(raw)
    console.print(
        Rule(
            f"[bold cyan]Data Quality Audit[/bold cyan] — "
            f"{input.name} ([white]{total:,}[/white] records)"
        )
    )

    # ── 1. Field coverage table ──────────────────────────────────────────────
    def _bar(pct: float, width: int = 24) -> str:
        filled = round(pct / 100 * width)
        full_block = "\u2588"
        light_shade = "\u2591"
        return f"[green]{full_block * filled}[/green][dim]{light_shade * (width - filled)}[/dim]"

    cov = Table(
        title="[bold]Field Coverage[/bold]",
        box=box.ROUNDED,
        expand=False,
        show_lines=False,
    )
    cov.add_column("Field", style="cyan", min_width=22)
    cov.add_column("Present", style="green", justify="right", min_width=8)
    cov.add_column("Missing", style="red", justify="right", min_width=8)
    cov.add_column("% Filled", style="yellow", justify="right", min_width=9)
    cov.add_column("Coverage", min_width=26)

    for field in _AUDIT_CORE_FIELDS:
        miss = sum(1 for r in raw if _missing(r.get(field)))
        present = total - miss
        pct = present / total * 100
        cov.add_row(field, str(present), str(miss), f"{pct:.1f}%", _bar(pct))
    # images is a list field — treat separately
    no_img = sum(1 for r in raw if not r.get("images"))
    pct_img = (total - no_img) / total * 100
    cov.add_row("images", str(total - no_img), str(no_img), f"{pct_img:.1f}%", _bar(pct_img))
    console.print(cov)

    # ── 2. Irrelevant records ────────────────────────────────────────────────
    irrelevant = [r for r in raw if _is_irrelevant_record(r)]
    irrelevant_urls = {r["wikipedia_url"] for r in irrelevant}
    console.print()
    console.print(
        f"[bold yellow]Potentially irrelevant[/bold yellow] "
        f"(no aviation signals): [red]{len(irrelevant)}[/red]"
    )
    if irrelevant:
        irr = Table(box=box.SIMPLE, show_header=True, expand=True)
        irr.add_column("Year", style="cyan", min_width=6)
        irr.add_column("Title", style="white", ratio=3)
        irr.add_column("URL", style="dim blue", ratio=4)
        for r in irrelevant[:25]:
            irr.add_row(
                r.get("year", ""),
                (r.get("page_title") or "")[:70],
                r.get("wikipedia_url", ""),
            )
        if len(irrelevant) > 25:
            irr.add_row("…", f"[dim]{len(irrelevant) - 25} more not shown[/dim]", "")
        console.print(
            Panel(irr, title="[bold red]Irrelevant Records[/bold red]", border_style="red")
        )

    # ── 3. Missing core data ─────────────────────────────────────────────────
    _core_check = ["date", "aircraft_type", "operator", "fatalities", "latitude"]
    missing_core = [
        r for r in raw
        if not _is_irrelevant_record(r)
        and sum(1 for f in _core_check if _missing(r.get(f))) >= threshold
    ]
    console.print()
    console.print(
        f"[bold yellow]Missing ≥{threshold} core fields[/bold yellow] "
        f"[dim](date, aircraft_type, operator, fatalities, lat)[/dim]: "
        f"[red]{len(missing_core)}[/red]"
    )
    if missing_core:
        mc = Table(box=box.SIMPLE, show_header=True, expand=True)
        mc.add_column("Year", style="cyan", min_width=6)
        mc.add_column("Title", style="white", ratio=3)
        mc.add_column("Missing fields", style="red", ratio=2)
        mc.add_column("URL", style="dim blue", ratio=3)
        for r in missing_core[:20]:
            absent = ", ".join(f for f in _core_check if _missing(r.get(f)))
            mc.add_row(
                r.get("year", ""),
                (r.get("page_title") or "")[:60],
                absent,
                r.get("wikipedia_url", ""),
            )
        if len(missing_core) > 20:
            mc.add_row("…", f"[dim]{len(missing_core) - 20} more[/dim]", "", "")
        console.print(
            Panel(mc, title="[bold yellow]Missing Core Data[/bold yellow]", border_style="yellow")
        )

    # ── 4. Summary panel ─────────────────────────────────────────────────────
    clean = total - len(irrelevant) - len({r["wikipedia_url"] for r in missing_core} - irrelevant_urls)
    console.print()
    console.print(
        Panel(
            f"  [cyan]Total records[/cyan]        : [white]{total:,}[/white]\n"
            f"  [red]Irrelevant[/red]           : [white]{len(irrelevant)}[/white] "
            f"[dim]({len(irrelevant)/total*100:.1f}%)[/dim]\n"
            f"  [yellow]Missing core data[/yellow]    : [white]{len(missing_core)}[/white] "
            f"[dim]({len(missing_core)/total*100:.1f}%)[/dim]\n"
            f"  [green]No issues detected[/green]   : [white]{max(clean, 0):,}[/white]",
            title="[bold green]Audit Summary[/bold green]",
            border_style="green",
            padding=(1, 2),
        )
    )

    if not out:
        return

    # ── 5. Export ─────────────────────────────────────────────────────────────
    out.mkdir(parents=True, exist_ok=True)

    # audit_coverage.csv — one row per record, value + presence flag per core field
    ok_cols = [f"{f}_ok" for f in _AUDIT_CORE_FIELDS]
    cov_fieldnames = (
        ["wikipedia_url", "page_title", "year", "image_count", "is_irrelevant"]
        + _AUDIT_CORE_FIELDS
        + ok_cols
    )
    cov_path = out / "audit_coverage.csv"
    with cov_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cov_fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in raw:
            row: dict = {
                "wikipedia_url": r.get("wikipedia_url", ""),
                "page_title": r.get("page_title") or "",
                "year": r.get("year", ""),
                "image_count": len(r.get("images", [])),
                "is_irrelevant": int(_is_irrelevant_record(r)),
            }
            for field in _AUDIT_CORE_FIELDS:
                v = r.get(field)
                row[field] = "" if v is None else v
                row[f"{field}_ok"] = 0 if _missing(v) else 1
            w.writerow(row)

    # audit_issues.csv — records flagged as irrelevant or missing core data
    issues = {r["wikipedia_url"]: r for r in (irrelevant + missing_core)}
    issue_fieldnames = [
        "wikipedia_url", "page_title", "year", "issue_type",
        "date", "aircraft_type", "operator", "fatalities",
        "latitude", "longitude", "registration",
    ]
    issues_path = out / "audit_issues.csv"
    with issues_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=issue_fieldnames, extrasaction="ignore")
        w.writeheader()
        for r in issues.values():
            itype = "irrelevant" if r["wikipedia_url"] in irrelevant_urls else "missing_core"
            row = {
                "wikipedia_url": r.get("wikipedia_url", ""),
                "page_title": r.get("page_title") or "",
                "year": r.get("year", ""),
                "issue_type": itype,
            }
            for field in ["date", "aircraft_type", "operator", "fatalities",
                          "latitude", "longitude", "registration"]:
                v = r.get(field)
                row[field] = "" if v is None else v
            w.writerow(row)

    console.print()
    console.print(f"[bold green]Exported audit files to[/bold green] [cyan]{out}[/cyan]:")
    for p in (cov_path, issues_path):
        size_kb = p.stat().st_size / 1024
        console.print(f"  [cyan]{p}[/cyan]  ([green]{size_kb:,.0f} KB[/green])")
