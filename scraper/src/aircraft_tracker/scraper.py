"""
scraper.py – Core async scraping logic.

Strategy:
  1. Fetch the main index page → collect all ~2 000 accident entries (url + metadata).
  2. For each accident URL → fetch the Wikipedia article and parse:
       • Infobox fields
       • All section text (headings + paragraphs + list items)
       • High-value sections: Investigation, Causes, Aircraft, Technical details
       • Images (thumbnail + full-res + caption)
  3. Return a list[AccidentRecord].

Rate-limiting: max 5 concurrent requests; polite per-request delay;
   up to 6 retries with exponential back-off + full jitter (tenacity).
   429 Retry-After headers are honoured before tenacity applies its own wait.
"""
import asyncio
import logging
import re
from urllib.parse import urljoin

import aiohttp
from bs4 import BeautifulSoup, Tag
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
    wait_random,
)

from .models import AccidentImage, AccidentRecord, AircraftEntry

_LOG = logging.getLogger(__name__)

# One shared timeout applied to every request
_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)

WIKIPEDIA_BASE = "https://en.wikipedia.org"
INDEX_URL = (
    "https://en.wikipedia.org/wiki/"
    "List_of_accidents_and_incidents_involving_commercial_aircraft"
)

# Sections whose full text is extracted into dedicated fields
INVESTIGATION_KEYWORDS = {"investigation", "findings", "probable cause", "ntsb", "investigation findings"}
CAUSE_KEYWORDS = {"cause", "causes", "contributing factors", "causal factors"}
AIRCRAFT_KEYWORDS = {"aircraft", "aircraft description", "specifications", "aircraft specifications"}
TECHNICAL_KEYWORDS = {"technical", "technical details", "mechanical", "systems"}
ACCIDENT_KEYWORDS = {
    "accident", "crash", "collision", "incident", "disaster",
    "timeline", "sequence of events", "the accident", "the crash",
}

# Wikipedia infobox field normalisation map
# Keys are lowercased label text; values are AccidentRecord field names.
_INFOBOX_MAP: dict[str, str] = {
    "date": "date",
    "summary": "summary_infobox",
    "site": "site",
    "location": "site",           # alternate label used by some articles
    "type": "aircraft_type",
    "aircraft type": "aircraft_type",   # most-common variant (1 780 articles)
    "name": "aircraft_name",
    "aircraft name": "aircraft_name",   # second-most-common variant
    "operator": "operator",
    "iata flight no.": "iata_flight",
    "icao flight no.": "icao_flight",
    "call sign": "call_sign",
    "registration": "registration",
    "flight origin": "flight_origin",
    "destination": "destination",
    "stopover": "stopover",
    "occupants": "occupants",
    "passengers": "passengers",
    "crew": "crew",
    "fatalities": "fatalities",
    "deaths": "fatalities",             # alternate label
    "total fatalities": "fatalities",   # overwrites partial value with the total
    "injuries": "injuries",
    "injured": "injuries",              # alternate label
    "total injuries": "injuries",       # overwrites partial value with the total
    "survivors": "survivors",
    "total survivors": "survivors",     # overwrites partial value with the total
    "ground fatalities": "ground_fatalities",
    "ground deaths": "ground_fatalities",
    "ground injuries": "ground_injuries",
    "ground injured": "ground_injuries",
}

# URL slug prefixes that indicate reference/meta pages, not accident articles.
# Matched against the /wiki/<slug> portion of each index URL.
_IGNORE_SLUG_RE = re.compile(
    r"/wiki/(?:Lists?_of_|Template_talk:)",
    re.IGNORECASE,
)

HEADERS = {
    "User-Agent": (
        "aircraft-tracker/0.1 "
        "(educational research; https://github.com/user/aircraft-tracker)"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


def _is_retryable(exc: BaseException) -> bool:
    """Return True for transient HTTP/network errors that are safe to retry."""
    if isinstance(exc, aiohttp.ClientResponseError):
        return exc.status in {429, 500, 502, 503, 504}
    return isinstance(exc, (aiohttp.ClientConnectionError, asyncio.TimeoutError))


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _clean(text: str) -> str:
    """Strip edit-section markers and normalise whitespace."""
    text = re.sub(r"\[edit\]", "", text)
    text = re.sub(r"\[\d+\]", "", text)          # citation numbers like [1]
    text = re.sub(r"\s+", " ", text)
    return text.strip()


# Matches a DMS/decimal coordinate appended to a location string, e.g.
# "Mangalore Airport12°56′48″N 074°52′25″E\ufeff / \ufeff12.946...".
# Wikipedia's {{coord}} template always emits \ufeff (ZWNBSP) as a separator.
_COORD_SUFFIX_RE = re.compile(
    r"\s*-?\d+[°º][\d\s°′″NSEWnsew,;./\-\ufeff\u200b\xa0]*$",
    re.UNICODE | re.DOTALL,
)


def _strip_coord_suffix(text: str) -> str:
    """Remove {{coord}} template text that gets appended to location strings."""
    # \ufeff (ZWNBSP) is always present in Wikipedia's coord rendering
    text = text.partition("\ufeff")[0]
    # Also catch DMS coordinates not preceded by \ufeff
    text = _COORD_SUFFIX_RE.sub("", text)
    return text.strip().rstrip("/ ;,").strip()


def _section_key(heading: str) -> str:
    return heading.lower().strip()


def _is_internal_link(href) -> bool:
    """True if href points to an actual Wikipedia article (not meta-pages)."""
    if not href:
        return False
    bad_prefixes = (
        "Special:", "Wikipedia:", "Help:", "Template:", "Template_talk:",
        "Talk:", "User:", "Category:", "Portal:", "File:",
    )
    return href.startswith("/wiki/") and not any(
        href.startswith(f"/wiki/{p}") for p in bad_prefixes
    )


def _parse_coordinates(soup: BeautifulSoup) -> tuple[float | None, float | None, str]:
    """
    Extract geographic coordinates from a Wikipedia page.
    Returns (latitude, longitude, raw_string).

    Tries (in order):
      1. <span class="geo">lat; lon</span>  – geo microformat
      2. <span class="latitude"> + <span class="longitude">  – split microformat
      3. GeoHack link  href="//tools.wmflabs.org/geohack/...params=..."  – URL params
    """
    # 1. Combined geo span  e.g.  "28.569; -80.703"
    geo = soup.find("span", class_="geo")
    if geo:
        raw = geo.get_text().strip()
        parts = [p.strip() for p in raw.split(";")]
        if len(parts) == 2:
            try:
                return float(parts[0]), float(parts[1]), raw
            except ValueError:
                pass

    # 2. Separate latitude / longitude spans
    lat_el = soup.find("span", class_="latitude")
    lon_el = soup.find("span", class_="longitude")
    if lat_el and lon_el:
        raw = f"{lat_el.get_text().strip()}; {lon_el.get_text().strip()}"
        lat_dec = _dms_to_decimal(lat_el.get_text().strip())
        lon_dec = _dms_to_decimal(lon_el.get_text().strip())
        if lat_dec is not None and lon_dec is not None:
            return lat_dec, lon_dec, raw

    # 3. GeoHack URL params  e.g. params=28_34_7_N_77_6_22_E
    geohack = soup.find("a", href=re.compile(r"geohack", re.I))
    if geohack:
        href = geohack.get("href", "")
        m = re.search(r"params=([^&\s]+)", href)
        if m:
            raw = m.group(1)
            lat_dec, lon_dec = _geohack_params_to_decimal(raw)
            if lat_dec is not None and lon_dec is not None:
                return lat_dec, lon_dec, raw

    return None, None, ""


def _dms_to_decimal(dms: str) -> float | None:
    """
    Convert a DMS string like "28°34'7\"N" or "28.569" to a decimal float.
    Returns None if parsing fails.
    """
    dms = dms.strip()
    # Already decimal
    try:
        return float(dms)
    except ValueError:
        pass
    # DMS pattern with optional direction letter
    m = re.match(
        r"""(\d+)[°d\s](\d+)?['\'\s]?(\d+(?:\.\d+)?)?[\"\'\s]?([NSEW])?""",
        dms,
        re.IGNORECASE,
    )
    if not m:
        return None
    degrees = float(m.group(1))
    minutes = float(m.group(2) or 0)
    seconds = float(m.group(3) or 0)
    direction = (m.group(4) or "").upper()
    decimal = degrees + minutes / 60 + seconds / 3600
    if direction in ("S", "W"):
        decimal = -decimal
    return round(decimal, 6)


def _dms_parts_to_decimal(parts: list[str], direction: str) -> float:
    """Convert a list of DMS component strings + a compass direction to decimal degrees."""
    vals = [float(p) for p in parts]
    dec = vals[0]
    if len(vals) >= 2:
        dec += vals[1] / 60
    if len(vals) >= 3:
        dec += vals[2] / 3600
    if direction in ("S", "W"):
        dec = -dec
    return round(dec, 6)


def _geohack_params_to_decimal(params: str) -> tuple[float | None, float | None]:
    """
    Parse GeoHack params string like "28_34_7_N_77_6_22_E" → (lat, lon).
    """
    # Split on underscore; expect groups of 3 or 4 tokens per coordinate
    tokens = params.split("_")
    try:
        # Find direction letters to split lat/lon
        n_idx = next((i for i, t in enumerate(tokens) if t.upper() in ("N", "S")), None)
        e_idx = next((i for i, t in enumerate(tokens) if t.upper() in ("E", "W")), None)
        if n_idx is None or e_idx is None:
            return None, None

        lat_tokens = tokens[:n_idx]
        lat_dir = tokens[n_idx].upper()
        lon_tokens = tokens[n_idx + 1 : e_idx]
        lon_dir = tokens[e_idx].upper()

        return _dms_parts_to_decimal(lat_tokens, lat_dir), _dms_parts_to_decimal(lon_tokens, lon_dir)
    except Exception:
        return None, None


def _full_img_url(src: str) -> str:
    """Convert a thumbnail URL to the full-resolution Wikimedia URL."""
    # e.g. .../thumb/a/ab/file.jpg/250px-file.jpg  →  .../a/ab/file.jpg
    if "/thumb/" in src:
        parts = src.split("/thumb/")
        base = parts[0]
        rest = parts[1]
        # rest = "a/ab/file.jpg/250px-file.jpg"
        segments = rest.split("/")
        # drop the last segment (the size-prefixed filename)
        file_path = "/".join(segments[:-1])
        return f"{base}/{file_path}"
    return src


# ──────────────────────────────────────────────────────────────────────────────
# Index-page parsing
# ──────────────────────────────────────────────────────────────────────────────

def parse_index_page(html: str) -> list[dict]:
    """
    Parse the main list page.
    Returns a list of dicts: {decade, year, index_summary, wikipedia_url}
    """
    soup = BeautifulSoup(html, "lxml")
    content = soup.find(id="mw-content-text")
    if not content:
        return []

    # These standard Wikipedia footer headings appear after the accident list.
    # Once we see one we stop — every <li> below it is non-accident content
    # (e.g. External links, See also, References, Further reading).
    _STOP_HEADINGS = frozenset(
        {
            "external links",
            "see also",
            "references",
            "notes",
            "further reading",
            "footnotes",
        }
    )

    entries: list[dict] = []
    current_decade = ""
    current_year = ""
    stopped = False

    for el in content.find_all(["h2", "h3", "li"]):
        if stopped:
            break
        tag = el.name
        if tag == "h2":
            heading = _clean(el.get_text()).lower()
            if heading in _STOP_HEADINGS:
                stopped = True
                break
            current_decade = _clean(el.get_text())
            current_year = ""   # reset year on each new decade heading
        elif tag == "h3":
            current_year = _clean(el.get_text())
        elif tag == "li":
            link = el.find("a", href=_is_internal_link)
            if not link:
                # Fallback: find first /wiki/ link that isn't a meta page
                link = el.find(
                    "a",
                    href=lambda h: h and _is_internal_link(h),
                )
            if link:
                href = link.get("href", "")
                if href.startswith("/wiki/") and not _IGNORE_SLUG_RE.search(href):
                    entries.append(
                        {
                            "decade": current_decade,
                            "year": current_year,
                            "index_summary": _clean(el.get_text()),
                            "wikipedia_url": urljoin(WIKIPEDIA_BASE, href),
                        }
                    )

    # De-duplicate by URL while preserving order
    seen: set[str] = set()
    unique: list[dict] = []
    for e in entries:
        if e["wikipedia_url"] not in seen:
            seen.add(e["wikipedia_url"])
            unique.append(e)

    return unique


# ──────────────────────────────────────────────────────────────────────────────
# Individual article parsing
# ──────────────────────────────────────────────────────────────────────────────

# Matches section-divider rows in multi-aircraft infoboxes, e.g.
# "First aircraft", "Second aircraft", "Third aircraft".
_AIRCRAFT_SECTION_RE = re.compile(
    r"^(first|second|third|fourth|fifth)\s+aircraft$", re.IGNORECASE
)

# Fields that map directly onto AircraftEntry attribute names.
_AIRCRAFT_ENTRY_FIELDS = frozenset({
    "aircraft_type", "aircraft_name", "operator", "iata_flight", "icao_flight",
    "call_sign", "registration", "flight_origin", "destination", "stopover",
    "occupants", "passengers", "crew", "fatalities", "injuries", "survivors",
})


def _make_aircraft_entry(fields: dict[str, str]) -> AircraftEntry:
    """Convert a raw key→value dict (mapped field names) into an AircraftEntry."""
    known = {k: v for k, v in fields.items() if k in _AIRCRAFT_ENTRY_FIELDS}
    extra = {k: v for k, v in fields.items() if k not in _AIRCRAFT_ENTRY_FIELDS}
    return AircraftEntry(**known, extra=extra)


def _parse_infobox(
    soup: BeautifulSoup,
) -> tuple[dict[str, str], dict[str, str], list[AircraftEntry]]:
    """
    Return (known_fields, extra_fields, aircraft_list) extracted from the infobox.

    For multi-aircraft incidents (e.g. mid-air collisions) Wikipedia infoboxes
    contain "First aircraft" / "Second aircraft" section dividers.  Each
    aircraft's fields are collected into a separate dict inside aircraft_list.

    Top-level aggregate rows ("Total fatalities", "Total survivors", etc.) take
    precedence over per-aircraft values.  When those aggregate rows are absent
    the totals are computed by summing the matching per-aircraft figures.
    """
    known: dict[str, str] = {}
    extra: dict[str, str] = {}
    source_kind: dict[str, str] = {}
    aircraft_list: list[AircraftEntry] = []

    infobox = soup.find("table", class_=re.compile(r"infobox"))
    if not infobox:
        return known, extra, aircraft_list

    # Accumulates raw key→value pairs for the aircraft currently being parsed.
    current_aircraft_fields: dict[str, str] | None = None

    for row in infobox.find_all("tr"):
        th = row.find("th")
        td = row.find("td")

        # Section-divider row: <th colspan="2">First aircraft</th> with no <td>.
        if th and not td:
            section_text = _clean(th.get_text()).lower()
            if _AIRCRAFT_SECTION_RE.match(section_text):
                if current_aircraft_fields is not None:
                    aircraft_list.append(_make_aircraft_entry(current_aircraft_fields))
                current_aircraft_fields = {}
            continue

        if th and td:
            key = _clean(th.get_text()).lower()
            value = _strip_coord_suffix(_clean(td.get_text()))

            if current_aircraft_fields is not None:
                # Inside a per-aircraft section – store under the mapped field
                # name when available, otherwise keep the raw label text.
                mapped = _INFOBOX_MAP.get(key)
                current_aircraft_fields[mapped if mapped else key] = value
            else:
                # Top-level infobox field.
                mapped = _INFOBOX_MAP.get(key)
                if mapped:
                    is_total_row = key.startswith("total ")
                    prev_source = source_kind.get(mapped)
                    # "Total …" rows must not be overwritten by later rows.
                    if is_total_row or prev_source != "total":
                        known[mapped] = value
                        source_kind[mapped] = "total" if is_total_row else "regular"
                else:
                    extra[_clean(th.get_text())] = value

    # Append the last aircraft block (no trailing divider row to trigger it).
    if current_aircraft_fields is not None:
        aircraft_list.append(_make_aircraft_entry(current_aircraft_fields))

    # For multi-aircraft accidents: when no explicit "Total …" aggregate row was
    # found for a field, compute the total by summing per-aircraft values.
    if len(aircraft_list) > 1:
        for agg_field in ("fatalities", "survivors", "occupants", "passengers", "crew", "injuries"):
            if source_kind.get(agg_field) != "total":
                try:
                    total = sum(
                        int(getattr(ac, agg_field).replace(",", "").split()[0])
                        for ac in aircraft_list
                        if getattr(ac, agg_field)
                    )
                    if total > 0:
                        known[agg_field] = str(total)
                        source_kind[agg_field] = "computed"
                except (ValueError, AttributeError):
                    pass

    return known, extra, aircraft_list


def _parse_images(soup: BeautifulSoup) -> list[AccidentImage]:
    """Extract all images with captions from thumbs and figures."""
    images: list[AccidentImage] = []
    seen_src: set[str] = set()

    # Thumbnail divs  (MediaWiki classic layout)
    for thumb in soup.find_all("div", class_="thumbinner"):
        img = thumb.find("img")
        if not img:
            continue
        src = img.get("src", "")
        if not src or src in seen_src:
            continue
        seen_src.add(src)
        caption_el = thumb.find("div", class_="thumbcaption")
        caption = _clean(caption_el.get_text()) if caption_el else ""
        alt = img.get("alt", "")
        images.append(
            AccidentImage(
                src=f"https:{src}" if src.startswith("//") else src,
                alt=alt,
                caption=caption,
                full_src=_full_img_url(
                    f"https:{src}" if src.startswith("//") else src
                ),
            )
        )

    # <figure> elements (Parsoid / new Vector layout)
    for fig in soup.find_all("figure"):
        img = fig.find("img")
        if not img:
            continue
        src = img.get("src", "")
        if not src or src in seen_src:
            continue
        seen_src.add(src)
        caption_el = fig.find("figcaption")
        caption = _clean(caption_el.get_text()) if caption_el else ""
        alt = img.get("alt", "")
        images.append(
            AccidentImage(
                src=f"https:{src}" if src.startswith("//") else src,
                alt=alt,
                caption=caption,
                full_src=_full_img_url(
                    f"https:{src}" if src.startswith("//") else src
                ),
            )
        )

    return images


def _paragraphs_after(heading_tag: Tag) -> str:
    """
    Collect text from sibling paragraph/list elements immediately following
    a heading tag (until the next heading of same or higher level).

    Modern Wikipedia (2023+) wraps <hN> tags inside
    ``<div class="mw-heading mw-headingN">``.  In that layout the next
    paragraphs are siblings of the *wrapper div*, not siblings of the <hN>
    element itself, so we must pivot to the wrapper when present.
    """
    texts: list[str] = []
    current_level = int(heading_tag.name[1]) if heading_tag.name[1].isdigit() else 2

    # Detect modern wrapper: <div class="mw-heading …"><h2>…</h2> … </div>
    parent = heading_tag.parent
    pivot: Tag = (
        parent
        if isinstance(parent, Tag)
        and parent.name == "div"
        and "mw-heading" in (parent.get("class") or [])
        else heading_tag
    )

    for sibling in pivot.find_next_siblings():
        if not isinstance(sibling, Tag):
            continue
        name = sibling.name
        # Direct heading stop
        if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
            if int(name[1]) <= current_level:
                break
        # Wrapped heading stop (mw-heading div)
        elif name == "div" and "mw-heading" in (sibling.get("class") or []):
            inner = sibling.find(re.compile(r"^h[1-6]$"))
            if inner and int(inner.name[1]) <= current_level:
                break
        if name in ("p", "ul", "ol", "dl", "blockquote"):
            texts.append(_clean(sibling.get_text()))

    return "\n\n".join(filter(None, texts))


def parse_article(html: str, url: str, meta: dict) -> AccidentRecord:
    """Parse a full Wikipedia accident article into an AccidentRecord."""
    soup = BeautifulSoup(html, "lxml")

    # Page title
    title_el = soup.find("h1", id="firstHeading") or soup.find("h1")
    page_title = _clean(title_el.get_text()) if title_el else meta.get("wikipedia_url", "")

    # Infobox
    known_fields, extra_fields, aircraft_list = _parse_infobox(soup)

    # Images
    images = _parse_images(soup)

    # Coordinates
    latitude, longitude, coordinates_raw = _parse_coordinates(soup)

    # Sections – collect all h2/h3/h4 headings + their body text
    content = soup.find(id="mw-content-text")
    sections: dict[str, str] = {}
    investigation_text = cause_text = aircraft_specs_text = ""
    technical_details_text = accident_description = ""

    if content:
        for heading in content.find_all(["h2", "h3", "h4"]):
            heading_text = _clean(heading.get_text())
            body = _paragraphs_after(heading)
            sections[heading_text] = body

            key = _section_key(heading_text)
            if any(kw in key for kw in INVESTIGATION_KEYWORDS):
                investigation_text += "\n\n" + body
            if any(kw in key for kw in CAUSE_KEYWORDS):
                cause_text += "\n\n" + body
            if any(kw in key for kw in AIRCRAFT_KEYWORDS):
                aircraft_specs_text += "\n\n" + body
            if any(kw in key for kw in TECHNICAL_KEYWORDS):
                technical_details_text += "\n\n" + body
            if any(kw in key for kw in ACCIDENT_KEYWORDS):
                accident_description += "\n\n" + body

    rec = AccidentRecord(
        decade=meta["decade"],
        year=meta["year"],
        index_summary=meta["index_summary"],
        wikipedia_url=url,
        page_title=page_title,
        images=images,
        sections=sections,
        investigation_text=investigation_text.strip() or None,
        cause_text=cause_text.strip() or None,
        aircraft_specs_text=aircraft_specs_text.strip() or None,
        technical_details_text=technical_details_text.strip() or None,
        accident_description=accident_description.strip() or None,
        latitude=latitude,
        longitude=longitude,
        coordinates_raw=coordinates_raw or None,
        infobox_extra=extra_fields,
        aircraft_list=aircraft_list,
    )

    # Apply known infobox fields; skip empty strings so the None default is kept
    for field_name, value in known_fields.items():
        if value:
            setattr(rec, field_name, value)

    return rec


# ──────────────────────────────────────────────────────────────────────────────
# Async HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

# ──────────────────────────────────────────────────────────────────────────────
# Async HTTP helpers
# ──────────────────────────────────────────────────────────────────────────────

@retry(
    stop=stop_after_attempt(6),
    wait=wait_exponential(multiplier=1.5, min=2, max=60) + wait_random(0, 3),
    retry=retry_if_exception(_is_retryable),
    before_sleep=before_sleep_log(_LOG, logging.WARNING),
    reraise=True,
)
async def _fetch(session: aiohttp.ClientSession, url: str) -> str:
    """
    Fetch *url* and return its text body.

    • Retries up to 6 times on transient errors (429, 5xx, connection loss).
    • Exponential back-off with full jitter to avoid thundering-herd retries.
    • Honours the HTTP ``Retry-After`` header on 429 responses by sleeping for
      the requested duration before re-raising so tenacity can reschedule.
    """
    async with session.get(url, headers=HEADERS, timeout=_TIMEOUT) as resp:
        if resp.status == 429:
            retry_after = float(resp.headers.get("Retry-After", 10))
            _LOG.warning("Rate limited by %s — honouring Retry-After: %.1fs", url, retry_after)
            await asyncio.sleep(retry_after)
            resp.raise_for_status()
        resp.raise_for_status()
        return await resp.text()


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

async def fetch_index_entries() -> list[dict]:
    """Fetch the main index page and return all accident metadata dicts."""
    connector = aiohttp.TCPConnector(ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:
        html = await _fetch(session, INDEX_URL)
    return parse_index_page(html)


async def scrape_all(
    entries: list[dict],
    concurrency: int = 5,
    delay: float = 0.4,
    progress_callback=None,
    year_filter: tuple[int, int] | None = None,
) -> list[AccidentRecord]:
    """
    Scrape all accident articles concurrently.

    Args:
        entries:            List of metadata dicts from fetch_index_entries().
        concurrency:        Max simultaneous HTTP requests.
        delay:              Polite sleep (seconds) after each individual fetch.
        progress_callback:  Called with (completed: int, total: int, record) after each fetch.
        year_filter:        Optional (start_year, end_year) inclusive tuple to restrict scraping.
    """
    if year_filter:
        start_y, end_y = year_filter
        entries = [
            e for e in entries
            if e["year"].isdigit() and start_y <= int(e["year"]) <= end_y
        ]

    semaphore = asyncio.Semaphore(concurrency)
    results: list[AccidentRecord | None] = [None] * len(entries)
    completed = 0

    # One shared session for the entire scrape run — enables connection pooling
    # and avoids the overhead of creating a new TCP connection per URL.
    connector = aiohttp.TCPConnector(limit=concurrency * 2, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=connector) as session:

        async def scrape_one(idx: int, meta: dict) -> None:
            nonlocal completed
            async with semaphore:
                url = meta["wikipedia_url"]
                try:
                    html = await _fetch(session, url)
                    rec = parse_article(html, url, meta)
                except Exception as exc:
                    _LOG.error("Failed to scrape %s: %s", url, exc)
                    rec = AccidentRecord(
                        decade=meta["decade"],
                        year=meta["year"],
                        index_summary=meta["index_summary"],
                        wikipedia_url=url,
                        scrape_error=str(exc),
                    )
                results[idx] = rec
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(entries), rec)
                await asyncio.sleep(delay)

        await asyncio.gather(*[scrape_one(i, m) for i, m in enumerate(entries)])

    return [r for r in results if r is not None]
