#!/usr/bin/env python3
"""
Scrape City of North Vancouver events into dashboard-ready JSON.

Usage:
  python scraper.py --output events.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_BASE_URL = "https://www.cnv.org"
DEFAULT_EVENTS_PATH = "/parks-recreation/events"
DEFAULT_TIMEOUT = 25
KNOWN_FEED_URLS = [
    "https://www.trumba.com/calendars/city-of-north-vancouver-community-events.json",
    "https://www.trumba.com/calendars/city-of-north-vancouver-community-events.rss",
]


@dataclass(slots=True)
class Event:
    title: str
    start: str | None
    end: str | None
    location: str | None
    summary: str | None
    category: str | None
    url: str | None
    source: str


def make_session() -> requests.Session:
    retry = Retry(
        total=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "north-van-events-scraper/1.1 "
                "(https://github.com/your-org/north-van-events)"
            ),
            "Accept": "text/html,application/json,application/xml,text/xml;q=0.9,*/*;q=0.8",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def clean_text(value: str | None) -> str | None:
    if not value:
        return None
    return re.sub(r"\s+", " ", value).strip() or None


def parse_date(value: str | None) -> str | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None

    normalized = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    patterns = (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%dT%H%M%S",
        "%Y%m%d",
        "%B %d, %Y %I:%M %p",
        "%B %d, %Y",
        "%b %d, %Y %I:%M %p",
        "%b %d, %Y",
    )
    for pattern in patterns:
        try:
            dt = datetime.strptime(raw, pattern)
            dt = dt.replace(tzinfo=timezone.utc)
            return dt.isoformat()
        except ValueError:
            continue

    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except (TypeError, ValueError):
        pass
    return None


def is_probably_event_url(url: str | None) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    return any(token in path for token in ("event", "events", "calendar"))


def _iter_json_nodes(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_json_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_json_nodes(item)


def fetch_text(session: requests.Session, url: str, timeout: int) -> str | None:
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        logging.warning("Fetch failed for %s: %s", url, exc)
        return None
    return response.text


def pick_first_dict_value(node: dict[str, Any], keys: list[str]) -> Any:
    for key in keys:
        if key in node and node[key] not in (None, ""):
            return node[key]
    return None


def text_from_maybe_dict(value: Any) -> str | None:
    if isinstance(value, dict):
        for key in ("name", "title", "label", "address", "value", "text"):
            if key in value:
                nested = text_from_maybe_dict(value[key])
                if nested:
                    return nested
        return clean_text(json.dumps(value, ensure_ascii=False))
    if isinstance(value, list):
        parts = [clean_text(text_from_maybe_dict(item)) for item in value]
        joined = ", ".join(part for part in parts if part)
        return clean_text(joined)
    if isinstance(value, str):
        return clean_text(value)
    return clean_text(str(value)) if value is not None else None


def extract_json_ld_events(soup: BeautifulSoup, page_url: str) -> list[Event]:
    events: list[Event] = []
    scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
    for script in scripts:
        text = script.string or script.get_text(strip=True)
        if not text:
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue

        for node in _iter_json_nodes(payload):
            node_type = node.get("@type")
            if isinstance(node_type, list):
                is_event = "Event" in node_type
            else:
                is_event = node_type == "Event"
            if not is_event:
                continue

            loc = node.get("location")
            if isinstance(loc, dict):
                loc_text = loc.get("name") or loc.get("address")
            else:
                loc_text = loc

            url = node.get("url")
            if isinstance(url, str):
                url = urljoin(page_url, url)

            events.append(
                Event(
                    title=clean_text(node.get("name")) or "Untitled Event",
                    start=parse_date(node.get("startDate")),
                    end=parse_date(node.get("endDate")),
                    location=clean_text(text_from_maybe_dict(loc_text)),
                    summary=clean_text(node.get("description")),
                    category=clean_text(node.get("eventStatus")),
                    url=url if isinstance(url, str) else None,
                    source="json-ld",
                )
            )
    return events


def parse_trumba_json(text: str, source_url: str) -> list[Event]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return []

    events: list[Event] = []
    for node in _iter_json_nodes(payload):
        if not isinstance(node, dict):
            continue
        title = text_from_maybe_dict(
            pick_first_dict_value(node, ["title", "name", "eventTitle"])
        )
        if not title:
            continue

        start_raw = pick_first_dict_value(
            node,
            [
                "start",
                "startDate",
                "startDateTime",
                "localStartDateTime",
                "eventDate",
                "date",
            ],
        )
        end_raw = pick_first_dict_value(
            node,
            ["end", "endDate", "endDateTime", "localEndDateTime"],
        )
        link_raw = pick_first_dict_value(node, ["url", "link", "eventUrl", "detailUrl"])
        category_raw = pick_first_dict_value(
            node, ["category", "categories", "calendar", "calendarName"]
        )
        location_raw = pick_first_dict_value(
            node, ["location", "where", "venue", "address", "locationName"]
        )
        summary_raw = pick_first_dict_value(
            node, ["description", "summary", "body", "details"]
        )

        url = (
            urljoin(source_url, str(link_raw))
            if isinstance(link_raw, str) and link_raw.strip()
            else None
        )
        events.append(
            Event(
                title=title,
                start=parse_date(text_from_maybe_dict(start_raw)),
                end=parse_date(text_from_maybe_dict(end_raw)),
                location=text_from_maybe_dict(location_raw),
                summary=text_from_maybe_dict(summary_raw),
                category=text_from_maybe_dict(category_raw),
                url=url,
                source="trumba-json",
            )
        )
    return events


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1].lower() if "}" in tag else tag.lower()


def find_first_text_by_local_names(item: ET.Element, names: set[str]) -> str | None:
    for child in item.iter():
        if local_name(child.tag) in names:
            text = clean_text(" ".join(child.itertext()))
            if text:
                return text
    return None


def parse_rss_description_fields(description: str | None) -> tuple[str | None, str | None]:
    if not description:
        return (None, None)
    plain = BeautifulSoup(description, "html.parser").get_text("\n", strip=True)
    when_match = re.search(r"(?im)^when:\s*(.+)$", plain)
    where_match = re.search(r"(?im)^where:\s*(.+)$", plain)
    if not where_match:
        at_match = re.search(r"(?im)\s@\s*([^\n]+)$", plain)
        where_match = at_match
    return (
        clean_text(when_match.group(1)) if when_match else None,
        clean_text(where_match.group(1)) if where_match else None,
    )


def parse_start_from_description(description: str | None) -> str | None:
    if not description:
        return None
    plain = BeautifulSoup(description, "html.parser").get_text(" ", strip=True)
    plain = clean_text(plain)
    if not plain:
        return None

    # Example: "Wednesday, February 18, 2026, 6:00 PM - 8:00 PM @ ..."
    dt_match = re.search(
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s*"
        r"([A-Za-z]+ \d{1,2}, \d{4})(?:,\s*([0-9]{1,2}:[0-9]{2}\s*[AP]M))?",
        plain,
        flags=re.IGNORECASE,
    )
    if dt_match:
        date_part = dt_match.group(1)
        time_part = dt_match.group(2)
        candidate = f"{date_part} {time_part}" if time_part else date_part
        parsed = parse_date(candidate)
        if parsed:
            return parsed

    date_only_match = re.search(r"\b([A-Za-z]+ \d{1,2}, \d{4})\b", plain)
    if date_only_match:
        return parse_date(date_only_match.group(1))
    return None


def parse_start_from_link(link: str | None) -> str | None:
    if not link:
        return None
    match = re.search(r"(?:[?&]date=)(\d{8})", link)
    if not match:
        return None
    value = match.group(1)
    return parse_date(value)


def parse_trumba_rss(text: str, source_url: str) -> list[Event]:
    try:
        root = ET.fromstring(text)
    except ET.ParseError:
        return []

    items = list(root.findall(".//item"))
    if not items:
        items = list(root.findall(".//{http://www.w3.org/2005/Atom}entry"))

    events: list[Event] = []
    for item in items:
        title = clean_text(find_first_text_by_local_names(item, {"title"}))
        if not title:
            continue

        link = find_first_text_by_local_names(item, {"link", "url"})
        if not link:
            for child in item:
                if local_name(child.tag) == "link":
                    href = child.attrib.get("href")
                    if href:
                        link = href
                        break

        description = find_first_text_by_local_names(item, {"description", "summary"})
        category = find_first_text_by_local_names(item, {"category"})
        location = find_first_text_by_local_names(
            item, {"location", "where", "venue", "locationname"}
        )
        start_text = find_first_text_by_local_names(
            item,
            {
                "dtstart",
                "start",
                "startdate",
                "startdatetime",
                "eventstartdate",
                "when",
            },
        )
        end_text = find_first_text_by_local_names(
            item, {"dtend", "end", "enddate", "enddatetime", "eventenddate"}
        )

        desc_when, desc_where = parse_rss_description_fields(description)
        start = (
            parse_date(start_text)
            or parse_date(desc_when)
            or parse_start_from_description(description)
            or parse_start_from_link(link)
        )
        end = parse_date(end_text)
        url = urljoin(source_url, link) if link else None

        events.append(
            Event(
                title=title,
                start=start,
                end=end,
                location=location or desc_where,
                summary=clean_text(
                    BeautifulSoup(description or "", "html.parser").get_text(" ", strip=True)
                ),
                category=category,
                url=url,
                source="trumba-rss",
            )
        )
    return events


def pick_text(container: Any, selectors: list[str]) -> str | None:
    for selector in selectors:
        el = container.select_one(selector)
        if el:
            text = clean_text(el.get_text(" ", strip=True))
            if text:
                return text
    return None


def extract_card_events(soup: BeautifulSoup, page_url: str) -> list[Event]:
    events: list[Event] = []
    roots = soup.select(
        "main, #main-content, #content, .main-content, .content, .page-content, article"
    )
    if not roots:
        roots = [soup]

    seen_cards: set[int] = set()
    for root in roots:
        card_candidates = root.select(
            ".event, .events-list-item, .calendar-item, .event-card, [class*='event-'], [class*='calendar-'], article, li"
        )
        for card in card_candidates:
            marker = id(card)
            if marker in seen_cards:
                continue
            seen_cards.add(marker)

            title = pick_text(card, ["h1", "h2", "h3", ".title", ".event-title", "a"])
            if not title:
                continue
            link = card.select_one("a[href]")
            href = link["href"] if link and link.has_attr("href") else None
            url = urljoin(page_url, href) if href else None

            date_text = pick_text(
                card,
                [
                    "time[datetime]",
                    "time",
                    ".date",
                    ".event-date",
                    "[class*=date]",
                ],
            )
            time_el = card.select_one("time[datetime]")
            if time_el and time_el.has_attr("datetime"):
                start = parse_date(time_el["datetime"])
            else:
                start = parse_date(date_text)

            summary = pick_text(card, [".summary", ".description", "p"])
            location = pick_text(card, [".location", "[class*=location]", ".venue"])
            category = pick_text(card, [".category", ".tag", "[class*=category]"])

            if not (start or summary or location or (url and is_probably_event_url(url))):
                continue

            events.append(
                Event(
                    title=title,
                    start=start,
                    end=None,
                    location=location,
                    summary=summary,
                    category=category,
                    url=url if (not url or is_probably_event_url(url)) else None,
                    source="html-card",
                )
            )
    return events


def dedupe_events(events: list[Event]) -> list[Event]:
    seen: set[tuple[str, str | None, str | None]] = set()
    deduped: list[Event] = []
    for event in events:
        key = (
            (event.title or "").strip().lower(),
            event.start,
            (event.url or "").strip().lower() or None,
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(event)
    return deduped


def filter_by_days(events: list[Event], days: int | None) -> list[Event]:
    if days is None:
        return events
    now = datetime.now(timezone.utc)
    upper = now + timedelta(days=days)
    filtered: list[Event] = []
    for event in events:
        if not event.start:
            filtered.append(event)
            continue
        try:
            dt = datetime.fromisoformat(event.start)
        except ValueError:
            filtered.append(event)
            continue
        if now <= dt <= upper:
            filtered.append(event)
    return filtered


def sort_events(events: list[Event]) -> list[Event]:
    def sort_key(item: Event):
        if not item.start:
            return (1, datetime.max.replace(tzinfo=timezone.utc))
        try:
            return (0, datetime.fromisoformat(item.start))
        except ValueError:
            return (1, datetime.max.replace(tzinfo=timezone.utc))

    return sorted(events, key=sort_key)


def discover_feed_urls(session: requests.Session, base_url: str, timeout: int) -> list[str]:
    rss_page = urljoin(base_url, "/RSS")
    text = fetch_text(session, rss_page, timeout)
    discovered: list[str] = []
    if not text:
        return discovered
    soup = BeautifulSoup(text, "html.parser")
    for link in soup.select("a[href]"):
        href = link.get("href")
        if not href:
            continue
        full = urljoin(rss_page, href.strip())
        parsed = urlparse(full)
        if "trumba.com" not in parsed.netloc.lower():
            continue
        if full.lower().endswith((".rss", ".xml", ".json")):
            discovered.append(full)
    return list(dict.fromkeys(discovered))


def scrape_html_page(session: requests.Session, url: str, timeout: int) -> list[Event]:
    text = fetch_text(session, url, timeout)
    if not text:
        return []
    soup = BeautifulSoup(text, "html.parser")
    events: list[Event] = []
    events.extend(extract_json_ld_events(soup, url))
    events.extend(extract_card_events(soup, url))
    return events


def scrape_events(
    session: requests.Session, base_url: str, events_url: str, timeout: int
) -> tuple[list[Event], list[str], list[str]]:
    warnings: list[str] = []
    sources_tried: list[str] = []
    events: list[Event] = []

    feed_urls = discover_feed_urls(session, base_url, timeout)
    for url in KNOWN_FEED_URLS:
        if url not in feed_urls:
            feed_urls.append(url)

    for feed_url in feed_urls:
        sources_tried.append(feed_url)
        text = fetch_text(session, feed_url, timeout)
        if not text:
            warnings.append(f"Failed to fetch feed: {feed_url}")
            continue

        parsed: list[Event] = []
        lower_url = feed_url.lower()
        content_starts_with_xml = text.lstrip().startswith("<?xml")
        if lower_url.endswith(".json") and not content_starts_with_xml:
            parsed = parse_trumba_json(text, feed_url)
            if not parsed:
                parsed = parse_trumba_rss(text, feed_url)
        else:
            parsed = parse_trumba_rss(text, feed_url)
            if not parsed:
                parsed = parse_trumba_json(text, feed_url)

        if parsed:
            logging.info("Parsed %d events from %s", len(parsed), feed_url)
            events.extend(parsed)
        else:
            warnings.append(f"No events parsed from feed: {feed_url}")

    if not events:
        fallback_pages = [
            events_url,
            urljoin(base_url, "/Calendar-of-Events"),
            urljoin(base_url, "/parks-recreation/events"),
        ]
        for page_url in fallback_pages:
            if page_url in sources_tried:
                continue
            sources_tried.append(page_url)
            parsed = scrape_html_page(session, page_url, timeout)
            if parsed:
                logging.info("Parsed %d events from %s", len(parsed), page_url)
                events.extend(parsed)
            else:
                warnings.append(f"No events parsed from HTML page: {page_url}")

    return (dedupe_events(events), warnings, sources_tried)


def write_output(
    path: str,
    source_url: str,
    events: list[Event],
    warnings: list[str] | None = None,
    sources_tried: list[str] | None = None,
) -> None:
    payload = {
        "metadata": {
            "source": source_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "event_count": len(events),
            "generator": "north-van-events scraper.py",
        },
        "events": [asdict(event) for event in events],
    }
    if warnings:
        payload["metadata"]["warnings"] = warnings
    if sources_tried:
        payload["metadata"]["sources_tried"] = sources_tried
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape City of North Vancouver events into JSON."
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"Base URL (default: {DEFAULT_BASE_URL})",
    )
    parser.add_argument(
        "--events-path",
        default=DEFAULT_EVENTS_PATH,
        help=f"Events path (default: {DEFAULT_EVENTS_PATH})",
    )
    parser.add_argument(
        "--output",
        default="events.json",
        help="Output JSON path (default: events.json)",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=180,
        help="Only keep events happening in the next N days (default: 180, use -1 to disable)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"HTTP timeout seconds (default: {DEFAULT_TIMEOUT})",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    events_url = urljoin(args.base_url, args.events_path)
    days = None if args.days < 0 else args.days

    logging.info("Scraping events from %s", events_url)
    session = make_session()

    warnings: list[str] = []
    sources_tried: list[str] = []
    events: list[Event] = []
    try:
        events, scrape_warnings, sources_tried = scrape_events(
            session, args.base_url, events_url, timeout=args.timeout
        )
        warnings.extend(scrape_warnings)
    except Exception as exc:
        logging.exception("Unexpected scraper failure: %s", exc)
        warnings.append(f"Unexpected scraper failure: {exc}")

    events = filter_by_days(events, days)
    events = sort_events(events)
    write_output(
        args.output,
        events_url,
        events,
        warnings=warnings,
        sources_tried=sources_tried,
    )

    if warnings:
        logging.warning("Completed with %d warning(s)", len(warnings))
    logging.info("Wrote %d events to %s", len(events), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
