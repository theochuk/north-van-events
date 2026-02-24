#!/usr/bin/env python3
"""
Scrape events from the City of North Vancouver website and write normalized JSON.

Usage:
  python scraper.py --output data/events.json
"""

from __future__ import annotations

import argparse
import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


DEFAULT_BASE_URL = "https://www.cnv.org"
DEFAULT_EVENTS_PATH = "/Parks-Recreation-and-Culture/Events"
DEFAULT_TIMEOUT = 25


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
                "north-van-events-scraper/1.0 "
                "(https://github.com/your-org/north-van-events)"
            )
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

    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    except ValueError:
        pass

    patterns = (
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
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
    return None


def is_probably_event_url(url: str | None) -> bool:
    if not url:
        return False
    path = urlparse(url).path.lower()
    return "event" in path or "calendar" in path


def _iter_json_nodes(node: Any):
    if isinstance(node, dict):
        yield node
        for value in node.values():
            yield from _iter_json_nodes(value)
    elif isinstance(node, list):
        for item in node:
            yield from _iter_json_nodes(item)


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
                    location=clean_text(loc_text),
                    summary=clean_text(node.get("description")),
                    category=clean_text(node.get("eventStatus")),
                    url=url if isinstance(url, str) else None,
                    source="json-ld",
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
    card_candidates = soup.select(
        ".event, .events-list-item, .calendar-item, .event-card, article, li"
    )
    for card in card_candidates:
        title = pick_text(card, ["h1", "h2", "h3", ".title", ".event-title", "a"])
        link = card.select_one("a[href]")
        href = link["href"] if link and link.has_attr("href") else None
        url = urljoin(page_url, href) if href else None
        if not title:
            continue
        if url and not is_probably_event_url(url):
            continue

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

        events.append(
            Event(
                title=title,
                start=start,
                end=None,
                location=location,
                summary=summary,
                category=category,
                url=url,
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


def scrape_events(session: requests.Session, events_url: str, timeout: int) -> list[Event]:
    response = session.get(events_url, timeout=timeout)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    events = []
    events.extend(extract_json_ld_events(soup, events_url))
    events.extend(extract_card_events(soup, events_url))

    return dedupe_events(events)


def write_output(path: str, source_url: str, events: list[Event]) -> None:
    payload = {
        "metadata": {
            "source": source_url,
            "scraped_at": datetime.now(timezone.utc).isoformat(),
            "event_count": len(events),
            "generator": "north-van-events scraper.py",
        },
        "events": [asdict(event) for event in events],
    }
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
    try:
        events = scrape_events(session, events_url, timeout=args.timeout)
    except requests.RequestException as exc:
        logging.error("Failed to scrape events: %s", exc)
        return 1

    events = filter_by_days(events, days)
    events = sort_events(events)
    write_output(args.output, events_url, events)
    logging.info("Wrote %d events to %s", len(events), args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
