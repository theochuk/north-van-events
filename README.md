# North Van Events Scraper

Production-ready scraper and dashboard for City of North Vancouver events.

## What this project includes

- `scraper.py`: resilient Python scraper that extracts events from City of North Vancouver pages.
- `index.html`: interactive dashboard to browse, search, and filter events.
- `styles.css`: professional, responsive styling for the dashboard.
- `events.json` (generated): normalized data file consumed by the dashboard.

## Features

- HTTP retry/backoff and timeout handling
- Multiple extraction strategies:
  - JSON-LD `Event` schema
  - HTML event card parsing fallback
- Event normalization:
  - title, date/time, location, category, summary, URL, source
- De-duplication and date sorting
- Configurable CLI options (`--base-url`, `--events-path`, `--days`, `--output`)
- Browser dashboard with:
  - full-text search
  - category filter
  - location filter
  - date-from filter
  - sorting options
  - high-level stats

## Requirements

- Python 3.10+
- Pip packages:
  - `requests`
  - `beautifulsoup4`

Install dependencies:

```bash
python -m pip install requests beautifulsoup4
```

## Usage

Generate events JSON:

```bash
python scraper.py --output events.json
```

Optional arguments:

```bash
python scraper.py \
  --base-url "https://www.cnv.org" \
  --events-path "/Parks-Recreation-and-Culture/Events" \
  --days 180 \
  --timeout 25 \
  --log-level INFO \
  --output events.json
```

Notes:

- Set `--days -1` to disable date-range filtering.
- If the target page structure changes, adjust HTML selectors in `extract_card_events()`.

## Run the dashboard

Serve files via a local web server (recommended, avoids browser file access limits):

```bash
python -m http.server 8000
```

Then open:

- `http://localhost:8000/index.html`

## Output format

`events.json` structure:

```json
{
  "metadata": {
    "source": "https://www.cnv.org/Parks-Recreation-and-Culture/Events",
    "scraped_at": "2026-01-01T00:00:00+00:00",
    "event_count": 0,
    "generator": "north-van-events scraper.py"
  },
  "events": [
    {
      "title": "Sample Event",
      "start": "2026-01-15T18:30:00+00:00",
      "end": null,
      "location": "North Vancouver",
      "summary": "Event summary",
      "category": "General",
      "url": "https://www.cnv.org/...",
      "source": "json-ld"
    }
  ]
}
```

## Production notes

- Schedule `scraper.py` via cron/CI to refresh `events.json`.
- Keep dashboard static for low hosting overhead.
- Log level can be raised to `DEBUG` for parser troubleshooting.
- For long-term robustness, consider adding:
  - unit tests for date parsing and dedupe logic
  - selector health checks in CI
  - alerts when event count drops unexpectedly

## License

Add your preferred license (for example: MIT).
