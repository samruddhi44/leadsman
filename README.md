# LeadsMan

LeadsMan is a FastAPI + Playwright dashboard for collecting lead data from:

- Google Business listings
- Public social profiles and channel pages scraped with Playwright

The frontend is served by the backend, so the app should be opened through the FastAPI server instead of opening `index.html` directly.

## Stack

- Python
- FastAPI
- Playwright
- Pandas
- HTML, CSS, JavaScript

## Project layout

- `backend/app.py`: API server and frontend entry point
- `backend/state.py`: in-memory scraper state and progress tracking
- `backend/export_utils.py`: CSV/XLSX export helpers
- `backend/scraper/google_business.py`: Google Business scraping flow
- `backend/scraper/social_lookup.py`: direct social profile lookup flow
- `frontend/`: static UI served by FastAPI

## Run locally

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Install the Playwright Chromium browser once:

```bash
python -m playwright install chromium
```

3. Start the app from the repository root:

```bash
python backend/app.py
```

4. Open:

```text
http://127.0.0.1:8000
```

## Notes

- Exports are written to `backend/exports/`.
- The app keeps runtime state in memory, so restarting the server clears current progress/results.
- Social Lookup tries direct public platform search first and falls back to public discovery when a platform blocks anonymous search.
- Social Lookup exports these columns: `title`, `description`, `domain`, `phones`, `emails`, `link`, `source`, `category`, `location`.
