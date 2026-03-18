from pathlib import Path
from threading import Thread
from typing import List

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.state import (
    reset_mode,
    add_log,
    set_running,
    set_stop,
    get_mode_state,
    APP_STATE,
)
from backend.export_utils import export_results
from backend.scraper.google_business import run_google_business_scrape
from backend.scraper.social_lookup import run_social_lookup_scrape

app = FastAPI(title="LeadsMan")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/frontend", StaticFiles(directory="frontend"), name="frontend")


class GoogleBusinessRequest(BaseModel):
    keywords: str
    locations: str
    enable_email_scraping: bool = False


class SocialLookupRequest(BaseModel):
    keyword: str
    location: str = ""
    max_pages: int = 5
    platforms: List[str]


class StopRequest(BaseModel):
    mode: str


@app.get("/", response_class=HTMLResponse)
def serve_home():
    return Path("frontend/index.html").read_text(encoding="utf-8")


@app.post("/api/google-business/start")
def start_google_business(payload: GoogleBusinessRequest):
    mode = "google_business"
    reset_mode(mode)
    set_running(mode, True)
    set_stop(mode, False)
    add_log(mode, "Starting Google Business scraping...")

    thread = Thread(
        target=run_google_business_scrape,
        args=(payload.keywords, payload.locations, payload.enable_email_scraping),
        daemon=True,
    )
    APP_STATE[mode]["thread"] = thread
    thread.start()

    return {"message": "Google Business scraping started"}


@app.post("/api/social-lookup/start")
def start_social_lookup(payload: SocialLookupRequest):
    mode = "social_lookup"
    reset_mode(mode)
    set_running(mode, True)
    set_stop(mode, False)
    add_log(mode, "Starting Social Lookup scraping...")

    thread = Thread(
        target=run_social_lookup_scrape,
        args=(payload.keyword, payload.location, payload.platforms, payload.max_pages),
        daemon=True,
    )
    APP_STATE[mode]["thread"] = thread
    thread.start()

    return {"message": "Social Lookup scraping started"}


@app.post("/api/stop")
def stop_scraping(payload: StopRequest):
    set_stop(payload.mode, True)
    add_log(payload.mode, "Stop requested by user")
    return {"message": f"Stop requested for {payload.mode}"}


@app.get("/api/progress")
def get_progress(mode: str):
    return get_mode_state(mode)


@app.get("/api/export")
def export_data(mode: str, format: str):
    state = get_mode_state(mode)
    file_path = export_results(state["results"], mode, format)

    media_type = "text/csv"
    if format == "xlsx":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type=media_type,
    )