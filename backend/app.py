from pathlib import Path
import sys
from threading import Thread
from typing import List

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from backend.export_utils import SUPPORTED_EXPORT_FORMATS, export_results
from backend.scraper.google_business import run_google_business_scrape
from backend.scraper.social_lookup import SUPPORTED_PLATFORMS, run_social_lookup_scrape
from backend.state import (
    APP_STATE,
    VALID_MODES,
    add_log,
    get_mode_state,
    reset_mode,
    set_running,
    set_stop,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
FRONTEND_DIR = PROJECT_ROOT / "frontend"
DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000

app = FastAPI(title="LeadsMan")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/frontend", StaticFiles(directory=str(FRONTEND_DIR)), name="frontend")


class GoogleBusinessRequest(BaseModel):
    keywords: str
    locations: str
    enable_email_scraping: bool = False
    max_pages: int = 3


class SocialLookupRequest(BaseModel):
    keyword: str
    location: str = ""
    max_pages: int = 3
    platforms: List[str]


class StopRequest(BaseModel):
    mode: str


def split_csv_values(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def validate_mode_or_400(mode: str) -> str:
    normalized_mode = (mode or "").strip()
    if normalized_mode not in VALID_MODES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported mode. Use one of: {', '.join(VALID_MODES)}",
        )
    return normalized_mode


def validate_export_format_or_400(export_format: str) -> str:
    normalized_format = (export_format or "").strip().lower()
    if normalized_format not in SUPPORTED_EXPORT_FORMATS:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported export format. Use one of: {', '.join(SUPPORTED_EXPORT_FORMATS)}",
        )
    return normalized_format


def validate_social_platforms(platforms: list[str]) -> list[str]:
    cleaned_platforms = []

    for platform in platforms:
        normalized_platform = str(platform).strip().lower()
        if not normalized_platform:
            continue

        if normalized_platform not in SUPPORTED_PLATFORMS:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Unsupported social platform. "
                    f"Use one of: {', '.join(SUPPORTED_PLATFORMS)}"
                ),
            )

        if normalized_platform not in cleaned_platforms:
            cleaned_platforms.append(normalized_platform)

    if not cleaned_platforms:
        raise HTTPException(status_code=400, detail="Please select at least one platform")

    return cleaned_platforms


def build_frontend_asset_url(relative_path: str) -> str:
    asset_path = FRONTEND_DIR / relative_path
    version = int(asset_path.stat().st_mtime_ns) if asset_path.exists() else 0
    normalized_path = relative_path.replace("\\", "/")
    return f"/frontend/{normalized_path}?v={version}"


@app.get("/", response_class=HTMLResponse)
def serve_home():
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")
    html = html.replace(
        '/frontend/css/style.css',
        build_frontend_asset_url("css/style.css"),
    )
    html = html.replace(
        "/frontend/js/app.js",
        build_frontend_asset_url("js/app.js"),
    )
    return HTMLResponse(
        content=html,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


@app.post("/api/google-business/start")
def start_google_business(payload: GoogleBusinessRequest):
    mode = "google_business"
    keywords = split_csv_values(payload.keywords)
    locations = split_csv_values(payload.locations)
    max_pages = max(0, min(payload.max_pages, 10))

    if not keywords or not locations:
        raise HTTPException(
            status_code=400,
            detail="Please provide at least one keyword and one location",
        )

    current_state = get_mode_state(mode)
    if current_state["running"]:
        raise HTTPException(status_code=400, detail="Google Business scraping is already running")

    reset_mode(mode)
    set_running(mode, True)
    set_stop(mode, False)
    add_log(mode, "Starting Google Business scraping...")

    thread = Thread(
        target=run_google_business_scrape,
        args=(keywords, locations, payload.enable_email_scraping, max_pages),
        daemon=True,
    )
    APP_STATE[mode]["thread"] = thread
    thread.start()

    return {"message": "Google Business scraping started"}


@app.post("/api/social-lookup/start")
def start_social_lookup(payload: SocialLookupRequest):
    mode = "social_lookup"
    keyword = payload.keyword.strip()
    location = payload.location.strip()
    platforms = validate_social_platforms(payload.platforms)
    max_pages = max(1, min(payload.max_pages, 3))

    if not keyword:
        raise HTTPException(status_code=400, detail="Please provide a keyword")

    current_state = get_mode_state(mode)
    if current_state["running"]:
        raise HTTPException(status_code=400, detail="Social Lookup scraping is already running")

    reset_mode(mode)
    set_running(mode, True)
    set_stop(mode, False)
    add_log(mode, "Starting Social Lookup scraping...")

    thread = Thread(
        target=run_social_lookup_scrape,
        args=(keyword, location, platforms, max_pages),
        daemon=True,
    )
    APP_STATE[mode]["thread"] = thread
    thread.start()

    return {"message": "Social Lookup scraping started"}


@app.post("/api/stop")
def stop_scraping(payload: StopRequest):
    mode = validate_mode_or_400(payload.mode)
    set_stop(mode, True)
    add_log(mode, "Stop requested by user")
    return {"message": f"Stop requested for {mode}"}


@app.get("/api/progress")
def get_progress(mode: str):
    return get_mode_state(validate_mode_or_400(mode))


@app.get("/api/export")
def export_data(mode: str, format: str):
    mode = validate_mode_or_400(mode)
    export_format = validate_export_format_or_400(format)
    state = get_mode_state(mode)
    file_path = export_results(state["results"], mode, export_format)

    media_type = "text/csv"
    if export_format == "xlsx":
        media_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    return FileResponse(
        path=file_path,
        filename=file_path.name,
        media_type=media_type,
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=DEFAULT_HOST, port=DEFAULT_PORT)
