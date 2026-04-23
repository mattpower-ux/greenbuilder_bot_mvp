from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Any

import gspread
from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from google.oauth2.service_account import Credentials

from app.admin_ui import HTML as ADMIN_HTML
from app.config import get_settings
from app.corrections import (
    append_log,
    find_correction,
    load_corrections,
    load_logs,
    save_correction,
)
from app.generation import answer_question, summarize_private_usage
from app.models import (
    ChatRequest,
    ChatResponse,
    CorrectionCreate,
    CorrectionListResponse,
    LogListResponse,
    SourceItem,
)
from app.retrieval import search

settings = get_settings()

app = FastAPI(title="Green Builder Media Retrieval Bot", version="0.3.0")
security = HTTPBasic()

# Temporary open CORS for testing the HubSpot embed.
# Once the bot is working, tighten this back down to your real domains.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TODAY = date.today()
DAILY_CRAWL_INTERVAL_SECONDS = 60 * 60 * 24

FUTURE_EVENT_TERMS = [
    "coming up",
    "upcoming",
    "future conference",
    "future conferences",
    "future event",
    "future events",
    "next conference",
    "next conferences",
    "next event",
    "next events",
    "conference schedule",
    "event schedule",
    "calendar",
    "webinar",
    "webinars",
    "summit",
    "summits",
    "symposium",
    "symposiums",
    "conference",
    "conferences",
]

MONTH_PATTERN = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

DATE_PATTERNS = [
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}},\s+\d{{4}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\s*[-–—]\s*\d{{1,2}},\s+\d{{4}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\s*[-–—]\s*\d{{1,2}}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
    r"\b\d{1,2}/\d{1,2}/\d{4}\b",
]


def admin_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    expected_username = settings.admin_username.encode("utf-8")
    expected_password = settings.admin_password.encode("utf-8")
    given_username = credentials.username.encode("utf-8")
    given_password = credentials.password.encode("utf-8")

    if not (
        secrets.compare_digest(given_username, expected_username)
        and secrets.compare_digest(given_password, expected_password)
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


def get_google_sheet():
    raw_json = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    sheet_id = os.getenv("GOOGLE_SHEET_ID", "").strip()

    if not raw_json:
        raise RuntimeError("Missing GOOGLE_SERVICE_ACCOUNT_JSON environment variable.")
    if not sheet_id:
        raise RuntimeError("Missing GOOGLE_SHEET_ID environment variable.")

    service_account_info = json.loads(raw_json)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    credentials = Credentials.from_service_account_info(
        service_account_info,
        scopes=scopes,
    )

    client = gspread.authorize(credentials)
    spreadsheet = client.open_by_key(sheet_id)
    worksheet = spreadsheet.sheet1
    return worksheet


def ensure_sheet_header(worksheet) -> None:
    expected_header = [
        "timestamp_utc",
        "session_id",
        "page_url",
        "referrer",
        "user_agent",
        "event_query",
        "question",
        "answer",
        "sources_json",
        "private_archive_used",
        "attribution_note",
        "correction_applied",
        "correction_id",
    ]

    existing_header = worksheet.row_values(1)
    if existing_header != expected_header:
        worksheet.update("A1:M1", [expected_header])


def log_to_google_sheet(payload: dict) -> None:
    worksheet = get_google_sheet()
    ensure_sheet_header(worksheet)

    row = [
        datetime.utcnow().isoformat(),
        payload.get("session_id", "") or "",
        payload.get("page_url", "") or "",
        payload.get("referrer", "") or "",
        payload.get("user_agent", "") or "",
        str(payload.get("event_query", False)),
        payload.get("question", "") or "",
        payload.get("answer", "") or "",
        json.dumps(payload.get("public_sources", []), ensure_ascii=False),
        str(payload.get("private_archive_used", False)),
        payload.get("attribution_note", "") or "",
        str(payload.get("correction_applied", False)),
        payload.get("correction_id", "") or "",
    ]

    worksheet.append_row(row, value_input_option="RAW")


def append_log_everywhere(payload: dict) -> None:
    append_log(payload)
    try:
        log_to_google_sheet(payload)
    except Exception as exc:
        print(f"Google Sheets logging failed: {exc}")


async def run_crawl_and_reindex_once() -> None:
    """
    Run the site crawl and index rebuild inside the same service so it uses
    the same environment and storage as the live app.
    """
    print("Starting scheduled crawl + index rebuild...")

    # Import inside the function to avoid circular/import-time issues.
    from app.crawl_greenbuilder import main as crawl_main
    from app.build_index import main as build_main

    await crawl_main()
    build_main()

    print("Scheduled crawl + index rebuild completed.")


async def run_daily_crawl_loop() -> None:
    """
    Run once on startup, then every 24 hours.
    """
    while True:
        try:
            await run_crawl_and_reindex_once()
        except Exception as exc:
            print(f"Scheduled crawl + index rebuild failed: {exc}")

        await asyncio.sleep(DAILY_CRAWL_INTERVAL_SECONDS)


@app.on_event("startup")
async def startup_event() -> None:
    # Start the crawl loop in the background so the API can still serve traffic.
    asyncio.create_task(run_daily_crawl_loop())


def is_future_event_query(question: str) -> bool:
    q = (question or "").lower()
    return any(term in q for term in FUTURE_EVENT_TERMS)


def parse_published_year(published_at: str | None) -> int | None:
    if not published_at:
        return None

    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
        return dt.year
    except Exception:
        pass

    match = re.search(r"\b(20\d{2}|19\d{2})\b", published_at)
    if match:
        return int(match.group(1))

    return None


def parse_single_event_date(raw: str, default_year: int | None = None) -> date | None:
    if not raw:
        return None

    raw = raw.strip()

    # Convert "June 5-6, 2026" or "June 5–6, 2026" -> "June 5, 2026"
    raw = re.sub(
        r"(\b[A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*\d{1,2},\s+(\d{4})",
        r"\1 \2, \3",
        raw,
    )

    # Convert "June 5-6" or "June 5–6" -> "June 5"
    raw = re.sub(
        r"(\b[A-Za-z]+)\s+(\d{1,2})\s*[-–—]\s*\d{1,2}",
        r"\1 \2",
        raw,
    )

    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            pass

    if default_year is not None:
        for fmt in ("%B %d", "%b %d"):
            try:
                partial = datetime.strptime(raw, fmt)
                return date(default_year, partial.month, partial.day)
            except ValueError:
                pass

    return None


def extract_all_event_dates_from_text(text: str, default_year: int | None = None) -> list[date]:
    if not text:
        return []

    found_dates: list[date] = []

    for pattern in DATE_PATTERNS:
        for match in re.finditer(pattern, text, re.IGNORECASE):
            parsed = parse_single_event_date(match.group(0), default_year=default_year)
            if parsed:
                found_dates.append(parsed)

    unique_dates: list[date] = []
    seen = set()
    for d in found_dates:
        if d not in seen:
            unique_dates.append(d)
            seen.add(d)

    return unique_dates


def extract_future_event_dates(chunk: dict[str, Any]) -> list[date]:
    future_dates: list[date] = []
    published_year = parse_published_year(chunk.get("published_at")) or TODAY.year

    for field in ("event_date", "event_start_date"):
        value = chunk.get(field)
        if isinstance(value, str):
            parsed = parse_single_event_date(value, default_year=published_year)
            if parsed and parsed >= TODAY:
                future_dates.append(parsed)

    for field in ("title", "text"):
        value = chunk.get(field)
        if isinstance(value, str):
            for parsed in extract_all_event_dates_from_text(value, default_year=published_year):
                if parsed >= TODAY:
                    future_dates.append(parsed)

    unique_dates: list[date] = []
    seen = set()
    for d in future_dates:
        if d not in seen:
            unique_dates.append(d)
            seen.add(d)

    return sorted(unique_dates)


def extract_best_event_date(chunk: dict[str, Any]) -> date | None:
    future_dates = extract_future_event_dates(chunk)
    if future_dates:
        return future_dates[0]

    published_at = chunk.get("published_at")
    if isinstance(published_at, str):
        return parse_single_event_date(published_at)

    return None


def filter_to_future_event_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    future_chunks: list[dict[str, Any]] = []

    for chunk in chunks:
        future_dates = extract_future_event_dates(chunk)
        if future_dates:
            enriched = dict(chunk)
            enriched["_next_future_event_date"] = future_dates[0].isoformat()
            future_chunks.append(enriched)

    return sorted(
        future_chunks,
        key=lambda c: c.get("_next_future_event_date", "9999-12-31"),
    )


@app.get("/health")
def health() -> dict:
    return {"ok": True}


@app.get("/widget.js")
def widget() -> FileResponse:
    root = Path(__file__).resolve().parents[1]
    widget_path = root / "widget" / "embed.js"
    return FileResponse(widget_path, media_type="application/javascript")


@app.post("/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    correction = find_correction(req.question)
    if correction:
        response = ChatResponse(
            answer=correction["answer_override"],
            sources=[],
            corrected_by_editor=True,
            correction_note=correction.get("editor_note")
            or f"Editor override by {correction.get('editor_name') or 'editor'}",
        )
        append_log_everywhere(
            {
                "question": req.question,
                "session_id": req.session_id,
                "page_url": req.page_url,
                "referrer": req.referrer,
                "user_agent": req.user_agent,
                "event_query": is_future_event_query(req.question),
                "answer": response.answer,
                "public_sources": [],
                "private_archive_used": False,
                "correction_applied": True,
                "correction_id": correction.get("id"),
            }
        )
        return response

    try:
        chunks = search(req.question)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Search failed: {exc}") from exc

    future_query = is_future_event_query(req.question)
    if future_query:
        chunks = filter_to_future_event_chunks(chunks)

        if not chunks:
            response = ChatResponse(
                answer=(
                    "I’m not seeing any confirmed future conferences in the current Green Builder Media excerpts. "
                    "The available event-related content appears to be past or undated, so I can’t verify an upcoming conference from the retrieved material."
                ),
                sources=[],
            )
            append_log_everywhere(
                {
                    "question": req.question,
                    "session_id": req.session_id,
                    "page_url": req.page_url,
                    "referrer": req.referrer,
                    "user_agent": req.user_agent,
                    "event_query": future_query,
                    "answer": response.answer,
                    "public_sources": [],
                    "private_archive_used": False,
                }
            )
            return response

    if not chunks:
        response = ChatResponse(
            answer="I couldn't find relevant Green Builder Media content for that question.",
            sources=[],
        )
        append_log_everywhere(
            {
                "question": req.question,
                "session_id": req.session_id,
                "page_url": req.page_url,
                "referrer": req.referrer,
                "user_agent": req.user_agent,
                "event_query": future_query,
                "answer": response.answer,
                "public_sources": [],
                "private_archive_used": False,
            }
        )
        return response

    try:
        answer = answer_question(req.question, chunks)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Generation failed: {exc}") from exc

    private_used, attribution_note = summarize_private_usage(chunks)

    seen = set()
    sources = []
    for chunk in chunks:
        visibility = chunk.get("visibility", "public")
        if visibility != "public":
            continue

        key = chunk.get("url")
        if not key or key in seen:
            continue

        seen.add(key)
        sources.append(
            SourceItem(
                title=chunk.get("title", "Untitled"),
                url=chunk.get("url", ""),
                published_at=chunk.get("published_at"),
                excerpt=chunk.get("text", "")[:240].strip(),
                score=float(chunk.get("score", 0.0)),
                visibility=visibility,
                attribution_label=chunk.get("attribution_label"),
                surface_policy=chunk.get("surface_policy"),
            )
        )

    response = ChatResponse(
        answer=answer,
        sources=sources[:5],
        private_archive_used=private_used,
        attribution_note=attribution_note,
    )

    append_log_everywhere(
        {
            "question": req.question,
            "session_id": req.session_id,
            "page_url": req.page_url,
            "referrer": req.referrer,
            "user_agent": req.user_agent,
            "event_query": future_query,
            "answer": response.answer,
            "public_sources": [s.model_dump() for s in response.sources],
            "private_archive_used": private_used,
            "attribution_note": attribution_note,
        }
    )
    return response


@app.get("/admin", response_class=HTMLResponse)
def admin_page(_: str = Depends(admin_auth)) -> HTMLResponse:
    return HTMLResponse(ADMIN_HTML)


@app.get("/api/admin/logs", response_model=LogListResponse)
def admin_logs(_: str = Depends(admin_auth)) -> LogListResponse:
    return LogListResponse(logs=load_logs())


@app.get("/api/admin/corrections", response_model=CorrectionListResponse)
def admin_corrections(_: str = Depends(admin_auth)) -> CorrectionListResponse:
    return CorrectionListResponse(corrections=load_corrections())


@app.post("/api/admin/corrections")
def admin_create_correction(
    payload: CorrectionCreate, username: str = Depends(admin_auth)
) -> dict:
    saved = save_correction(
        {**payload.model_dump(), "editor_name": payload.editor_name or username}
    )
    return {"ok": True, "message": "Correction saved", "correction": saved}


@app.get("/")
def root() -> Response:
    return Response(
        "Green Builder Media Retrieval Bot is running.",
        media_type="text/plain",
    )
