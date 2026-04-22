from __future__ import annotations

import re
import secrets
from datetime import date, datetime
from pathlib import Path
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

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
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}-\d{{1,2}},\s+\d{{4}}\b",
    r"\b\d{4}-\d{2}-\d{2}\b",
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


def is_future_event_query(question: str) -> bool:
    q = (question or "").lower()
    return any(term in q for term in FUTURE_EVENT_TERMS)


def parse_event_date_from_text(text: str) -> date | None:
    if not text:
        return None

    for pattern in DATE_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if not match:
            continue

        raw = match.group(0)

        # Convert "June 5-6, 2026" -> "June 5, 2026"
        raw = re.sub(
            r"(\b[A-Za-z]+)\s+(\d{1,2})-\d{1,2},\s+(\d{4})",
            r"\1 \2, \3",
            raw,
        )

        for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                pass

    return None


def extract_best_event_date(chunk: dict[str, Any]) -> date | None:
    candidates = [
        chunk.get("event_date"),
        chunk.get("event_start_date"),
        chunk.get("published_at"),
        chunk.get("title"),
        chunk.get("text"),
    ]

    for candidate in candidates:
        if not candidate:
            continue

        if isinstance(candidate, str):
            parsed = parse_event_date_from_text(candidate)
            if parsed:
                return parsed

    return None


def filter_to_future_event_chunks(chunks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    future_chunks: list[dict[str, Any]] = []
    undated_chunks: list[dict[str, Any]] = []

    for chunk in chunks:
        event_date = extract_best_event_date(chunk)
        if event_date is None:
            undated_chunks.append(chunk)
            continue

        if event_date >= TODAY:
            future_chunks.append(chunk)

    # Best case: confirmed future-dated chunks only
    if future_chunks:
        return sorted(
            future_chunks,
            key=lambda c: extract_best_event_date(c) or date.max,
        )

    # Next best: undated chunks only, instead of clearly past ones
    if undated_chunks:
        return undated_chunks

    # Fallback: original chunks if everything was dated in the past
    return chunks


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
        append_log(
            {
                "question": req.question,
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

    if is_future_event_query(req.question):
        chunks = filter_to_future_event_chunks(chunks)

    if not chunks:
        response = ChatResponse(
            answer="I couldn't find relevant Green Builder Media content for that question.",
            sources=[],
        )
        append_log(
            {
                "question": req.question,
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

    append_log(
        {
            "question": req.question,
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
