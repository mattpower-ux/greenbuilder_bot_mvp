from __future__ import annotations

import asyncio
import json
import os
import re
import secrets
import subprocess
import time
from datetime import date, datetime
from pathlib import Path
from typing import Any, List

import gspread
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Response, status
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

TODAY = date.today()
DAILY_CRAWL_INTERVAL_SECONDS = 60 * 60 * 24
STARTUP_CRAWL_DELAY_SECONDS = 30
ENABLE_BACKGROUND_CRAWL = os.getenv("ENABLE_BACKGROUND_CRAWL", "true").strip().lower() in {
    "1", "true", "yes", "on",
}

crawl_lock = asyncio.Lock()
rebuild_task: asyncio.Task | None = None

FUTURE_EVENT_TERMS = [
    "coming up", "upcoming", "future conference", "future conferences",
    "future event", "future events", "next conference", "next conferences",
    "next event", "next events", "conference schedule", "event schedule",
    "calendar", "webinar", "webinars", "summit", "summits", "symposium",
    "symposiums", "conference", "conferences",
]

MONTH_PATTERN = (
    "January|February|March|April|May|June|July|August|September|October|November|December|"
    "Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
)

DATE_PATTERNS = [
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}},\s+\d{{4}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\s*[-â€“â€”]\s*\d{{1,2}},\s+\d{{4}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\b",
    rf"\b({MONTH_PATTERN})\s+\d{{1,2}}\s*[-â€“â€”]\s*\d{{1,2}}\b",
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
    return spreadsheet.sheet1


def ensure_sheet_header(worksheet) -> None:
    expected_header = [
        "timestamp_utc", "session_id", "page_url", "referrer", "user_agent",
        "event_query", "question", "answer", "sources_json",
        "private_archive_used", "attribution_note", "correction_applied",
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
    if crawl_lock.locked():
        print("Scheduled crawl skipped because another crawl is already running.")
        return

    async with crawl_lock:
        print("Starting scheduled crawl + index rebuild...")
        from app.crawl_greenbuilder import main as crawl_main
        from app.build_index import main as build_main
        await crawl_main()
        build_main()
        print("Scheduled crawl + index rebuild completed.")


async def run_daily_crawl_loop() -> None:
    await asyncio.sleep(STARTUP_CRAWL_DELAY_SECONDS)
    while True:
        try:
            await run_crawl_and_reindex_once()
        except Exception as exc:
            print(f"Scheduled crawl + index rebuild failed: {exc}")
        await asyncio.sleep(DAILY_CRAWL_INTERVAL_SECONDS)


async def run_rebuild_once() -> None:
    from app.build_index import main as build_main
    await asyncio.to_thread(build_main)


@app.on_event("startup")
async def startup_event() -> None:
    if ENABLE_BACKGROUND_CRAWL:
        asyncio.create_task(run_daily_crawl_loop())
    else:
        print("Background crawl loop disabled by ENABLE_BACKGROUND_CRAWL.")


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
    raw = re.sub(
        r"(\b[A-Za-z]+)\s+(\d{1,2})\s*[-â€“â€”]\s*\d{1,2},\s+(\d{4})",
        r"\1 \2, \3",
        raw,
    )
    raw = re.sub(
        r"(\b[A-Za-z]+)\s+(\d{1,2})\s*[-â€“â€”]\s*\d{1,2}",
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
        error_text = str(exc)
        if "LanceDB table 'greenbuilder_chunks' not found" in error_text:
            response = ChatResponse(
                answer=(
                    "The chatbot index is still being prepared right now. "
                    "Please try again in a few minutes."
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
                    "event_query": is_future_event_query(req.question),
                    "answer": response.answer,
                    "public_sources": [],
                    "private_archive_used": False,
                }
            )
            return response
        raise HTTPException(status_code=500, detail=f"Search failed: {exc}") from exc

    future_query = is_future_event_query(req.question)
    if future_query:
        chunks = filter_to_future_event_chunks(chunks)
        if not chunks:
            response = ChatResponse(
                answer=(
                    "Iâ€™m not seeing any confirmed future conferences in the current Green Builder Media excerpts. "
                    "The available event-related content appears to be past or undated, so I canâ€™t verify an upcoming conference from the retrieved material."
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

    # Build clean, deduplicated public source list.
    # Blogs are deduplicated by URL.
    # Magazine PDFs are deduplicated by PDF URL so each issue appears only once.
    seen = set()
    sources = []
    for chunk in chunks:
        visibility = chunk.get("visibility", "public")
        if visibility != "public":
            continue

        url = chunk.get("url")
        if not url:
            continue

        if url in seen:
            continue
        seen.add(url)

        is_magazine = str(url).startswith("/magazines/") or "/magazines/" in str(url)

        if is_magazine:
            clean_title = (
                chunk.get("source_name")
                or chunk.get("title")
                or chunk.get("pdf_filename")
                or "Green Builder Magazine Archive"
            )

            page = chunk.get("page")
            if page is not None:
                try:
                    clean_title = f"{clean_title} (PDF, p. {int(page)})"
                except Exception:
                    clean_title = f"{clean_title} (PDF)"
            elif not clean_title.lower().endswith("(pdf)"):
                clean_title = f"{clean_title} (PDF)"

            attribution_label = "Magazine archive"
        else:
            clean_title = chunk.get("title", "Untitled")
            attribution_label = chunk.get("attribution_label")

        sources.append(
            SourceItem(
                title=clean_title,
                url=url,
                published_at=chunk.get("published_at"),
                excerpt=chunk.get("text", "")[:240].strip(),
                score=float(chunk.get("score", 0.0)),
                visibility=visibility,
                attribution_label=attribution_label,
                surface_policy=chunk.get("surface_policy"),
            )
        )

    # Ensure at least one magazine PDF source appears if magazine chunks were used.
    blog_sources = [s for s in sources if not s.url.startswith("/magazines/")]
    pdf_sources = [s for s in sources if s.url.startswith("/magazines/")]

    final_sources = blog_sources[:4]

    if pdf_sources:
        final_sources.append(pdf_sources[0])

    # If no PDF source was used, keep normal top 5 behavior.
    if not pdf_sources:
        final_sources = sources[:5]

    response = ChatResponse(
        answer=answer,
        sources=final_sources[:5],
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


@app.post("/api/admin/rebuild-index")
async def admin_rebuild_index(_: str = Depends(admin_auth)) -> dict:
    global rebuild_task

    if rebuild_task and not rebuild_task.done():
        return {"ok": True, "message": "Index rebuild already running"}

    rebuild_task = asyncio.create_task(run_rebuild_once())
    return {"ok": True, "message": "Index rebuild started"}


@app.get("/api/admin/rebuild-index-status")
def admin_rebuild_index_status(_: str = Depends(admin_auth)) -> dict:
    global rebuild_task

    if rebuild_task is None:
        return {"status": "idle"}

    if rebuild_task.done():
        exc = rebuild_task.exception()
        if exc:
            return {"status": "failed", "error": str(exc)}
        return {"status": "completed"}

    return {"status": "running"}


@app.get("/")
def root() -> Response:
    return Response(
        "Green Builder Media Retrieval Bot is running.",
        media_type="text/plain",
    )


# === Safe Magazine PDF Upload + Controlled Ingest Endpoints ===
from fastapi import UploadFile, File
from fastapi.staticfiles import StaticFiles
import shutil

MAGAZINE_DIR = Path("/data/magazines")
MAGAZINE_DIR.mkdir(parents=True, exist_ok=True)

# Safe upload folders. Uploads land in pdf_inbox only.
# They do NOT automatically ingest or touch the live chatbot index.
PDF_INBOX_DIR = Path("/data/pdf_inbox")
PDF_PROCESSING_DIR = Path("/data/pdf_processing")
PDF_DONE_DIR = Path("/data/pdf_done")
PDF_FAILED_DIR = Path("/data/pdf_failed")

for _folder in [PDF_INBOX_DIR, PDF_PROCESSING_DIR, PDF_DONE_DIR, PDF_FAILED_DIR]:
    _folder.mkdir(parents=True, exist_ok=True)

MAGAZINE_INGEST_STATUS_FILE = Path("/data/magazine_ingest_status.json")
PDF_INGEST_LOCK_FILE = Path("/data/pdf_ingest.lock")


def require_data_disk_space(min_free_gb: float = 1.0) -> None:
    total, used, free = shutil.disk_usage("/data")
    min_free_bytes = int(min_free_gb * 1024 * 1024 * 1024)

    if free < min_free_bytes:
        raise HTTPException(
            status_code=507,
            detail=(
                f"Not enough free space on /data. "
                f"Need at least {min_free_gb} GB free before accepting or ingesting PDF uploads."
            ),
        )


def write_magazine_ingest_status(payload: dict) -> None:
    MAGAZINE_INGEST_STATUS_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {**payload, "updated_at_utc": datetime.utcnow().isoformat()}
    MAGAZINE_INGEST_STATUS_FILE.write_text(json.dumps(payload, indent=2))


def read_magazine_ingest_status() -> dict:
    if not MAGAZINE_INGEST_STATUS_FILE.exists():
        return {
            "status": "idle",
            "message": "Safe upload mode is ON. PDFs are stored in /data/pdf_inbox. Auto-ingest is OFF.",
            "current_file": "",
            "processed": 0,
            "total": 0,
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "recovered": [],
        }
    try:
        return json.loads(MAGAZINE_INGEST_STATUS_FILE.read_text())
    except Exception as exc:
        return {
            "status": "error",
            "message": f"Could not read ingest status: {exc}",
            "current_file": "",
            "processed": 0,
            "total": 0,
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "recovered": [],
        }


def pdf_file_info(path: Path) -> dict[str, Any]:
    try:
        stat = path.stat()
        size_mb = round(stat.st_size / (1024 * 1024), 2)
        modified_at_utc = datetime.utcfromtimestamp(stat.st_mtime).isoformat()
    except Exception:
        size_mb = 0
        modified_at_utc = ""
    return {"name": path.name, "size_mb": size_mb, "modified_at_utc": modified_at_utc}


def list_pdf_folder(folder: Path, pattern: str = "*.pdf") -> list[dict[str, Any]]:
    return [pdf_file_info(path) for path in sorted(folder.glob(pattern))]


def recover_interrupted_processing_files() -> list[dict[str, str]]:
    """Move PDFs left in /data/pdf_processing back to inbox so the next ingest can resume."""
    recovered: list[dict[str, str]] = []
    for path in sorted(PDF_PROCESSING_DIR.glob("*.pdf")):
        target = PDF_INBOX_DIR / path.name
        try:
            if target.exists():
                target = PDF_INBOX_DIR / f"recovered-{int(time.time())}-{path.name}"
            shutil.move(str(path), str(target))
            recovered.append({"file": path.name, "moved_to": str(target)})
        except Exception as exc:
            recovered.append({"file": path.name, "error": str(exc)})
    return recovered


def run_pdf_inbox_ingest(pause_seconds: int = 60) -> None:
    """Process PDFs from /data/pdf_inbox one at a time. Resume-safe: recovers leftovers first."""
    PDF_INGEST_LOCK_FILE.write_text(datetime.utcnow().isoformat())
    recovered = recover_interrupted_processing_files()
    pdfs = sorted(PDF_INBOX_DIR.glob("*.pdf"))
    total = len(pdfs)

    if total == 0:
        write_magazine_ingest_status({
            "status": "idle",
            "message": "No PDFs waiting in /data/pdf_inbox.",
            "current_file": "",
            "processed": 0,
            "total": 0,
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "recovered": recovered,
        })
        PDF_INGEST_LOCK_FILE.unlink(missing_ok=True)
        return

    succeeded: list[dict[str, str]] = []
    skipped: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []

    try:
        write_magazine_ingest_status({
            "status": "running",
            "message": f"Starting controlled ingest for {total} PDF(s) from /data/pdf_inbox.",
            "current_file": "",
            "processed": 0,
            "total": total,
            "succeeded": succeeded,
            "skipped": skipped,
            "failed": failed,
            "recovered": recovered,
        })

        for index, inbox_file in enumerate(pdfs, start=1):
            filename = inbox_file.name
            processing_file = PDF_PROCESSING_DIR / filename
            magazine_file = MAGAZINE_DIR / filename
            failed_file = PDF_FAILED_DIR / filename
            done_marker = PDF_DONE_DIR / f"{filename}.done.txt"

            write_magazine_ingest_status({
                "status": "running",
                "message": f"Ingesting {filename} ({index}/{total})",
                "current_file": filename,
                "processed": index - 1,
                "total": total,
                "succeeded": succeeded,
                "skipped": skipped,
                "failed": failed,
                "recovered": recovered,
            })

            try:
                require_data_disk_space(1.0)
                if inbox_file.exists():
                    shutil.move(str(inbox_file), str(processing_file))

                if magazine_file.exists() and magazine_file.stat().st_size > 0:
                    skipped.append({
                        "file": filename,
                        "reason": "A PDF with this filename already exists in /data/magazines. Skipped to avoid duplicate ingest.",
                    })
                    if processing_file.exists():
                        processing_file.unlink()
                    done_marker.write_text(f"Skipped duplicate on {datetime.utcnow().isoformat()} UTC. Existing file: {magazine_file}\n")
                else:
                    # Existing ingest script expects the PDF in /data/magazines and receives only filename.
                    shutil.move(str(processing_file), str(magazine_file))
                    result = subprocess.run(
                        ["python", "scripts/ingest_one_magazine.py", filename],
                        cwd=Path(__file__).resolve().parents[1],
                        capture_output=True,
                        text=True,
                        timeout=1800,
                    )
                    if result.returncode != 0:
                        failed.append({
                            "file": filename,
                            "returncode": str(result.returncode),
                            "stdout": result.stdout[-2000:],
                            "stderr": result.stderr[-2000:],
                        })
                        if magazine_file.exists():
                            shutil.move(str(magazine_file), str(failed_file))
                    else:
                        succeeded.append({"file": filename, "stored_at": str(magazine_file)})
                        done_marker.write_text(f"Ingested successfully on {datetime.utcnow().isoformat()} UTC. Stored at: {magazine_file}\n")

            except Exception as exc:
                failed.append({"file": filename, "error": str(exc)})
                try:
                    if processing_file.exists():
                        shutil.move(str(processing_file), str(failed_file))
                    elif magazine_file.exists() and filename not in {item.get("file") for item in succeeded}:
                        shutil.move(str(magazine_file), str(failed_file))
                except Exception as move_exc:
                    failed.append({"file": filename, "error": f"Also failed while moving bad PDF to failed folder: {move_exc}"})

            write_magazine_ingest_status({
                "status": "running",
                "message": f"Finished {filename}. Pausing before next PDF." if index < total else f"Finished {filename}.",
                "current_file": filename,
                "processed": index,
                "total": total,
                "succeeded": succeeded,
                "skipped": skipped,
                "failed": failed,
                "recovered": recovered,
            })
            if index < total:
                time.sleep(pause_seconds)

        final_status = "completed" if not failed else "completed_with_errors"
        write_magazine_ingest_status({
            "status": final_status,
            "message": f"Controlled ingest finished. {len(succeeded)} succeeded; {len(skipped)} skipped; {len(failed)} failed.",
            "current_file": "",
            "processed": total,
            "total": total,
            "succeeded": succeeded,
            "skipped": skipped,
            "failed": failed,
            "recovered": recovered,
        })
    finally:
        PDF_INGEST_LOCK_FILE.unlink(missing_ok=True)


def get_indexed_magazine_filenames() -> set[str]:
    """Return PDF filenames that are already referenced by the live LanceDB index."""
    try:
        import lancedb
        from urllib.parse import unquote

        db = lancedb.connect("/data/lancedb")
        table = db.open_table("greenbuilder_chunks")
        df = table.to_pandas()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read LanceDB index: {exc}") from exc

    indexed: set[str] = set()
    if "url" not in df.columns:
        return indexed

    for raw_url in df["url"].dropna().unique():
        url = str(raw_url)
        if "/magazines/" not in url:
            continue
        filename = Path(unquote(url.split("/magazines/", 1)[1])).name
        if filename.lower().endswith(".pdf"):
            indexed.add(filename)
    return indexed


def get_unused_magazine_pdfs() -> list[dict[str, object]]:
    """List PDFs in /data/magazines that are not indexed and not waiting in /data/pdf_inbox."""
    indexed = get_indexed_magazine_filenames()
    inbox = {p.name for p in PDF_INBOX_DIR.glob("*.pdf")}
    unused = []

    for path in sorted(MAGAZINE_DIR.glob("*.pdf")):
        if path.name in indexed:
            continue
        if path.name in inbox:
            continue
        unused.append(pdf_file_info(path))

    return unused


@app.get("/admin/unused-pdf-preview")
def unused_pdf_preview(_: str = Depends(admin_auth)) -> dict:
    unused = get_unused_magazine_pdfs()
    indexed = get_indexed_magazine_filenames()
    on_disk = list(MAGAZINE_DIR.glob("*.pdf"))
    return {
        "ok": True,
        "message": f"Found {len(unused)} unused PDF(s) safe to delete.",
        "unused": unused,
        "indexed_count": len(indexed),
        "magazines_on_disk_count": len(on_disk),
    }


@app.post("/admin/clean-unused-pdfs")
def clean_unused_pdfs(_: str = Depends(admin_auth)) -> dict:
    unused = get_unused_magazine_pdfs()
    deleted = []
    failed = []

    for item in unused:
        name = str(item.get("name", ""))
        path = MAGAZINE_DIR / name
        try:
            if path.exists() and path.is_file():
                path.unlink()
                deleted.append(item)
        except Exception as exc:
            failed.append({"name": name, "error": str(exc)})

    return {
        "ok": len(failed) == 0,
        "message": f"Deleted {len(deleted)} unused PDF(s). {len(failed)} failed.",
        "deleted": deleted,
        "failed": failed,
    }


@app.post("/admin/upload-magazine")
async def upload_magazine(files: List[UploadFile] = File(...)):
    uploaded = []
    skipped = []
    require_data_disk_space(1.0)

    for file in files:
        filename = file.filename or ""
        if not filename.lower().endswith(".pdf"):
            skipped.append(filename)
            continue
        target = PDF_INBOX_DIR / filename
        if target.exists():
            skipped.append(f"{filename} (already in inbox)")
            continue
        with target.open("wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        uploaded.append(filename)

    return {
        "ok": True,
        "message": f"Uploaded {len(uploaded)} PDF(s) safely to /data/pdf_inbox. Auto-ingest is OFF.",
        "files": uploaded,
        "skipped": skipped,
    }


@app.get("/admin/pdf-inbox-status")
def pdf_inbox_status(_: str = Depends(admin_auth)) -> dict:
    total, used, free = shutil.disk_usage("/data")
    return {
        "ok": True,
        "disk": {
            "total_gb": round(total / (1024 ** 3), 2),
            "used_gb": round(used / (1024 ** 3), 2),
            "free_gb": round(free / (1024 ** 3), 2),
        },
        "inbox": list_pdf_folder(PDF_INBOX_DIR),
        "processing": list_pdf_folder(PDF_PROCESSING_DIR),
        "done": [pdf_file_info(path) for path in sorted(PDF_DONE_DIR.glob("*.done.txt"))],
        "failed": list_pdf_folder(PDF_FAILED_DIR),
        "status": read_magazine_ingest_status(),
        "lock_exists": PDF_INGEST_LOCK_FILE.exists(),
    }


@app.post("/admin/ingest-pdf-inbox")
async def ingest_pdf_inbox(background_tasks: BackgroundTasks, _: str = Depends(admin_auth)) -> dict:
    current_status = read_magazine_ingest_status()
    if current_status.get("status") == "running" and PDF_INGEST_LOCK_FILE.exists():
        return {"ok": True, "message": "PDF ingest is already running."}

    # If the prior service died mid-ingest, recover PDFs that were left in processing.
    recovered = recover_interrupted_processing_files()
    pdf_count = len(list(PDF_INBOX_DIR.glob("*.pdf")))

    if pdf_count == 0:
        write_magazine_ingest_status({
            "status": "idle",
            "message": "No PDFs waiting in /data/pdf_inbox.",
            "current_file": "",
            "processed": 0,
            "total": 0,
            "succeeded": [],
            "skipped": [],
            "failed": [],
            "recovered": recovered,
        })
        return {"ok": True, "message": "No PDFs waiting in /data/pdf_inbox."}

    require_data_disk_space(1.0)
    background_tasks.add_task(run_pdf_inbox_ingest, 60)
    write_magazine_ingest_status({
        "status": "running",
        "message": f"Controlled ingest queued for {pdf_count} PDF(s).",
        "current_file": "",
        "processed": 0,
        "total": pdf_count,
        "succeeded": [],
        "skipped": [],
        "failed": [],
        "recovered": recovered,
    })
    return {
        "ok": True,
        "message": f"Controlled ingest started for {pdf_count} PDF(s). Files will process one at a time with a pause between PDFs.",
    }


@app.get("/admin/magazine-ingest-status")
def magazine_ingest_status(_: str = Depends(admin_auth)) -> dict:
    return read_magazine_ingest_status()


# === Serve Magazine PDFs Already Ingested ===
app.mount("/magazines", StaticFiles(directory="/data/magazines"), name="magazines")
