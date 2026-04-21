from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
app = FastAPI(title="Green Builder Media Retrieval Bot", version="0.3.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://www.greenbuildermedia.com",
        "https://greenbuildermedia.com"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
security = HTTPBasic()
import secrets
from pathlib import Path
from typing import Optional

from fastapi import Depends, FastAPI, HTTPException, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from app.admin_ui import HTML as ADMIN_HTML
from app.config import get_settings
from app.corrections import append_log, find_correction, load_corrections, load_logs, save_correction
from app.generation import answer_question, summarize_private_usage
from app.models import ChatRequest, ChatResponse, CorrectionCreate, CorrectionListResponse, LogListResponse, SourceItem
from app.retrieval import search

settings = get_settings()

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def admin_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    expected_username = settings.admin_username.encode("utf-8")
    expected_password = settings.admin_password.encode("utf-8")
    given_username = credentials.username.encode("utf-8")
    given_password = credentials.password.encode("utf-8")
    if not (secrets.compare_digest(given_username, expected_username) and secrets.compare_digest(given_password, expected_password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid admin credentials",
            headers={"WWW-Authenticate": "Basic"},
        )
    return credentials.username


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
            correction_note=correction.get("editor_note") or f"Editor override by {correction.get('editor_name') or 'editor'}",
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
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    if not chunks:
        response = ChatResponse(
            answer="I couldn't find relevant Green Builder Media content for that question.",
            sources=[],
        )
        append_log({"question": req.question, "answer": response.answer, "public_sources": [], "private_archive_used": False})
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
def admin_create_correction(payload: CorrectionCreate, username: str = Depends(admin_auth)) -> dict:
    saved = save_correction({**payload.model_dump(), "editor_name": payload.editor_name or username})
    return {"ok": True, "message": "Correction saved", "correction": saved}


@app.get("/")
def root() -> Response:
    return Response("Green Builder Media Retrieval Bot is running.", media_type="text/plain")
