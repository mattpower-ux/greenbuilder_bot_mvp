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

# (snipped unchanged content for brevity in explanation — full file continues below)

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


# ✅ NEW ADMIN REBUILD ENDPOINT
@app.post("/api/admin/rebuild-index")
def admin_rebuild_index(_: str = Depends(admin_auth)) -> dict:
    from app.build_index import main as build_main
    build_main()
    return {"ok": True, "message": "Index rebuild completed"}


@app.get("/")
def root() -> Response:
    return Response(
        "Green Builder Media Retrieval Bot is running.",
        media_type="text/plain",
    )
