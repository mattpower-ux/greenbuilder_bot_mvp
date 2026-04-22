from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel, Field


class ChatRequest(BaseModel):
    question: str = Field(..., min_length=3, max_length=4000)
    session_id: Optional[str] = None
    page_url: Optional[str] = Field(default=None, max_length=3000)
    referrer: Optional[str] = Field(default=None, max_length=3000)
    user_agent: Optional[str] = Field(default=None, max_length=2000)


class SourceItem(BaseModel):
    title: str
    url: Optional[str] = None
    published_at: Optional[str] = None
    excerpt: str
    score: float
    visibility: str = "public"
    attribution_label: Optional[str] = None
    surface_policy: Optional[str] = None


class ChatResponse(BaseModel):
    answer: str
    sources: List[SourceItem]
    private_archive_used: bool = False
    attribution_note: Optional[str] = None
    corrected_by_editor: bool = False
    correction_note: Optional[str] = None


class CorrectionCreate(BaseModel):
    question_pattern: str = Field(..., min_length=3, max_length=500)
    match_type: str = Field(default="exact", pattern="^(exact|contains|regex)$")
    answer_override: str = Field(..., min_length=3, max_length=12000)
    editor_name: Optional[str] = Field(default=None, max_length=100)
    editor_note: Optional[str] = Field(default=None, max_length=500)


class CorrectionListResponse(BaseModel):
    corrections: List[dict]


class LogListResponse(BaseModel):
    logs: List[dict]
