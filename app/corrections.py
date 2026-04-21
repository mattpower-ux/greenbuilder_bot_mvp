from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.config import get_settings


def normalize_question(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def _ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, payload: Any) -> None:
    _ensure_parent(path)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_corrections() -> List[Dict[str, Any]]:
    settings = get_settings()
    return _load_json(settings.corrections_file, [])


def save_correction(correction: Dict[str, Any]) -> Dict[str, Any]:
    settings = get_settings()
    corrections = load_corrections()
    correction = dict(correction)
    correction.setdefault("id", hashlib.sha1((correction.get("question_pattern", "") + correction.get("answer_override", "")).encode("utf-8")).hexdigest()[:12])
    correction.setdefault("created_at", _now())
    correction.setdefault("is_active", True)
    corrections = [c for c in corrections if c.get("id") != correction["id"]]
    corrections.insert(0, correction)
    _save_json(settings.corrections_file, corrections)
    return correction


def find_correction(question: str) -> Optional[Dict[str, Any]]:
    q_norm = normalize_question(question)
    for correction in load_corrections():
        if not correction.get("is_active", True):
            continue
        pattern = correction.get("question_pattern", "")
        match_type = correction.get("match_type", "exact")
        if match_type == "exact" and q_norm == normalize_question(pattern):
            return correction
        if match_type == "contains" and normalize_question(pattern) in q_norm:
            return correction
        if match_type == "regex":
            try:
                if re.search(pattern, question, re.I):
                    return correction
            except re.error:
                continue
    return None


def load_logs(limit: int = 50) -> List[Dict[str, Any]]:
    settings = get_settings()
    logs = _load_json(settings.qa_log_file, [])
    return logs[:limit]


def append_log(entry: Dict[str, Any]) -> Dict[str, Any]:
    settings = get_settings()
    logs = _load_json(settings.qa_log_file, [])
    entry = dict(entry)
    entry.setdefault("id", hashlib.sha1((entry.get("question", "") + entry.get("answer", "") + _now()).encode("utf-8")).hexdigest()[:12])
    entry.setdefault("created_at", _now())
    logs.insert(0, entry)
    _save_json(settings.qa_log_file, logs[: settings.max_logged_interactions])
    return entry
