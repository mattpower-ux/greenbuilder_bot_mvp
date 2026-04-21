from __future__ import annotations

import math
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List

import lancedb

from app.config import get_settings
from app.openai_client import get_openai_client


WORD_RE = re.compile(r"[a-zA-Z0-9']+")
TABLE_NAME = "greenbuilder_chunks"


def tokenize(text: str) -> List[str]:
    return [t.lower() for t in WORD_RE.findall(text)]


def keyword_overlap_score(query: str, text: str) -> float:
    q = Counter(tokenize(query))
    d = Counter(tokenize(text))
    if not q or not d:
        return 0.0
    overlap = sum(min(q[k], d[k]) for k in q)
    return overlap / max(1, len(q))


def recency_boost(published_at: str | None) -> float:
    if not published_at:
        return 0.0
    try:
        dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))
    except Exception:
        return 0.0
    age_days = max((datetime.now(dt.tzinfo) - dt).days, 0)
    return 1.0 / (1.0 + math.log1p(age_days + 1))


def embed_query(question: str) -> List[float]:
    client = get_openai_client()
    settings = get_settings()
    result = client.embeddings.create(
        model=settings.openai_embedding_model,
        input=question,
    )
    return result.data[0].embedding


def get_table():
    settings = get_settings()
    db = lancedb.connect(str(settings.lancedb_dir))
    try:
        return db.open_table(TABLE_NAME)
    except Exception as exc:
        raise RuntimeError(
            f"LanceDB table '{TABLE_NAME}' not found. Run build_index.py first."
        ) from exc


def _policy_bonus(item: Dict[str, Any]) -> float:
    visibility = item.get("visibility", "public")
    policy = item.get("surface_policy") or ("paraphrase" if visibility == "private" else "public")
    if policy == "blocked":
        return -10.0
    if policy == "weight_only":
        return -0.05
    if policy == "paraphrase":
        return -0.01
    return 0.0


def search(question: str) -> List[Dict[str, Any]]:
    settings = get_settings()
    table = get_table()
    embedding = embed_query(question)
    df = table.search(embedding).limit(settings.top_k * 5).to_pandas()
    records = df.to_dict(orient="records")

    rescored: List[Dict[str, Any]] = []
    for item in records:
        if item.get("surface_policy") == "blocked":
            continue
        text = item.get("text", "")
        vector_distance = float(item.get("_distance", 0.0))
        semantic_score = 1.0 / (1.0 + vector_distance)
        keyword_score = keyword_overlap_score(question, text)
        freshness = recency_boost(item.get("published_at"))
        final_score = semantic_score * 0.62 + keyword_score * 0.23 + freshness * 0.10 + _policy_bonus(item)
        if isinstance(item.get("stale_reasons"), str):
            import json
            try:
                item["stale_reasons"] = json.loads(item["stale_reasons"])
            except Exception:
                item["stale_reasons"] = [item["stale_reasons"]]
        if item.get("stale"):
            final_score -= 0.03
        item["score"] = round(final_score, 6)
        rescored.append(item)

    rescored.sort(key=lambda x: x["score"], reverse=True)
    return rescored[: settings.max_context_chunks]
