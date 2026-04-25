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

EVENT_TERMS = {
    "conference",
    "conferences",
    "event",
    "events",
    "summit",
    "summits",
    "symposium",
    "symposiums",
    "expo",
    "webinar",
    "webinars",
    "calendar",
    "schedule",
    "upcoming",
    "coming",
    "save",
    "date",
}

HIGH_VALUE_EVENT_PHRASES = [
    "sustainability symposium",
    "save the date",
    "next generation water summit",
    "conference & expo",
    "conference and expo",
]

FRANCHISE_TERMS = [
    "brand index",
    "sustainable brand index",
    "sustainable products",
    "green home of the year",
    "vision house",
    "symposium",
]


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
    policy = item.get("surface_policy") or (
        "paraphrase" if visibility == "private" else "public"
    )
    if policy == "blocked":
        return -10.0
    if policy == "weight_only":
        return -0.05
    if policy == "paraphrase":
        return -0.01
    return 0.0


def extract_year(text: str) -> int | None:
    match = re.search(r"\b(20\d{2})\b", text or "")
    if match:
        return int(match.group(1))
    return None


def is_event_query(question: str) -> bool:
    q_tokens = set(tokenize(question))
    if q_tokens & EVENT_TERMS:
        return True

    q = (question or "").lower()
    return any(phrase in q for phrase in HIGH_VALUE_EVENT_PHRASES)


def is_archive_or_date_query(question: str) -> bool:
    q = (question or "").lower()
    archive_terms = ["magazine","pdf","issue","archive","older","past","history","historical","previously","back issue"]
    if any(t in q for t in archive_terms):
        return True
    if re.search(r"\b20\d{2}\b", q):
        return True
    return False

def is_magazine_item(item: Dict[str, Any]) -> bool:
    url = str(item.get("url", "") or "")
    return url.startswith("/magazines/") or "/magazines/" in url


def source_type_bonus(question: str, item: Dict[str, Any]) -> float:
    if not is_magazine_item(item):
        return 0.0

    # Small boost so magazine archive chunks can surface, but blogs still dominate.
    bonus = 0.025

    # Extra lift for year/date/archive-style questions.
    if is_archive_or_date_query(question):
        bonus += 0.055

    return bonus

def combined_search_text(item: Dict[str, Any]) -> str:
    title = item.get("title", "") or ""
    text = item.get("text", "") or ""
    url = item.get("url", "") or ""
    attribution = item.get("attribution_label", "") or ""

    return "\n".join(
        part for part in [title, text, url, attribution] if part
    )


def title_match_bonus(question: str, item: Dict[str, Any]) -> float:
    q = (question or "").lower()
    title = (item.get("title", "") or "").lower()

    if not title:
        return 0.0

    bonus = 0.0

    for phrase in HIGH_VALUE_EVENT_PHRASES:
        if phrase in q and phrase in title:
            bonus += 0.22

    title_overlap = keyword_overlap_score(question, title)
    bonus += title_overlap * 0.18

    return bonus


def event_bonus(question: str, item: Dict[str, Any]) -> float:
    if not is_event_query(question):
        return 0.0

    haystack = combined_search_text(item).lower()
    bonus = 0.0

    if "conference" in haystack:
        bonus += 0.04
    if "summit" in haystack:
        bonus += 0.04
    if "symposium" in haystack:
        bonus += 0.08
    if "expo" in haystack:
        bonus += 0.03
    if "webinar" in haystack:
        bonus += 0.03
    if "save the date" in haystack:
        bonus += 0.08
    if "schedule" in haystack or "calendar" in haystack:
        bonus += 0.03

    for phrase in HIGH_VALUE_EVENT_PHRASES:
        if phrase in haystack:
            bonus += 0.12

    q = (question or "").lower()
    if "sustainability symposium" in q and "sustainability symposium" in haystack:
        bonus += 0.35

    return bonus


def franchise_bonus(question: str, item: Dict[str, Any]) -> float:
    q = (question or "").lower()
    title = (item.get("title", "") or "").lower()
    url = (item.get("url", "") or "").lower()
    text = (item.get("text", "") or "").lower()

    year = extract_year(q)
    bonus = 0.0

    matched_franchise = None
    for phrase in FRANCHISE_TERMS:
        if phrase in q:
            matched_franchise = phrase
            break

    if not matched_franchise:
        return 0.0

    # Franchise phrase matches
    if matched_franchise in title:
        bonus += 0.25
    if matched_franchise in url:
        bonus += 0.15
    if matched_franchise in text:
        bonus += 0.08

    # Exact year matches
    if year:
        year_str = str(year)
        if year_str in title:
            bonus += 0.35
        if year_str in url:
            bonus += 0.25
        if year_str in text:
            bonus += 0.10

    # Strong combo boost: year + franchise together in title
    if year and matched_franchise in title and str(year) in title:
        bonus += 0.45

    # Additional URL combo boost
    if year and matched_franchise in url and str(year) in url:
        bonus += 0.20

    return bonus


def search(question: str) -> List[Dict[str, Any]]:
    settings = get_settings()
    table = get_table()
    embedding = embed_query(question)

    candidate_limit = max(settings.top_k * 8, settings.max_context_chunks * 8)

    df = table.search(embedding).limit(candidate_limit).to_pandas()
    records = df.to_dict(orient="records")

    rescored: List[Dict[str, Any]] = []
    for item in records:
        if item.get("surface_policy") == "blocked":
            continue

        search_text = combined_search_text(item)
        vector_distance = float(item.get("_distance", 0.0))
        semantic_score = 1.0 / (1.0 + vector_distance)
        keyword_score = keyword_overlap_score(question, search_text)
        freshness = recency_boost(item.get("published_at"))
        title_bonus = title_match_bonus(question, item)
        topical_event_bonus = event_bonus(question, item)
        franchise_match = franchise_bonus(question, item)
        magazine_lift = source_type_bonus(question, item)

        final_score = (
            semantic_score * 0.50
            + keyword_score * 0.22
            + freshness * 0.08
            + title_bonus
            + topical_event_bonus
            + franchise_match
            + magazine_lift
            + _policy_bonus(item)
        )

        if isinstance(item.get("stale_reasons"), str):
            import json
            try:
                item["stale_reasons"] = json.loads(item["stale_reasons"])
            except Exception:
                item["stale_reasons"] = [item["stale_reasons"]]

        if item.get("stale"):
            final_score -= 0.02

        item["score"] = round(final_score, 6)
        rescored.append(item)

    rescored.sort(key=lambda x: x["score"], reverse=True)

    max_chunks = settings.max_context_chunks
    archive_query = is_archive_or_date_query(question)

    blogs = [r for r in rescored if not is_magazine_item(r)]
    magazines = [r for r in rescored if is_magazine_item(r)]

    if archive_query:
        # Archive/date questions: allow more magazine context.
        magazine_target = max(1, min(3, max_chunks // 2))
    else:
        # Normal questions: allow a small amount of archive support.
        magazine_target = max(1, min(2, max_chunks // 4))

    blog_target = max_chunks - magazine_target
    final_results = blogs[:blog_target] + magazines[:magazine_target]

    # Fill remaining slots from the overall ranking, avoiding duplicate chunks.
    seen_ids = {r.get("id") for r in final_results}
    for r in rescored:
        rid = r.get("id")
        if rid in seen_ids:
            continue
        final_results.append(r)
        seen_ids.add(rid)
        if len(final_results) >= max_chunks:
            break

    final_results.sort(key=lambda x: x["score"], reverse=True)
    return final_results[:max_chunks]
