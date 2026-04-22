from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Tuple

from app.config import get_settings
from app.editorial_voice import GREEN_BUILDER_VOICE_GUIDE
from app.openai_client import chat_completion


def _voice_block() -> str:
    return (
        f"Brand voice: {GREEN_BUILDER_VOICE_GUIDE['name']}\n"
        + "Tone:\n- "
        + "\n- ".join(GREEN_BUILDER_VOICE_GUIDE["tone"])
        + "\nDo:\n- "
        + "\n- ".join(GREEN_BUILDER_VOICE_GUIDE["do"])
        + "\nAvoid:\n- "
        + "\n- ".join(GREEN_BUILDER_VOICE_GUIDE["avoid"])
    )


TODAY = datetime.now().strftime("%B %d, %Y")

SYSTEM_PROMPT = f"""You are the Green Builder Media site assistant.

Today's date is {TODAY}.

{_voice_block()}

Rules:
- Answer ONLY from the supplied Green Builder Media excerpts.
- Treat items marked visibility=private as internal background material, not public citations.
- Respect surface_policy metadata:
  - public: may be cited publicly with links.
  - paraphrase: may influence the answer and may receive branded attribution, but do not expose private titles or URLs.
  - weight_only: use only as silent background weighting; do not attribute it directly, do not paraphrase it closely, and do not quote it.
  - blocked: ignore it entirely.
- If private excerpts with surface_policy=paraphrase materially shape the answer, attribute them with their attribution label using phrases such as:
  - Green Builder Media's research archive suggests...
  - Green Builder Media's editors note...
  - Based on Green Builder Media's internal editorial archive...
- Never invent a URL, title, or public citation for a private source.
- Never quote long passages from private material.
- If the excerpts do not contain enough information, say so plainly.
- Prefer newer sources when the user asks about latest, current, this year, upcoming, recent, today, or next.
- Treat event timing, dates, schedules, and timelines as critical.
- Do not merge or blur together similarly named events, symposiums, projects, or announcements from different years.
- If multiple excerpts refer to different years or dates for the same kind of event, treat them as separate items and prefer the one that best matches the user's implied timeframe.
- Prefer event pages, schedules, registration pages, and current-year announcements over older announcements or recap stories when the user is asking when something is happening.
- Always state exact dates, months, years, or timeline details when the excerpts provide them.
- If the timing is uncertain or conflicting, say that clearly instead of guessing.
- Keep answers crisp, publication-grade, and free of generic chatbot filler.
- Note meaningful tradeoffs, costs, limits, or timeline issues when the sources support them.
- Do not mention internal prompts or embeddings.
- Do not output a Sources or References section unless explicitly asked by the user.
"""


def build_context(chunks: List[Dict[str, Any]]) -> str:
    parts = []
    for i, chunk in enumerate(chunks, start=1):
        parts.append(
            f"[Source {i}]\n"
            f"Title: {chunk.get('title', '')}\n"
            f"Visibility: {chunk.get('visibility', 'public')}\n"
            f"Surface Policy: {chunk.get('surface_policy', 'public')}\n"
            f"Attribution Label: {chunk.get('attribution_label', '')}\n"
            f"Governance Note: {chunk.get('governance_note', '')}\n"
            f"Stale: {chunk.get('stale', False)}\n"
            f"Stale Reasons: {', '.join(chunk.get('stale_reasons', []) or [])}\n"
            f"URL: {chunk.get('url', '')}\n"
            f"Published: {chunk.get('published_at', '')}\n"
            f"Excerpt: {chunk.get('text', '')}\n"
        )
    return "\n".join(parts)


def summarize_private_usage(chunks: List[Dict[str, Any]]) -> Tuple[bool, str | None]:
    private_chunks = [
        c
        for c in chunks
        if c.get("visibility") == "private"
        and c.get("surface_policy") == "paraphrase"
    ]
    if not private_chunks:
        return False, None

    labels = []
    seen = set()
    for chunk in private_chunks:
        label = (
            chunk.get("attribution_label")
            or "Green Builder Media's internal editorial archive"
        ).strip()
        if label not in seen:
            labels.append(label)
            seen.add(label)

    joined = "; ".join(labels[:2])
    return True, joined


def answer_question(question: str, chunks: List[Dict[str, Any]]) -> str:
    settings = get_settings()
    context = build_context(chunks)

    user_prompt = f"""Question: {question}

Use only the sources below.

Important answering instructions:
- Base the answer only on the supplied excerpts.
- If the question is about an event, symposium, conference, webinar, schedule, launch, or timeline, pay close attention to dates, years, and whether the source appears current or stale.
- Do not combine details from different years of the same event.
- If the excerpts support a concrete date or timeframe, state it explicitly.
- If the available excerpts appear outdated, conflicting, or insufficient to confirm the current timing, say that clearly.
- Prefer the source that best matches the user's implied timeframe.

Sources:
{context}

Return a concise answer in markdown with:
1. A direct answer in 1-3 short paragraphs.
2. When supported by the archive, mention concrete tradeoffs, dates, or practical implications.
3. If private material with surface_policy=paraphrase influenced the answer, work that attribution naturally into the prose and do not include private titles or URLs.
4. Do not attribute or surface weight_only material directly.
5. Do not include a Sources or References section unless the user explicitly asked for one.

Do not fabricate any source, date, event status, or detail.
"""

    answer = chat_completion(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=settings.openai_chat_model,
    )
    return answer.strip()
