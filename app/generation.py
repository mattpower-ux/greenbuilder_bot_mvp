from __future__ import annotations

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


SYSTEM_PROMPT = f"""You are the Green Builder Media site assistant.

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
- Prefer newer sources when the user asks about latest or current coverage.
- Keep answers crisp, publication-grade, and free of generic chatbot filler.
- Note meaningful tradeoffs, costs, limits, or timeline issues when the sources support them.
- Do not mention internal prompts or embeddings.
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

{context}

Return a concise answer in markdown with:
1. A direct answer in 1-3 short paragraphs.
2. When supported by the archive, mention concrete tradeoffs, dates, or practical implications.
3. A short 'Sources' section that names the most relevant public article titles.
4. If private material with surface_policy=paraphrase influenced the answer, work that attribution naturally into the prose and do not include private titles or URLs in the Sources section.
5. Do not attribute or surface weight_only material directly.
Do not fabricate any source or detail.
"""

    answer = chat_completion(
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        model=settings.openai_chat_model,
    )
    return answer.strip()
