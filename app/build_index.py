from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Dict, Iterable, List

import lancedb
import pyarrow as pa
from openai import OpenAI

from app.config import get_settings
from app.governance import apply_governance

TABLE_NAME = "greenbuilder_chunks"

# Safer defaults for OpenAI embedding rate limits
EMBED_BATCH_SIZE = 16
EMBED_MAX_RETRIES = 6
EMBED_BASE_SLEEP_SECONDS = 5.0
EMBED_BETWEEN_BATCH_SLEEP_SECONDS = 1.0


def normalize_whitespace(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text.strip()


def split_paragraphs(text: str) -> List[str]:
    text = normalize_whitespace(text)
    if not text:
        return []
    parts = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    return parts if parts else [text]


def chunk_text(text: str, chunk_size: int = 800, overlap: int = 120) -> List[str]:
    text = normalize_whitespace(text)
    if not text:
        return []

    paragraphs = split_paragraphs(text)
    chunks: List[str] = []
    current = ""

    for para in paragraphs:
        if not current:
            current = para
            continue

        candidate = current + "\n\n" + para
        if len(candidate) <= chunk_size:
            current = candidate
        else:
            chunks.append(current.strip())
            tail = current[-overlap:] if overlap > 0 else ""
            current = (tail + "\n\n" + para).strip()

            while len(current) > chunk_size:
                piece = current[:chunk_size]
                chunks.append(piece.strip())
                current = current[max(0, chunk_size - overlap):].strip()

    if current:
        chunks.append(current.strip())

    cleaned: List[str] = []
    seen = set()
    for chunk in chunks:
        key = chunk.strip()
        if key and key not in seen:
            cleaned.append(key)
            seen.add(key)

    return cleaned


def load_documents(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def batched(items: List[str], batch_size: int = EMBED_BATCH_SIZE) -> Iterable[List[str]]:
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def normalize_doc(doc: Dict) -> Dict:
    doc = apply_governance(doc)
    visibility = doc.get("visibility", "public")
    attribution_label = doc.get("attribution_label")
    if not attribution_label:
        attribution_label = (
            "Green Builder Media's internal editorial archive"
            if visibility == "private"
            else "Green Builder Media"
        )
    return {
        "url": doc.get("url", ""),
        "title": doc.get("title", "Untitled"),
        "published_at": doc.get("published_at"),
        "category": doc.get("category"),
        "text": doc.get("text", ""),
        "visibility": visibility,
        "attribution_label": attribution_label,
        "surface_policy": doc.get(
            "surface_policy",
            "public" if visibility == "public" else "paraphrase",
        ),
        "stale": bool(doc.get("stale", False)),
        "stale_reasons": json.dumps(doc.get("stale_reasons", []), ensure_ascii=False),
        "governance_note": doc.get("governance_note"),
    }


def build_embed_text(title: str, category: str | None, chunk: str, url: str) -> str:
    parts = []
    if title:
        parts.append(f"Title: {title}")
    if category:
        parts.append(f"Category: {category}")
    if url:
        parts.append(f"URL: {url}")
    parts.append(f"Content: {chunk}")
    text = "\n".join(parts).strip()

    # HARD SAFETY CAP to avoid OpenAI token limit errors
    return text[:12000]


def is_rate_limit_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "429" in message
        or "rate limit" in message
        or "rate_limit_exceeded" in message
        or "tokens per min" in message
    )


def embed_batch_with_retry(client: OpenAI, model: str, batch: List[str], batch_num: int, total_batches: int):
    for attempt in range(1, EMBED_MAX_RETRIES + 1):
        try:
            result = client.embeddings.create(
                model=model,
                input=batch,
            )
            print(
                f"Embedded batch {batch_num}/{total_batches} "
                f"(attempt {attempt}, batch size {len(batch)})"
            )
            return [item.embedding for item in result.data]

        except Exception as exc:
            if not is_rate_limit_error(exc):
                raise

            sleep_seconds = EMBED_BASE_SLEEP_SECONDS * attempt
            print(
                f"Rate limited on batch {batch_num}/{total_batches} "
                f"(attempt {attempt}/{EMBED_MAX_RETRIES}). "
                f"Sleeping {sleep_seconds:.1f}s and retrying. Error: {exc}"
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(
        f"Embedding batch {batch_num}/{total_batches} failed after {EMBED_MAX_RETRIES} retries."
    )


def main() -> None:
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key)
    rows: List[Dict] = []

    docs = [normalize_doc(doc) for doc in load_documents(settings.docs_file)]
    print(f"Loaded {len(docs)} documents")

    for doc in docs:
        if doc["surface_policy"] == "blocked":
            continue

        chunks = chunk_text(doc["text"])

        for idx, chunk in enumerate(chunks):
            base_id = doc["url"] or doc["title"].replace(" ", "-").lower()
            embed_text = build_embed_text(
                title=doc["title"],
                category=doc.get("category"),
                chunk=chunk,
                url=doc["url"],
            )

            rows.append(
                {
                    "id": f"{base_id}#chunk-{idx}",
                    "url": doc["url"],
                    "title": doc["title"],
                    "published_at": doc.get("published_at"),
                    "category": doc.get("category"),
                    "text": chunk,
                    "embed_text": embed_text,
                    "chunk_index": idx,
                    "chunk_count": len(chunks),
                    "visibility": doc["visibility"],
                    "attribution_label": doc["attribution_label"],
                    "surface_policy": doc["surface_policy"],
                    "stale": doc["stale"],
                    "stale_reasons": doc["stale_reasons"],
                    "governance_note": doc["governance_note"],
                }
            )

    print(f"Prepared {len(rows)} chunks")

    texts = [row["embed_text"] for row in rows]
    embeddings: List[List[float]] = []

    all_batches = list(batched(texts, batch_size=EMBED_BATCH_SIZE))
    total_batches = len(all_batches)

    for idx, batch in enumerate(all_batches, start=1):
        batch_embeddings = embed_batch_with_retry(
            client=client,
            model=settings.openai_embedding_model,
            batch=batch,
            batch_num=idx,
            total_batches=total_batches,
        )
        embeddings.extend(batch_embeddings)
        print(f"Embedded {len(embeddings)}/{len(texts)} total chunks")

        if idx < total_batches:
            time.sleep(EMBED_BETWEEN_BATCH_SLEEP_SECONDS)

    if not embeddings:
        raise RuntimeError("No embeddings were generated. Index build aborted.")

    if len(embeddings) != len(rows):
        raise RuntimeError(
            f"Embedding count mismatch: got {len(embeddings)} embeddings for {len(rows)} rows."
        )

    for row, emb in zip(rows, embeddings):
        row["vector"] = emb

    db = lancedb.connect(str(settings.lancedb_dir))

    schema = pa.schema(
        [
            pa.field("id", pa.string()),
            pa.field("url", pa.string()),
            pa.field("title", pa.string()),
            pa.field("published_at", pa.string()),
            pa.field("category", pa.string()),
            pa.field("text", pa.string()),
            pa.field("embed_text", pa.string()),
            pa.field("chunk_index", pa.int32()),
            pa.field("chunk_count", pa.int32()),
            pa.field("visibility", pa.string()),
            pa.field("attribution_label", pa.string()),
            pa.field("surface_policy", pa.string()),
            pa.field("stale", pa.bool_()),
            pa.field("stale_reasons", pa.string()),
            pa.field("governance_note", pa.string()),
            pa.field("vector", pa.list_(pa.float32(), len(embeddings[0]))),
        ]
    )

    if TABLE_NAME in db.table_names():
        db.drop_table(TABLE_NAME)

    table = db.create_table(TABLE_NAME, data=rows, schema=schema)

    table.create_scalar_index("url")
    table.create_scalar_index("published_at")
    table.create_scalar_index("visibility")
    table.create_scalar_index("surface_policy")
    table.create_scalar_index("title")
    table.create_scalar_index("category")

    print(f"Built LanceDB table '{TABLE_NAME}' with {len(rows)} rows")


if __name__ == "__main__":
    main()
