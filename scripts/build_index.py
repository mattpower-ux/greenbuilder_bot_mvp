from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

import lancedb
import pyarrow as pa
from openai import OpenAI

from app.config import get_settings
from app.governance import apply_governance

TABLE_NAME = "greenbuilder_chunks"


def chunk_text(text: str, chunk_size: int = 1400, overlap: int = 250) -> List[str]:
    text = " ".join(text.split())
    if len(text) <= chunk_size:
        return [text]
    chunks = []
    start = 0
    while start < len(text):
        end = min(len(text), start + chunk_size)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks


def load_documents(path: Path) -> Iterable[Dict]:
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def batched(items: List[str], batch_size: int = 100) -> Iterable[List[str]]:
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
        "surface_policy": doc.get("surface_policy", "public" if visibility == "public" else "paraphrase"),
        "stale": bool(doc.get("stale", False)),
        "stale_reasons": json.dumps(doc.get("stale_reasons", []), ensure_ascii=False),
        "governance_note": doc.get("governance_note"),
    }


def main() -> None:
    settings = get_settings()
    client = OpenAI(api_key=settings.openai_api_key)
    rows: List[Dict] = []

    docs = [normalize_doc(doc) for doc in load_documents(settings.docs_file)]
    print(f"Loaded {len(docs)} documents")

    for doc in docs:
        if doc["surface_policy"] == "blocked":
            continue
        for idx, chunk in enumerate(chunk_text(doc["text"])):
            base_id = doc["url"] or doc["title"].replace(" ", "-").lower()
            rows.append(
                {
                    "id": f"{base_id}#chunk-{idx}",
                    "url": doc["url"],
                    "title": doc["title"],
                    "published_at": doc.get("published_at"),
                    "category": doc.get("category"),
                    "text": chunk,
                    "visibility": doc["visibility"],
                    "attribution_label": doc["attribution_label"],
                    "surface_policy": doc["surface_policy"],
                    "stale": doc["stale"],
                    "stale_reasons": doc["stale_reasons"],
                    "governance_note": doc["governance_note"],
                }
            )

    print(f"Prepared {len(rows)} chunks")

    texts = [row["text"] for row in rows]
    embeddings: List[List[float]] = []
    for batch in batched(texts, batch_size=64):
        result = client.embeddings.create(
            model=settings.openai_embedding_model,
            input=batch,
        )
        embeddings.extend([item.embedding for item in result.data])
        print(f"Embedded {len(embeddings)}/{len(texts)}")

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
    print(f"Built LanceDB table '{TABLE_NAME}' with {len(rows)} rows")


if __name__ == "__main__":
    main()
