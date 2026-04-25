from __future__ import annotations

import hashlib
import os
import re
from datetime import datetime
from pathlib import Path

import lancedb
from openai import OpenAI
from pypdf import PdfReader

# Render disk paths
MAGAZINE_DIR = Path(os.getenv("MAGAZINE_DIR", "/data/magazines"))
LANCEDB_DIR = os.getenv("LANCEDB_DIR", "/data/lancedb")

# Match your existing retrieval table unless your project uses a different table name.
TABLE_NAME = os.getenv("LANCEDB_TABLE", "greenbuilder_chunks")

# Public URL path served by main.py:
# app.mount("/magazines", StaticFiles(directory="/data/magazines"), name="magazines")
PUBLIC_MAGAZINE_PREFIX = os.getenv("PUBLIC_MAGAZINE_PREFIX", "/magazines")

EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")

client = OpenAI()


def clean_text(text: str) -> str:
    text = text or ""
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200) -> list[str]:
    text = clean_text(text)
    if not text:
        return []

    chunks: list[str] = []
    start = 0

    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end].strip()

        if chunk:
            chunks.append(chunk)

        if end >= len(text):
            break

        start = max(0, end - overlap)

    return chunks


def embed_text(text: str) -> list[float]:
    response = client.embeddings.create(
        model=EMBED_MODEL,
        input=text,
    )
    return response.data[0].embedding


def safe_title_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    stem = stem.replace("_", " ").replace("-", " ")
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or filename


def row_id(pdf_filename: str, page_num: int, chunk_index: int, text: str) -> str:
    raw = f"{pdf_filename}|{page_num}|{chunk_index}|{text[:80]}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def ensure_table(db):
    table_names = db.table_names()

    if TABLE_NAME in table_names:
        return db.open_table(TABLE_NAME)

    # Create a compatible first row so LanceDB has a schema.
    placeholder_text = "placeholder"
    return db.create_table(
        TABLE_NAME,
        data=[
            {
                "id": "placeholder",
                "title": "placeholder",
                "url": "/magazines/placeholder.pdf",
                "text": placeholder_text,
                "source_type": "magazine",
                "source_name": "placeholder",
                "pdf_filename": "placeholder.pdf",
                "page": 0,
                "published_at": "",
                "visibility": "public",
                "attribution_label": "Magazine archive",
                "surface_policy": "public",
                "vector": embed_text(placeholder_text),
            }
        ],
        mode="overwrite",
    )


def ingest_magazines() -> dict:
    MAGAZINE_DIR.mkdir(parents=True, exist_ok=True)
    Path(LANCEDB_DIR).mkdir(parents=True, exist_ok=True)

    db = lancedb.connect(LANCEDB_DIR)
    table = ensure_table(db)

    rows = []
    pdfs = sorted(MAGAZINE_DIR.glob("*.pdf"))

    if not pdfs:
        return {
            "ok": True,
            "message": f"No PDF files found in {MAGAZINE_DIR}",
            "pdfs_processed": 0,
            "chunks_added": 0,
        }

    for pdf_path in pdfs:
        print(f"Reading {pdf_path.name}")

        try:
            reader = PdfReader(str(pdf_path))
        except Exception as exc:
            print(f"Skipping {pdf_path.name}: could not read PDF: {exc}")
            continue

        source_title = safe_title_from_filename(pdf_path.name)
        pdf_url = f"{PUBLIC_MAGAZINE_PREFIX}/{pdf_path.name}"

        for page_num, page in enumerate(reader.pages, start=1):
            try:
                page_text = clean_text(page.extract_text() or "")
            except Exception as exc:
                print(f"Skipping {pdf_path.name} page {page_num}: {exc}")
                continue

            for chunk_index, chunk in enumerate(chunk_text(page_text), start=1):
                rid = row_id(pdf_path.name, page_num, chunk_index, chunk)

                rows.append(
                    {
                        "id": rid,
                        "title": source_title,
                        "url": pdf_url,
                        "text": chunk,
                        "source_type": "magazine",
                        "source_name": source_title,
                        "pdf_filename": pdf_path.name,
                        "page": page_num,
                        "published_at": "",
                        "visibility": "public",
                        "attribution_label": f"Magazine archive, p. {page_num}",
                        "surface_policy": "public",
                        "vector": embed_text(chunk),
                    }
                )

    if rows:
        # This appends magazine rows. If you re-run repeatedly, duplicate IDs may still be added
        # depending on LanceDB version/schema behavior. For a cleaner rebuild later, add delete-by-source logic.
        table.add(rows)

    return {
        "ok": True,
        "message": f"Magazine ingest complete. Added {len(rows)} chunks.",
        "pdfs_processed": len(pdfs),
        "chunks_added": len(rows),
        "table": TABLE_NAME,
        "magazine_dir": str(MAGAZINE_DIR),
        "lancedb_dir": LANCEDB_DIR,
    }


if __name__ == "__main__":
    result = ingest_magazines()
    print(result)
