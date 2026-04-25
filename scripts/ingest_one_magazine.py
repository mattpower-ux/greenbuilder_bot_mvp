from __future__ import annotations

import hashlib
import os
import re
import sys
import time
from pathlib import Path

import lancedb
from openai import OpenAI
from pypdf import PdfReader

MAGAZINE_DIR = Path(os.getenv("MAGAZINE_DIR", "/data/magazines"))
LANCEDB_DIR = os.getenv("LANCEDB_DIR", "/data/lancedb")
TABLE_NAME = os.getenv("LANCEDB_TABLE", "greenbuilder_chunks")
PUBLIC_MAGAZINE_PREFIX = os.getenv("PUBLIC_MAGAZINE_PREFIX", "/magazines")
EMBED_MODEL = os.getenv("EMBED_MODEL", "text-embedding-3-small")
BATCH_SIZE = int(os.getenv("MAGAZINE_INGEST_BATCH_SIZE", "20"))

client = OpenAI()


def log(message: str) -> None:
    print(message, flush=True)


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def chunk_text(text: str, chunk_size: int = 1200, overlap: int = 200) -> list[str]:
    text = clean_text(text)
    if not text:
        return []

    chunks = []
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


def safe_title_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    stem = stem.replace("_", " ").replace("-", " ")
    return re.sub(r"\s+", " ", stem).strip() or filename


def row_id(pdf_filename: str, page_num: int, chunk_index: int, text: str) -> str:
    raw = f"{pdf_filename}|{page_num}|{chunk_index}|{text[:80]}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def embed_batch(texts: list[str]) -> list[list[float]]:
    response = client.embeddings.create(model=EMBED_MODEL, input=texts)
    return [item.embedding for item in response.data]


def url_already_ingested(table, pdf_url: str) -> bool:
    try:
        df = table.to_pandas()
        if "url" not in df.columns:
            return False
        return pdf_url in set(df["url"].astype(str).unique())
    except Exception as exc:
        log(f"Warning: could not check existing URLs: {exc}")
        return False


def flush_rows(table, rows: list[dict]) -> int:
    if not rows:
        return 0

    table.add(rows)
    count = len(rows)
    rows.clear()
    return count


def ingest_one(filename: str) -> int:
    pdf_path = MAGAZINE_DIR / filename
    if not pdf_path.exists():
        raise FileNotFoundError(f"PDF not found: {pdf_path}")

    db = lancedb.connect(LANCEDB_DIR)
    table = db.open_table(TABLE_NAME)

    pdf_url = f"{PUBLIC_MAGAZINE_PREFIX}/{pdf_path.name}"

    if url_already_ingested(table, pdf_url):
        log(f"SKIP already ingested: {pdf_path.name}")
        return 0

    source_title = safe_title_from_filename(pdf_path.name)
    reader = PdfReader(str(pdf_path))
    total_pages = len(reader.pages)

    log(f"Processing {pdf_path.name} ({total_pages} pages)")

    pending_texts: list[tuple[int, int, str]] = []
    pending_rows: list[dict] = []
    chunks_added = 0

    def flush_pending() -> int:
        nonlocal pending_texts, pending_rows

        if not pending_texts:
            return 0

        texts = [x[2] for x in pending_texts]
        vectors = embed_batch(texts)

        for (page_num, chunk_index, text), vector in zip(pending_texts, vectors):
            pending_rows.append({
                "id": row_id(pdf_path.name, page_num, chunk_index, text),
                "title": source_title,
                "url": pdf_url,
                "text": text,
                "page": page_num,
                "published_at": "",
                "visibility": "public",
                "attribution_label": f"Magazine archive, p. {page_num}",
                "surface_policy": "public",
                "vector": vector,
            })

        pending_texts.clear()
        return flush_rows(table, pending_rows)

    for page_num, page in enumerate(reader.pages, start=1):
        try:
            page_text = clean_text(page.extract_text() or "")
        except Exception as exc:
            log(f"Skipping page {page_num}: {exc}")
            continue

        chunks = chunk_text(page_text)
        log(f"  Page {page_num}/{total_pages}: {len(chunks)} chunks")

        for chunk_index, chunk in enumerate(chunks, start=1):
            pending_texts.append((page_num, chunk_index, chunk))

            if len(pending_texts) >= BATCH_SIZE:
                written = flush_pending()
                chunks_added += written
                log(f"    Wrote {written} chunks; total: {chunks_added}")
                time.sleep(1)

    written = flush_pending()
    chunks_added += written

    log(f"DONE {pdf_path.name}: added {chunks_added} chunks")
    return chunks_added


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise SystemExit("Usage: python scripts/ingest_one_magazine.py 'filename.pdf'")

    ingest_one(sys.argv[1])
