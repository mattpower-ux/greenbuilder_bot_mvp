from __future__ import annotations

import hashlib
import os
import re
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
    text = text or ""
    text = re.sub(r"\s+", " ", text)
    return text.strip()


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
    stem = re.sub(r"\s+", " ", stem).strip()
    return stem or filename


def row_id(pdf_filename: str, page_num: int, chunk_index: int, text: str) -> str:
    raw = f"{pdf_filename}|{page_num}|{chunk_index}|{text[:80]}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def embed_batch(texts: list[str]) -> list[list[float]]:
    response = client.embeddings.create(
        model=EMBED_MODEL,
        input=texts,
    )
    return [item.embedding for item in response.data]


def get_existing_magazine_urls(table) -> set[str]:
    try:
        df = table.to_pandas()
        if "url" not in df.columns:
            return set()

        urls = df["url"].astype(str)
        return set(urls[urls.str.contains("/magazines/", na=False)].unique())
    except Exception as exc:
        log(f"Warning: could not inspect existing URLs: {exc}")
        return set()


def flush_rows(table, rows: list[dict]) -> int:
    if not rows:
        return 0

    table.add(rows)
    count = len(rows)
    rows.clear()
    return count


def ingest_pdf(table, pdf_path: Path) -> int:
    pdf_url = f"{PUBLIC_MAGAZINE_PREFIX}/{pdf_path.name}"
    source_title = safe_title_from_filename(pdf_path.name)

    try:
        reader = PdfReader(str(pdf_path))
    except Exception as exc:
        log(f"ERROR opening {pdf_path.name}: {exc}")
        return 0

    total_pages = len(reader.pages)
    log(f"Processing {pdf_path.name} ({total_pages} pages)")

    pending_rows: list[dict] = []
    pending_texts: list[tuple[int, int, str]] = []
    chunks_added = 0

    def flush_pending() -> int:
        nonlocal pending_rows, pending_texts

        if not pending_texts:
            return 0

        texts = [x[2] for x in pending_texts]

        try:
            vectors = embed_batch(texts)
        except Exception as exc:
            log(f"Embedding batch failed for {pdf_path.name}: {exc}")
            time.sleep(3)
            return 0

        for (page_num, chunk_index, text), vector in zip(pending_texts, vectors):
            pending_rows.append(
                {
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
                }
            )

        pending_texts.clear()

        written = flush_rows(table, pending_rows)
        return written

    for page_num, page in enumerate(reader.pages, start=1):
        try:
            page_text = clean_text(page.extract_text() or "")
        except Exception as exc:
            log(f"Skipping {pdf_path.name} page {page_num}: {exc}")
            continue

        chunks = chunk_text(page_text)

        if chunks:
            log(f"  Page {page_num}/{total_pages}: {len(chunks)} chunks")
        else:
            log(f"  Page {page_num}/{total_pages}: no text")

        for chunk_index, chunk in enumerate(chunks, start=1):
            pending_texts.append((page_num, chunk_index, chunk))

            if len(pending_texts) >= BATCH_SIZE:
                written = flush_pending()
                chunks_added += written
                log(f"    Wrote {written} chunks; total for issue: {chunks_added}")

    written = flush_pending()
    chunks_added += written

    log(f"DONE {pdf_path.name}: added {chunks_added} chunks")
    return chunks_added


def ingest_magazines() -> dict:
    MAGAZINE_DIR.mkdir(parents=True, exist_ok=True)
    Path(LANCEDB_DIR).mkdir(parents=True, exist_ok=True)

    db = lancedb.connect(LANCEDB_DIR)
    table = db.open_table(TABLE_NAME)

    existing_urls = get_existing_magazine_urls(table)
    pdfs = sorted(MAGAZINE_DIR.glob("*.pdf"))

    log(f"Found {len(pdfs)} PDF(s) in {MAGAZINE_DIR}")
    log(f"Already ingested magazine URL count: {len(existing_urls)}")

    total_chunks = 0
    processed = 0
    skipped = 0
    failed = 0

    for pdf_path in pdfs:
        pdf_url = f"{PUBLIC_MAGAZINE_PREFIX}/{pdf_path.name}"

        if pdf_url in existing_urls:
            log(f"SKIP already ingested: {pdf_path.name}")
            skipped += 1
            continue

        try:
            added = ingest_pdf(table, pdf_path)
            total_chunks += added
            processed += 1
            if added > 0:
                existing_urls.add(pdf_url)
        except Exception as exc:
            failed += 1
            log(f"FAILED {pdf_path.name}: {exc}")
            continue

    result = {
        "ok": True,
        "pdfs_found": len(pdfs),
        "pdfs_processed": processed,
        "pdfs_skipped": skipped,
        "pdfs_failed": failed,
        "chunks_added": total_chunks,
        "table": TABLE_NAME,
        "magazine_dir": str(MAGAZINE_DIR),
        "lancedb_dir": LANCEDB_DIR,
    }

    log(str(result))
    return result


if __name__ == "__main__":
    ingest_magazines()
