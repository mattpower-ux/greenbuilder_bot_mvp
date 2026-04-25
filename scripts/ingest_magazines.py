# scripts/ingest_magazines.py

import os
import time
from pathlib import Path
import lancedb
from pypdf import PdfReader
from openai import OpenAI

MAGAZINE_DIR = Path("/data/magazines")
DB_DIR = "/data/lancedb"
TABLE_NAME = "greenbuilder_chunks"

client = OpenAI()


def get_existing_urls(table):
    try:
        df = table.to_pandas()
        return set(df["url"].dropna().unique())
    except:
        return set()


def embed(text):
    return client.embeddings.create(
        model="text-embedding-3-small",
        input=text
    ).data[0].embedding


def chunk(text, size=1200):
    return [text[i:i+size] for i in range(0, len(text), size) if text[i:i+size].strip()]


def ingest():
    db = lancedb.connect(DB_DIR)
    table = db.open_table(TABLE_NAME)

    existing = get_existing_urls(table)

    pdfs = sorted(MAGAZINE_DIR.glob("*.pdf"))

    print(f"Found {len(pdfs)} PDFs")

    for pdf_path in pdfs:
        url = f"/magazines/{pdf_path.name}"

        if url in existing:
            print(f"SKIP (already ingested): {pdf_path.name}")
            continue

        print(f"\n=== PROCESSING: {pdf_path.name} ===")

        try:
            reader = PdfReader(str(pdf_path))
        except Exception as e:
            print(f"ERROR opening PDF: {e}")
            continue

        rows = []

        for i, page in enumerate(reader.pages, start=1):
            try:
                text = page.extract_text() or ""
            except Exception as e:
                print(f"Page {i} error: {e}")
                continue

            chunks = chunk(text)

            for j, c in enumerate(chunks):
                print(f"Embedding page {i}, chunk {j+1}")

                try:
                    vec = embed(c)
                except Exception as e:
                    print(f"Embedding error: {e}")
                    time.sleep(2)
                    continue

                rows.append({
                    "id": f"{pdf_path.name}-{i}-{j}",
                    "title": pdf_path.stem,
                    "url": url,
                    "text": c,
                    "page": i,
                    "vector": vec,
                    "visibility": "public"
                })

        if rows:
            table.add(rows)
            print(f"Added {len(rows)} chunks for {pdf_path.name}")

        print(f"=== DONE: {pdf_path.name} ===\n")

    print("\nINGEST COMPLETE")


if __name__ == "__main__":
    ingest()
