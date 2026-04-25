from __future__ import annotations

import os
from pathlib import Path

import lancedb
from pypdf import PdfReader

MAGAZINE_DIR = Path(os.getenv("MAGAZINE_DIR", "/data/magazines"))
LANCEDB_DIR = os.getenv("LANCEDB_DIR", "/data/lancedb")
TABLE_NAME = os.getenv("LANCEDB_TABLE", "greenbuilder_chunks")
PUBLIC_MAGAZINE_PREFIX = os.getenv("PUBLIC_MAGAZINE_PREFIX", "/magazines")

# Tune these if needed.
MIN_CHUNKS_PER_PDF = int(os.getenv("VERIFY_MIN_CHUNKS_PER_PDF", "25"))
MIN_PAGE_COVERAGE = float(os.getenv("VERIFY_MIN_PAGE_COVERAGE", "0.50"))


def get_pdf_page_count(pdf_path: Path) -> int | None:
    try:
        reader = PdfReader(str(pdf_path))
        return len(reader.pages)
    except Exception as exc:
        print(f"ERROR reading PDF page count: {pdf_path.name}: {exc}")
        return None


def main() -> None:
    pdfs = sorted(MAGAZINE_DIR.glob("*.pdf"))

    print("\n=== Uploaded PDFs ===")
    print(f"PDF folder: {MAGAZINE_DIR}")
    print(f"Uploaded PDF count: {len(pdfs)}")

    db = lancedb.connect(LANCEDB_DIR)
    table = db.open_table(TABLE_NAME)
    df = table.to_pandas()

    if "url" not in df.columns:
        print("\nERROR: LanceDB table has no 'url' column.")
        return

    mag_df = df[df["url"].astype(str).str.contains("/magazines/", na=False)].copy()

    print("\n=== Ingested Magazine Data ===")
    print(f"Magazine chunks in database: {len(mag_df)}")
    print(f"Unique magazine URLs in database: {mag_df['url'].nunique()}")

    if len(pdfs) == 0:
        print("\nNo PDFs found in upload folder.")
        return

    print("\n=== Verification Results ===")

    missing = []
    suspicious = []
    ok = []

    for pdf_path in pdfs:
        expected_url = f"{PUBLIC_MAGAZINE_PREFIX}/{pdf_path.name}"
        rows = mag_df[mag_df["url"].astype(str) == expected_url]

        page_count = get_pdf_page_count(pdf_path)
        chunk_count = len(rows)

        if rows.empty:
            missing.append(pdf_path.name)
            print(f"❌ MISSING: {pdf_path.name}")
            print(f"   Expected URL: {expected_url}")
            continue

        pages_indexed = set()

        if "page" in rows.columns:
            for page in rows["page"].dropna():
                try:
                    pages_indexed.add(int(page))
                except Exception:
                    pass

        indexed_page_count = len(pages_indexed)

        if page_count:
            page_coverage = indexed_page_count / page_count
        else:
            page_coverage = 0.0

        flags = []

        if chunk_count < MIN_CHUNKS_PER_PDF:
            flags.append(f"low chunk count ({chunk_count})")

        if page_count and page_coverage < MIN_PAGE_COVERAGE:
            flags.append(f"low page coverage ({indexed_page_count}/{page_count}, {page_coverage:.0%})")

        if flags:
            suspicious.append(pdf_path.name)
            print(f"⚠️  CHECK: {pdf_path.name}")
            print(f"   Chunks: {chunk_count}")
            print(f"   Pages indexed: {indexed_page_count}/{page_count or 'unknown'}")
            print(f"   Flags: {', '.join(flags)}")
        else:
            ok.append(pdf_path.name)
            print(f"✅ OK: {pdf_path.name}")
            print(f"   Chunks: {chunk_count}")
            print(f"   Pages indexed: {indexed_page_count}/{page_count or 'unknown'}")

    db_urls = set(mag_df["url"].astype(str).unique())
    uploaded_urls = {f"{PUBLIC_MAGAZINE_PREFIX}/{p.name}" for p in pdfs}
    orphan_urls = sorted(db_urls - uploaded_urls)

    print("\n=== Summary ===")
    print(f"Uploaded PDFs: {len(pdfs)}")
    print(f"OK: {len(ok)}")
    print(f"Missing from DB: {len(missing)}")
    print(f"Suspicious / possibly partial: {len(suspicious)}")
    print(f"Orphan DB URLs with no matching PDF file: {len(orphan_urls)}")

    if missing:
        print("\nMissing PDFs:")
        for name in missing:
            print(f" - {name}")

    if suspicious:
        print("\nPossibly partial PDFs:")
        for name in suspicious:
            print(f" - {name}")

    if orphan_urls:
        print("\nOrphan database URLs:")
        for url in orphan_urls:
            print(f" - {url}")

    print("\nDone.")


if __name__ == "__main__":
    main()
