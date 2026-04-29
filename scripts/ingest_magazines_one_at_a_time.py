from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path

# Render disk folders
MAGAZINE_DIR = Path("/data/magazines")
QUEUE_DIR = Path("/data/magazines_queue")
DONE_DIR = Path("/data/magazines_done")
FAILED_DIR = Path("/data/magazines_failed")
DOCUMENTS_FILE = Path("/data/documents.jsonl")

# Existing low-memory ingest script, kept for backward compatibility
INGEST_COMMAND = ["python", "scripts/ingest_magazines.py"]

PAUSE_SECONDS = int(os.getenv("PDF_BATCH_PAUSE_SECONDS", "20"))
MAX_FILES = int(os.getenv("PDF_BATCH_MAX_FILES", "0"))


def log(msg: str) -> None:
    print(msg, flush=True)


def ensure_dirs() -> None:
    for folder in [MAGAZINE_DIR, QUEUE_DIR, DONE_DIR, FAILED_DIR]:
        folder.mkdir(parents=True, exist_ok=True)
    DOCUMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    DOCUMENTS_FILE.touch(exist_ok=True)


def magazine_url(filename: str) -> str:
    return f"/magazines/{filename}"


def extract_pdf_pages(pdf_path: Path) -> list[dict]:
    """
    Extract one document record per PDF page.
    Uses PyMuPDF if available.
    """
    try:
        import fitz  # PyMuPDF
    except ImportError as exc:
        raise RuntimeError(
            "PyMuPDF is required for PDF extraction. Add pymupdf to requirements.txt."
        ) from exc

    records = []
    filename = pdf_path.name
    source_name = pdf_path.stem

    doc = fitz.open(pdf_path)

    for page_index, page in enumerate(doc, start=1):
        text = page.get_text("text").strip()

        if not text:
            continue

        records.append(
            {
                "url": magazine_url(filename),
                "title": f"{source_name} (PDF, p. {page_index})",
                "source_name": source_name,
                "text": text,
                "published_at": None,
                "category": "Magazine archive",
                "visibility": "public",
                "surface_policy": "show_source",
                "attribution_label": "Magazine archive",
                "source_type": "pdf",
                "pdf_filename": filename,
                "page": page_index,
            }
        )

    doc.close()
    return records


def remove_existing_pdf_records(filename: str) -> int:
    """
    Remove old records for this PDF before re-adding clean ones.
    Prevents duplicates if the script is rerun.
    """
    if not DOCUMENTS_FILE.exists():
        return 0

    kept_lines = []
    removed = 0

    with DOCUMENTS_FILE.open("r", encoding="utf-8") as f:
        for line in f:
            raw = line.strip()
            if not raw:
                continue

            try:
                record = json.loads(raw)
            except Exception:
                kept_lines.append(raw)
                continue

            if record.get("pdf_filename") == filename or filename in str(record.get("url", "")):
                removed += 1
                continue

            kept_lines.append(json.dumps(record, ensure_ascii=False))

    with DOCUMENTS_FILE.open("w", encoding="utf-8") as f:
        for line in kept_lines:
            f.write(line + "\n")

    return removed


def append_pdf_records_to_documents(pdf_path: Path) -> int:
    filename = pdf_path.name

    removed = remove_existing_pdf_records(filename)
    if removed:
        log(f"Removed {removed} old document record(s) for {filename}")

    records = extract_pdf_pages(pdf_path)

    with DOCUMENTS_FILE.open("a", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    log(f"Appended {len(records)} public PDF page record(s) for {filename}")
    return len(records)


def move_current_uploads_to_queue() -> None:
    for pdf in sorted(MAGAZINE_DIR.glob("*.pdf")):
        target = QUEUE_DIR / pdf.name
        if target.exists():
            log(f"Queue already has {pdf.name}; leaving active copy in place.")
        else:
            shutil.move(str(pdf), str(target))
            log(f"Queued: {pdf.name}")


def clear_active_folder() -> None:
    for pdf in MAGAZINE_DIR.glob("*.pdf"):
        log(f"Removing leftover active PDF: {pdf.name}")
        pdf.unlink()


def run_ingest_for_one(pdf_path: Path) -> bool:
    active_path = MAGAZINE_DIR / pdf_path.name

    clear_active_folder()
    shutil.move(str(pdf_path), str(active_path))
    log(f"\n=== INGESTING ONE ISSUE: {active_path.name} ===")

    try:
        appended = append_pdf_records_to_documents(active_path)

        if appended == 0:
            raise RuntimeError("No text pages were extracted from PDF.")

        # Optional legacy ingest, retained but no longer required for documents.jsonl
        if os.getenv("RUN_LEGACY_PDF_INGEST", "false").lower() in {"1", "true", "yes"}:
            result = subprocess.run(INGEST_COMMAND)
            if result.returncode != 0:
                raise RuntimeError(f"Legacy ingest failed with return code {result.returncode}")

        log(f"SUCCESS: {active_path.name}")
        shutil.copy2(str(active_path), str(DONE_DIR / active_path.name))
        return True

    except Exception as exc:
        log(f"FAILED: {active_path.name}: {exc}")
        shutil.copy2(str(active_path), str(FAILED_DIR / active_path.name))
        return False


def main() -> None:
    ensure_dirs()

    log("Preparing queue...")
    move_current_uploads_to_queue()
    clear_active_folder()

    queue = sorted(QUEUE_DIR.glob("*.pdf"))

    if MAX_FILES > 0:
        queue = queue[:MAX_FILES]

    log(f"PDFs queued for this run: {len(queue)}")
    log(f"Pause between issues: {PAUSE_SECONDS} seconds")

    processed = 0
    succeeded = 0
    failed = 0

    for pdf in queue:
        processed += 1
        ok = run_ingest_for_one(pdf)

        if ok:
            succeeded += 1
            try:
                pdf.unlink()
            except Exception:
                pass
        else:
            failed += 1

        log(
            f"Progress: {processed}/{len(queue)} processed; "
            f"{succeeded} succeeded; {failed} failed"
        )

        if processed < len(queue):
            log(f"Pausing {PAUSE_SECONDS} seconds before next PDF...")
            time.sleep(PAUSE_SECONDS)

    clear_active_folder()

    log("\n=== BATCH INGEST COMPLETE ===")
    log(f"Processed: {processed}")
    log(f"Succeeded: {succeeded}")
    log(f"Failed: {failed}")
    log(f"Remaining queued: {len(list(QUEUE_DIR.glob('*.pdf')))}")
    log(f"Done folder: {DONE_DIR}")
    log(f"Failed folder: {FAILED_DIR}")


if __name__ == "__main__":
    main()
