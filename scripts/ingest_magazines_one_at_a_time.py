from __future__ import annotations

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

# Path to your existing low-memory ingest script
INGEST_COMMAND = ["python", "scripts/ingest_magazines.py"]

# Pause between issues to let memory settle
PAUSE_SECONDS = int(os.getenv("PDF_BATCH_PAUSE_SECONDS", "20"))

# Set to a number like 3 for testing, or leave 0 to process all
MAX_FILES = int(os.getenv("PDF_BATCH_MAX_FILES", "0"))


def log(msg: str) -> None:
    print(msg, flush=True)


def ensure_dirs() -> None:
    MAGAZINE_DIR.mkdir(parents=True, exist_ok=True)
    QUEUE_DIR.mkdir(parents=True, exist_ok=True)
    DONE_DIR.mkdir(parents=True, exist_ok=True)
    FAILED_DIR.mkdir(parents=True, exist_ok=True)


def move_current_uploads_to_queue() -> None:
    """
    Move PDFs currently in /data/magazines into /data/magazines_queue.
    The batch runner will then move one file at a time back into /data/magazines.
    """
    for pdf in sorted(MAGAZINE_DIR.glob("*.pdf")):
        target = QUEUE_DIR / pdf.name
        if target.exists():
            log(f"Queue already has {pdf.name}; removing duplicate from active folder.")
            pdf.unlink()
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

    result = subprocess.run(INGEST_COMMAND)

    if result.returncode == 0:
        log(f"SUCCESS: {active_path.name}")
        shutil.move(str(active_path), str(DONE_DIR / active_path.name))
        return True

    log(f"FAILED: {active_path.name} with return code {result.returncode}")
    shutil.move(str(active_path), str(FAILED_DIR / active_path.name))
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
        else:
            failed += 1

        log(f"Progress: {processed}/{len(queue)} processed; {succeeded} succeeded; {failed} failed")

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
