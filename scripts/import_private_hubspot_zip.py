from __future__ import annotations

import argparse
import html
import json
import re
import zipfile
from pathlib import Path
from typing import Iterable, Optional

from bs4 import BeautifulSoup

from app.governance import apply_governance

DEFAULT_ATTRIBUTION = "Green Builder Media's editorial archive"
BLOG_PATH_RE = re.compile(r"/blog/")
TEMP_SLUG_RE = re.compile(r"-temporary-slug-")
DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
PROMO_CLASS_RE = re.compile(r"related|blog-index|menu|social|newsletter|subscribe|comment|sidebar", re.I)
POST_CLASS_RE = re.compile(r"blog-post|post|article", re.I)


def _clean_lines(text: str) -> str:
    lines = []
    seen = set()
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw).strip()
        if not line:
            continue
        if line.lower() in {"calendar icon", "reading time"}:
            continue
        if line in seen and len(line) < 80:
            continue
        seen.add(line)
        lines.append(line)
    return "\n".join(lines).strip()


def _pick_main_content(soup: BeautifulSoup):
    node = soup.find("article", class_=re.compile(r"blog-post", re.I))
    if node:
        return node
    node = soup.find("article", class_=POST_CLASS_RE)
    if node:
        return node
    node = soup.find("article")
    if node:
        return node
    node = soup.find("div", class_=POST_CLASS_RE)
    if node:
        return node
    return soup.find("main") or soup.body


def extract_text_and_meta(raw_html: str, path: str) -> dict | None:
    soup = BeautifulSoup(raw_html, "lxml")

    title = None
    for candidate in [
        soup.find("meta", attrs={"property": "og:title"}),
        soup.find("meta", attrs={"name": "twitter:title"}),
        soup.title,
        soup.find("h1"),
    ]:
        if not candidate:
            continue
        value = candidate.get("content") if hasattr(candidate, "get") else None
        if not value:
            value = candidate.get_text(" ", strip=True)
        if value:
            title = html.unescape(value).replace("...", "").strip()
            break

    description = None
    for candidate in [
        soup.find("meta", attrs={"name": "description"}),
        soup.find("meta", attrs={"property": "og:description"}),
    ]:
        if candidate and candidate.get("content"):
            description = html.unescape(candidate["content"]).strip()
            break

    published_at: Optional[str] = None
    for candidate in [
        soup.find("meta", attrs={"property": "article:published_time"}),
        soup.find("meta", attrs={"name": "article:published_time"}),
        soup.find("time"),
    ]:
        if not candidate:
            continue
        value = candidate.get("content") if hasattr(candidate, "get") else None
        if not value:
            value = candidate.get_text(" ", strip=True)
        if value:
            m = DATE_RE.search(value)
            published_at = m.group(1) if m else value.strip()
            break

    for node in soup.find_all(["script", "style", "noscript", "header", "footer", "nav"]):
        node.decompose()
    for node in soup.find_all(class_=PROMO_CLASS_RE):
        node.decompose()

    main = _pick_main_content(soup)
    if not main:
        return None

    for node in main.find_all(["aside", "form"]):
        node.decompose()
    for node in main.find_all("article", class_=re.compile(r"blog-index__post", re.I)):
        node.decompose()

    text = main.get_text("\n", strip=True)
    text = html.unescape(text)
    text = _clean_lines(text)

    if description and description not in text:
        text = description + "\n\n" + text

    if len(text) < 400:
        return None

    category = None
    parts = path.split("/")
    if "blog" in parts:
        idx = parts.index("blog")
        if idx + 1 < len(parts) - 1:
            possible = parts[idx + 1]
            if ".html" not in possible:
                category = possible.replace("-", " ").title()

    return {
        "url": "",
        "title": title or Path(path).stem.replace("-", " ").title(),
        "published_at": published_at,
        "category": category,
        "text": text,
        "visibility": "private",
        "attribution_label": DEFAULT_ATTRIBUTION,
        "source_path": path,
    }


def iter_html_members(zf: zipfile.ZipFile) -> Iterable[str]:
    for name in zf.namelist():
        lower = name.lower()
        if not lower.endswith(".html"):
            continue
        if not BLOG_PATH_RE.search(lower):
            continue
        if TEMP_SLUG_RE.search(lower):
            continue
        yield name


def main() -> None:
    parser = argparse.ArgumentParser(description="Import private HubSpot-exported blog HTML into documents.jsonl")
    parser.add_argument("zip_path", help="Path to HubSpot export ZIP")
    parser.add_argument("--output", default="./data/private_documents.jsonl", help="Output JSONL path")
    parser.add_argument(
        "--append-to-docs",
        default=None,
        help="Optional path to append these private docs into an existing documents.jsonl file",
    )
    args = parser.parse_args()

    zip_path = Path(args.zip_path)
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    docs = []
    with zipfile.ZipFile(zip_path) as zf:
        for name in iter_html_members(zf):
            raw_html = zf.read(name).decode("utf-8", "ignore")
            doc = extract_text_and_meta(raw_html, name)
            if doc:
                docs.append(apply_governance(doc))

    with out_path.open("w", encoding="utf-8") as f:
        for doc in docs:
            f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    if args.append_to_docs:
        combined_path = Path(args.append_to_docs)
        combined_path.parent.mkdir(parents=True, exist_ok=True)
        with combined_path.open("a", encoding="utf-8") as f:
            for doc in docs:
                f.write(json.dumps(doc, ensure_ascii=False) + "\n")

    print(f"Imported {len(docs)} private blog documents from {zip_path} -> {out_path}")


if __name__ == "__main__":
    main()
