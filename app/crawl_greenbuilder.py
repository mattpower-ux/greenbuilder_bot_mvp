from __future__ import annotations

import asyncio
import json
import random
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse

import httpx
import trafilatura
from bs4 import BeautifulSoup

from app.config import get_settings

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
DEBUG_TARGET_PATTERNS = [
    "green-builder-sustainable-brand-index-2026",
    "sustainable-brand-index-2026",
]

FULL_CRAWL_INTERVAL_DAYS = 5
RECENT_LOOKBACK_HOURS = 24
CRAWL_STATE_FILE_NAME = "crawl_state.json"


@dataclass
class SitemapEntry:
    url: str
    lastmod: Optional[str] = None


@dataclass
class Doc:
    url: str
    title: str
    text: str
    published_at: Optional[str]
    category: Optional[str]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_dt(value: str | None) -> Optional[datetime]:
    if not value:
        return None

    value = value.strip()
    if not value:
        return None

    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        pass

    # Date-only fallback
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def iso_now() -> str:
    return utc_now().isoformat()


def normalize_text(text: str) -> str:
    text = text.replace("\r", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def looks_like_debug_target(url: str) -> bool:
    lower_url = (url or "").lower()
    return any(pattern in lower_url for pattern in DEBUG_TARGET_PATTERNS)


def crawl_state_path(settings) -> Path:
    return settings.data_dir / CRAWL_STATE_FILE_NAME


def load_crawl_state(settings) -> dict:
    path = crawl_state_path(settings)
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_crawl_state(settings, state: dict) -> None:
    path = crawl_state_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def should_run_full_crawl(settings) -> bool:
    state = load_crawl_state(settings)
    last_full = parse_dt(state.get("last_full_crawl_at"))
    if not last_full:
        return True
    return utc_now() - last_full >= timedelta(days=FULL_CRAWL_INTERVAL_DAYS)


def is_recent_entry(entry: SitemapEntry) -> bool:
    lastmod_dt = parse_dt(entry.lastmod)
    if not lastmod_dt:
        return False
    return utc_now() - lastmod_dt <= timedelta(hours=RECENT_LOOKBACK_HOURS)


def choose_entries_for_this_run(entries: List[SitemapEntry], full_crawl: bool) -> List[SitemapEntry]:
    if full_crawl:
        # Newest first improves odds of getting the pages that matter most before any block/rate-limit kicks in.
        return sorted(
            entries,
            key=lambda e: parse_dt(e.lastmod) or datetime.min.replace(tzinfo=timezone.utc),
            reverse=True,
        )

    recent_entries = [e for e in entries if is_recent_entry(e)]
    # Also crawl newest first for recent runs.
    return sorted(
        recent_entries,
        key=lambda e: parse_dt(e.lastmod) or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )


def allow_url(url: str) -> bool:
    parsed = urlparse(url)
    if parsed.netloc not in {"www.greenbuildermedia.com", "greenbuildermedia.com"}:
        return False

    blocked = [
        "/_hcms/preview/",
        "/hs/manage-preferences/",
        "/hs/preferences-center/",
    ]
    if any(part in url for part in blocked):
        return False

    likely_content = [
        "/blog",
        "/magazine",
        "/ebooks",
        "/podcasts",
        "/vision-house",
        "/todays-homeowner",
    ]
    return any(part in parsed.path.lower() for part in likely_content)


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    retries = 3
    base_delay_seconds = 2.0

    for attempt in range(1, retries + 1):
        try:
            resp = await client.get(url, follow_redirects=True, timeout=30)

            if resp.status_code == 403 and attempt < retries:
                wait = base_delay_seconds * attempt + random.uniform(0.25, 1.0)
                print(f"403 on {url} (attempt {attempt}/{retries}), retrying in {wait:.1f}s")
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.text
        except httpx.HTTPStatusError:
            if attempt >= retries:
                raise
            wait = base_delay_seconds * attempt + random.uniform(0.25, 1.0)
            await asyncio.sleep(wait)
        except httpx.HTTPError:
            if attempt >= retries:
                raise
            wait = base_delay_seconds * attempt + random.uniform(0.25, 1.0)
            await asyncio.sleep(wait)

    raise RuntimeError(f"Failed to fetch {url}")


async def fetch_sitemap_urls(client: httpx.AsyncClient, sitemap_url: str) -> List[SitemapEntry]:
    xml_text = await fetch_text(client, sitemap_url)
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    entries: List[SitemapEntry] = []

    if root.tag.endswith("sitemapindex"):
        sitemap_nodes = root.findall("sm:sitemap", ns)
        nested_urls = []
        for node in sitemap_nodes:
            loc_node = node.find("sm:loc", ns)
            if loc_node is not None and loc_node.text:
                nested_urls.append(loc_node.text.strip())
        for nested_url in nested_urls:
            entries.extend(await fetch_sitemap_urls(client, nested_url))
        return entries

    url_nodes = root.findall("sm:url", ns)
    for node in url_nodes:
        loc_node = node.find("sm:loc", ns)
        if loc_node is None or not loc_node.text:
            continue
        lastmod_node = node.find("sm:lastmod", ns)
        entries.append(
            SitemapEntry(
                url=loc_node.text.strip(),
                lastmod=lastmod_node.text.strip() if lastmod_node is not None and lastmod_node.text else None,
            )
        )

    return entries


def extract_best_title(soup: BeautifulSoup, extracted_title: str) -> str:
    if extracted_title:
        return extracted_title.strip()

    og_title = soup.find("meta", attrs={"property": "og:title"})
    if og_title and og_title.get("content"):
        return og_title["content"].strip()

    twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
    if twitter_title and twitter_title.get("content"):
        return twitter_title["content"].strip()

    h1 = soup.find("h1")
    if h1:
        h1_text = h1.get_text(" ", strip=True)
        if h1_text:
            return h1_text

    if soup.title and soup.title.string:
        return soup.title.string.strip()

    return "Untitled"


def extract_fallback_text(soup: BeautifulSoup) -> str:
    for node in [
        soup.find("article"),
        soup.find("main"),
        soup.find(attrs={"role": "main"}),
        soup.body,
    ]:
        if node:
            text = node.get_text("\n", strip=True)
            text = normalize_text(text)
            if text:
                return text
    return ""


def extract_metadata(html: str, url: str) -> Doc | None:
    downloaded = trafilatura.extract(
        html,
        include_links=False,
        include_comments=False,
        output_format="json",
    )

    soup = BeautifulSoup(html, "html.parser")

    extracted_title = ""
    extracted_text = ""

    if downloaded:
        try:
            data = json.loads(downloaded)
            extracted_title = (data.get("title") or "").strip()
            extracted_text = normalize_text((data.get("text") or "").strip())
        except Exception:
            extracted_title = ""
            extracted_text = ""

    title = extract_best_title(soup, extracted_title)
    text = extracted_text

    if not text or len(text) < 500:
        fallback_text = extract_fallback_text(soup)
        if len(fallback_text) > len(text):
            text = fallback_text

    if not text or len(text) < 500:
        return None

    published_at = None
    for candidate in [
        soup.find("meta", attrs={"property": "article:published_time"}),
        soup.find("meta", attrs={"name": "article:published_time"}),
        soup.find("meta", attrs={"property": "og:published_time"}),
        soup.find("time"),
    ]:
        if not candidate:
            continue
        content = candidate.get("content") or candidate.get_text(" ", strip=True)
        if content:
            published_at = content.strip()
            break

    category = None
    og_section = soup.find("meta", attrs={"property": "article:section"})
    if og_section and og_section.get("content"):
        category = og_section["content"].strip()

    return Doc(
        url=url,
        title=title,
        text=text,
        published_at=published_at,
        category=category,
    )


def load_existing_docs(docs_path: Path) -> Dict[str, Doc]:
    if not docs_path.exists():
        return {}

    docs: Dict[str, Doc] = {}
    with docs_path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                doc = Doc(
                    url=raw.get("url", ""),
                    title=raw.get("title", "Untitled"),
                    text=raw.get("text", ""),
                    published_at=raw.get("published_at"),
                    category=raw.get("category"),
                )
                if doc.url:
                    docs[doc.url] = doc
            except Exception:
                continue
    return docs


def save_docs(docs_path: Path, docs_by_url: Dict[str, Doc]) -> None:
    docs_path.parent.mkdir(parents=True, exist_ok=True)
    with docs_path.open("w", encoding="utf-8") as f:
        for url in sorted(docs_by_url.keys()):
            doc = docs_by_url[url]
            f.write(json.dumps(doc.__dict__, ensure_ascii=False) + "\n")


async def main() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    docs_path: Path = settings.docs_file

    full_crawl = should_run_full_crawl(settings)
    state = load_crawl_state(settings)

    headers = {
        "User-Agent": settings.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": settings.site_base_url,
    }

    async with httpx.AsyncClient(headers=headers) as client:
        sitemap_entries = await fetch_sitemap_urls(client, settings.sitemap_url)
        sitemap_entries = [e for e in sitemap_entries if allow_url(e.url)]

        deduped: Dict[str, SitemapEntry] = {}
        for entry in sitemap_entries:
            deduped[entry.url] = entry

        all_entries = list(deduped.values())
        entries_to_crawl = choose_entries_for_this_run(all_entries, full_crawl)

        mode = "FULL" if full_crawl else "RECENT"
        print(f"Crawl mode: {mode}")
        print(f"Candidate URLs total: {len(all_entries)}")
        print(f"URLs selected this run: {len(entries_to_crawl)}")

        existing_docs = load_existing_docs(docs_path)
        results_by_url: Dict[str, Doc] = existing_docs.copy()

        for idx, entry in enumerate(entries_to_crawl, start=1):
            url = entry.url
            try:
                if looks_like_debug_target(url):
                    print(f"DEBUG TARGET URL REACHED: {url}")

                html = await fetch_text(client, url)
                doc = extract_metadata(html, url)

                if doc:
                    results_by_url[url] = doc
                    print(f"[{idx}/{len(entries_to_crawl)}] kept {url}")

                    if looks_like_debug_target(url):
                        print(f"DEBUG TARGET TITLE: {doc.title}")
                        print(f"DEBUG TARGET TEXT LENGTH: {len(doc.text)}")
                        print(f"DEBUG TARGET PUBLISHED_AT: {doc.published_at}")
                        print(f"DEBUG TARGET TEXT PREVIEW: {doc.text[:500]}")
                else:
                    print(f"[{idx}/{len(entries_to_crawl)}] skipped {url}")
                    if looks_like_debug_target(url):
                        print("DEBUG TARGET WAS SKIPPED AFTER EXTRACTION")

                # Gentle pacing to reduce odds of 403/rate-limit blocks
                await asyncio.sleep(random.uniform(0.6, 1.2))

            except Exception as exc:
                print(f"[{idx}/{len(entries_to_crawl)}] error {url}: {exc}")
                await asyncio.sleep(random.uniform(1.5, 3.0))

    save_docs(docs_path, results_by_url)

    state["last_crawl_at"] = iso_now()
    state["last_crawl_mode"] = "full" if full_crawl else "recent"
    state["last_candidate_url_count"] = len(all_entries)
    state["last_selected_url_count"] = len(entries_to_crawl)
    state["last_saved_doc_count"] = len(results_by_url)
    if full_crawl:
        state["last_full_crawl_at"] = iso_now()
    save_crawl_state(settings, state)

    print(f"Saved {len(results_by_url)} documents to {docs_path}")


if __name__ == "__main__":
    asyncio.run(main())
