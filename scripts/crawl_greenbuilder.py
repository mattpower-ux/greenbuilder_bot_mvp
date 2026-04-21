from __future__ import annotations

import asyncio
import json
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import httpx
import trafilatura
from bs4 import BeautifulSoup

from app.config import get_settings

DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


@dataclass
class Doc:
    url: str
    title: str
    text: str
    published_at: Optional[str]
    category: Optional[str]


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    resp = await client.get(url, follow_redirects=True, timeout=30)
    resp.raise_for_status()
    return resp.text


async def fetch_sitemap_urls(client: httpx.AsyncClient, sitemap_url: str) -> List[str]:
    xml_text = await fetch_text(client, sitemap_url)
    root = ET.fromstring(xml_text)
    ns = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    urls: List[str] = []
    if root.tag.endswith("sitemapindex"):
        sitemap_nodes = root.findall("sm:sitemap/sm:loc", ns)
        nested = [node.text.strip() for node in sitemap_nodes if node.text]
        for nested_url in nested:
            urls.extend(await fetch_sitemap_urls(client, nested_url))
        return urls

    url_nodes = root.findall("sm:url/sm:loc", ns)
    for node in url_nodes:
        if node.text:
            urls.append(node.text.strip())
    return urls


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
    likely_content = ["/blog", "/magazine", "/ebooks", "/podcasts", "/vision-house", "/todays-homeowner"]
    return any(part in parsed.path.lower() for part in likely_content)


def extract_metadata(html: str, url: str) -> Doc | None:
    downloaded = trafilatura.extract(
        html,
        include_links=False,
        include_comments=False,
        output_format="json",
    )
    if not downloaded:
        return None

    data = json.loads(downloaded)
    text = (data.get("text") or "").strip()
    title = (data.get("title") or "").strip()
    if not text or len(text) < 500:
        return None

    soup = BeautifulSoup(html, "html.parser")
    published_at = None

    for candidate in [
        soup.find("meta", attrs={"property": "article:published_time"}),
        soup.find("meta", attrs={"name": "article:published_time"}),
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


async def main() -> None:
    settings = get_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    docs_path: Path = settings.docs_file

    headers = {"User-Agent": settings.user_agent}
    async with httpx.AsyncClient(headers=headers) as client:
        urls = await fetch_sitemap_urls(client, settings.sitemap_url)
        urls = [u for u in urls if allow_url(u)]
        urls = list(dict.fromkeys(urls))

        print(f"Candidate URLs: {len(urls)}")
        results: List[Doc] = []
        for idx, url in enumerate(urls, start=1):
            try:
                html = await fetch_text(client, url)
                doc = extract_metadata(html, url)
                if doc:
                    results.append(doc)
                    print(f"[{idx}/{len(urls)}] kept {url}")
                else:
                    print(f"[{idx}/{len(urls)}] skipped {url}")
            except Exception as exc:
                print(f"[{idx}/{len(urls)}] error {url}: {exc}")

    with docs_path.open("w", encoding="utf-8") as f:
        for doc in results:
            f.write(json.dumps(doc.__dict__, ensure_ascii=False) + "\n")

    print(f"Saved {len(results)} documents to {docs_path}")


if __name__ == "__main__":
    asyncio.run(main())
