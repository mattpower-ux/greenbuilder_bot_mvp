from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import List

from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()


class Settings(BaseModel):
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_chat_model: str = os.getenv("OPENAI_CHAT_MODEL", "gpt-4.1-mini")
    openai_embedding_model: str = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")

    site_base_url: str = os.getenv("SITE_BASE_URL", "https://www.greenbuildermedia.com")
    sitemap_url: str = os.getenv("SITEMAP_URL", "https://www.greenbuildermedia.com/sitemap.xml")
    robots_url: str = os.getenv("ROBOTS_URL", "https://www.greenbuildermedia.com/robots.txt")
    user_agent: str = os.getenv(
        "USER_AGENT",
        "GreenBuilderMediaBotMVP/1.0 (+https://www.greenbuildermedia.com)",
    )

    data_dir: Path = Path(os.getenv("DATA_DIR", "./data"))
    lancedb_dir: Path = Path(os.getenv("LANCEDB_DIR", "./data/lancedb"))
    docs_file: Path = Path(os.getenv("DOCS_FILE", "./data/documents.jsonl"))
    chunks_file: Path = Path(os.getenv("CHUNKS_FILE", "./data/chunks.jsonl"))
    corrections_file: Path = Path(os.getenv("CORRECTIONS_FILE", "./data/corrections.json"))
    qa_log_file: Path = Path(os.getenv("QA_LOG_FILE", "./data/qa_logs.json"))

    top_k: int = int(os.getenv("TOP_K", "8"))
    max_logged_interactions: int = int(os.getenv("MAX_LOGGED_INTERACTIONS", "200"))
    admin_username: str = os.getenv("ADMIN_USERNAME", "editor")
    admin_password: str = os.getenv("ADMIN_PASSWORD", "change-me")
    max_context_chunks: int = int(os.getenv("MAX_CONTEXT_CHUNKS", "6"))
    allowed_origins: List[str] = [
        origin.strip()
        for origin in os.getenv(
            "ALLOWED_ORIGINS",
            "https://www.greenbuildermedia.com,http://localhost:3000,http://localhost:8000",
        ).split(",")
        if origin.strip()
    ]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    settings.lancedb_dir.mkdir(parents=True, exist_ok=True)
    return settings
