from __future__ import annotations

from functools import lru_cache
from typing import List, Dict

from openai import OpenAI

from app.config import get_settings


@lru_cache(maxsize=1)
def get_openai_client() -> OpenAI:
    settings = get_settings()
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY is not configured.")
    return OpenAI(api_key=settings.openai_api_key)


def chat_completion(messages: List[Dict[str, str]], model: str = "gpt-4o-mini") -> str:
    """
    Safe wrapper for OpenAI chat completions.
    Always returns plain text output.
    """
    client = get_openai_client()

    completion = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=0.3,
    )

    return completion.choices[0].message.content or ""
