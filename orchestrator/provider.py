"""Thin async client for the Magic Hour REST API (shapes match magic-hour SDK v0.96).

We call the REST API directly instead of the SDK because the orchestrator needs the raw status
code and Retry-After header of every response to drive retries and rate limiting, and needs to
point at the mock server during load tests.
"""
from __future__ import annotations

import email.utils
import random
import time
from dataclasses import dataclass

import httpx

from .config import settings

IMAGE_TOOLS = {
    "ai-image-generator", "ai-image-editor", "ai-headshot-generator", "ai-face-editor", "ai-clothes-changer",
    "ai-qr-code-generator", "ai-gif-generator", "ai-meme-generator", "ai-image-upscaler", "face-swap-photo",
    "image-background-remover", "photo-colorizer", "head-swap", "body-swap",
}
AUDIO_TOOLS = {"ai-voice-generator", "ai-voice-cloner", "ai-music-generator"}


def project_kind(endpoint: str) -> str:
    tool = endpoint.rstrip("/").rsplit("/", 1)[-1]
    if tool in IMAGE_TOOLS:
        return "image"
    if tool in AUDIO_TOOLS:
        return "audio"
    return "video"


@dataclass
class Outcome:
    kind: str                  # ok | throttled | retryable | permanent
    status: int | None = None
    body: dict | None = None
    retry_after: float = 0.0
    error: str = ""


def parse_retry_after(value: str | None, default: float = 1.0) -> float:
    if not value:
        return default
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            return max(0.0, email.utils.parsedate_to_datetime(value).timestamp() - time.time())
        except Exception:
            return default


def classify(resp: httpx.Response | None, exc: Exception | None) -> Outcome:
    if exc is not None:
        # Timeouts and dropped connections are *ambiguous*: the project may or may not exist.
        # They are retried with the same upstream idempotency key.
        return Outcome("retryable", error=f"{type(exc).__name__}: {exc}"[:300])
    assert resp is not None
    code = resp.status_code
    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text[:300]}
    if 200 <= code < 300:
        return Outcome("ok", code, body)
    if code == 429:
        return Outcome("throttled", code, body, parse_retry_after(resp.headers.get("retry-after")), "429")
    if code in (408, 409, 425) or code >= 500:
        return Outcome("retryable", code, body, parse_retry_after(resp.headers.get("retry-after"), 0.0),
                       f"HTTP {code}")
    # 400/401/402/403/404/422: retrying will not help
    return Outcome("permanent", code, body, error=f"HTTP {code}: {str(body)[:300]}")


def backoff(tries: int, base: float | None = None, cap: float | None = None) -> float:
    """Exponential backoff with full jitter (AWS architecture blog): uniform(0, min(cap, base*2^n))."""
    base = settings.backoff_base_s if base is None else base
    cap = settings.backoff_cap_s if cap is None else cap
    return random.uniform(0, min(cap, base * (2 ** max(0, tries - 1))))


class MagicHour:
    def __init__(self, base_url: str | None = None, api_key: str | None = None, max_connections: int = 64):
        self.client = httpx.AsyncClient(
            base_url=base_url or settings.mh_base_url,
            headers={"Authorization": f"Bearer {api_key or settings.mh_api_key}"},
            timeout=httpx.Timeout(settings.mh_timeout_s, connect=5.0),
            limits=httpx.Limits(max_connections=max_connections, max_keepalive_connections=max_connections),
        )

    async def create(self, endpoint: str, payload: dict, idempotency_key: str) -> Outcome:
        headers = {"Idempotency-Key": idempotency_key} if settings.mh_idempotency_header else {}
        try:
            resp = await self.client.post(endpoint, json=payload, headers=headers)
            return classify(resp, None)
        except httpx.HTTPError as exc:
            return classify(None, exc)

    async def get_project(self, kind: str, project_id: str) -> Outcome:
        try:
            resp = await self.client.get(f"/v1/{kind}-projects/{project_id}")
            return classify(resp, None)
        except httpx.HTTPError as exc:
            return classify(None, exc)

    async def aclose(self) -> None:
        await self.client.aclose()
