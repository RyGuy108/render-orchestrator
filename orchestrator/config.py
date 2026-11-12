"""All knobs come from the environment so the same code runs locally, in Docker and in the load test."""
from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    if isinstance(default, bool):
        return raw.lower() in ("1", "true", "yes")
    return type(default)(raw)


@dataclass(frozen=True)
class Settings:
    database_url: str = field(default_factory=lambda: _env("DATABASE_URL", "postgresql://postgres@localhost:5432/orch"))
    redis_url: str = field(default_factory=lambda: _env("REDIS_URL", "redis://localhost:6379/0"))

    # Upstream (real Magic Hour or the mock)
    mh_base_url: str = field(default_factory=lambda: _env("MH_BASE_URL", "https://api.magichour.ai"))
    mh_api_key: str = field(default_factory=lambda: _env("MAGIC_HOUR_API_KEY", ""))
    mh_timeout_s: float = field(default_factory=lambda: _env("MH_TIMEOUT_S", 20.0))
    # Send an Idempotency-Key header upstream. The mock honours it; the real API ignores unknown headers.
    mh_idempotency_header: bool = field(default_factory=lambda: _env("MH_IDEMPOTENCY_HEADER", True))
    webhook_secret: str = field(default_factory=lambda: _env("WEBHOOK_SECRET", "dev-secret"))

    # Upstream capacity. The limiter adapts the concurrency between min and max (AIMD).
    upstream_rps: float = field(default_factory=lambda: _env("UPSTREAM_RPS", 15.0))
    upstream_burst: int = field(default_factory=lambda: _env("UPSTREAM_BURST", 20))
    max_inflight: int = field(default_factory=lambda: _env("MAX_INFLIGHT", 48))
    min_inflight: int = field(default_factory=lambda: _env("MIN_INFLIGHT", 4))
    tenant_max_inflight: int = field(default_factory=lambda: _env("TENANT_MAX_INFLIGHT", 32))

    # Retries
    max_attempts: int = field(default_factory=lambda: _env("MAX_ATTEMPTS", 6))
    backoff_base_s: float = field(default_factory=lambda: _env("BACKOFF_BASE_S", 0.5))
    backoff_cap_s: float = field(default_factory=lambda: _env("BACKOFF_CAP_S", 30.0))

    # Workers
    workers: int = field(default_factory=lambda: _env("WORKERS", 4))
    worker_concurrency: int = field(default_factory=lambda: _env("WORKER_CONCURRENCY", 16))
    lease_s: float = field(default_factory=lambda: _env("LEASE_S", 30.0))

    # Completion: webhooks first, polling as fallback for renders we have not heard about.
    poll_after_s: float = field(default_factory=lambda: _env("POLL_AFTER_S", 8.0))
    poll_max_interval_s: float = field(default_factory=lambda: _env("POLL_MAX_INTERVAL_S", 30.0))
    render_timeout_s: float = field(default_factory=lambda: _env("RENDER_TIMEOUT_S", 900.0))

    # Fairness: one lower-lane pick every N picks so batch work is never starved.
    starvation_guard_every: int = field(default_factory=lambda: _env("STARVATION_GUARD_EVERY", 5))


settings = Settings()
