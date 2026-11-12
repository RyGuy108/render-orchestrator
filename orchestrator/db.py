"""Postgres access and every job state transition.

Each transition is a single conditional UPDATE (compare-and-set on `status`, and on `lease_owner`
where a worker holds the job), so racing actors (two workers, a webhook and the poller, the reaper
and a slow worker) can never both win. Ledger and outbox rows are written in the same transaction
as the transition and are protected by unique keys, so money and events are exactly-once even when
the transition code runs more than once.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import uuid
from dataclasses import dataclass
from typing import Any

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import AsyncConnectionPool

from .config import settings

SCHEMA = (pathlib.Path(__file__).parent / "schema.sql").read_text()


async def make_pool(min_size: int = 2, max_size: int = 20) -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        settings.database_url,
        min_size=min_size,
        max_size=max_size,
        kwargs={"row_factory": dict_row, "autocommit": False},
        open=False,
    )
    await pool.open(wait=True)
    return pool


async def init_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        # Several processes may boot at once; serialise DDL.
        await conn.execute("SELECT pg_advisory_xact_lock(4242)")
        await conn.execute(SCHEMA)


def request_hash(endpoint: str, payload: dict, priority: int) -> str:
    canon = json.dumps({"e": endpoint, "p": payload, "pr": priority}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode()).hexdigest()


class IdempotencyConflict(Exception):
    """Same Idempotency-Key reused with a different request body."""


@dataclass
class Transition:
    """What a transition did, so callers can update Redis (requeue, release capacity)."""
    applied: bool
    job: dict | None = None
    requeue_delay_s: float | None = None
    released: bool = False


# --------------------------------------------------------------------------- intake

async def create_job(pool, tenant_id: str, idem_key: str, endpoint: str, payload: dict, priority: int):
    """Returns (job, created). Replays with the same key and body return the original job."""
    h = request_hash(endpoint, payload, priority)
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            INSERT INTO jobs (id, tenant_id, idempotency_key, request_hash, priority, endpoint, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
            RETURNING *
            """,
            (uuid.uuid4(), tenant_id, idem_key, h, priority, endpoint, Jsonb(payload)),
        )
        row = await cur.fetchone()
        if row:
            return row, True
        cur = await conn.execute(
            "SELECT * FROM jobs WHERE tenant_id = %s AND idempotency_key = %s", (tenant_id, idem_key)
        )
        row = await cur.fetchone()
        if row["request_hash"] != h:
            raise IdempotencyConflict(str(row["id"]))
        return row, False


async def get_job(pool, job_id) -> dict | None:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
        return await cur.fetchone()


# --------------------------------------------------------------------------- worker side

async def claim(pool, job_id: str, owner: str) -> dict | None:
    """queued -> submitting. A new provider idempotency key is minted only when the previous attempt
    is finished; after an ambiguous failure (timeout, 5xx) the same key is reused so the upstream can
    dedupe the replay."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE jobs SET
                status = 'submitting',
                lease_owner = %(owner)s,
                lease_until = now() + make_interval(secs => %(lease)s),
                tries = tries + 1,
                attempt = CASE WHEN provider_key IS NULL THEN attempt + 1 ELSE attempt END,
                provider_key = COALESCE(provider_key, id::text || ':' || (attempt + 1)),
                first_submit_at = COALESCE(first_submit_at, now())
            WHERE id = %(id)s AND status = 'queued' AND not_before <= now()
            RETURNING *
            """,
            {"id": job_id, "owner": owner, "lease": settings.lease_s},
        )
        return await cur.fetchone()


async def submit_succeeded(pool, job: dict, owner: str, provider_id: str, credits: int, kind: str) -> Transition:
    async with pool.connection() as conn:
        async with conn.transaction():
            # The provider created a project: record it and charge for it no matter what happened
            # to our lease in the meantime. Keyed by (job, attempt) -> at most one charge per render.
            await conn.execute(
                """
                INSERT INTO attempts (job_id, attempt, provider_key, provider_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (provider_key) DO UPDATE SET provider_id = EXCLUDED.provider_id
                """,
                (job["id"], job["attempt"], job["provider_key"], provider_id),
            )
            await conn.execute(
                """
                INSERT INTO ledger (tenant_id, job_id, attempt, kind, credits)
                VALUES (%s, %s, %s, 'charge', %s) ON CONFLICT DO NOTHING
                """,
                (job["tenant_id"], job["id"], job["attempt"], -int(credits)),
            )
            cur = await conn.execute(
                """
                UPDATE jobs SET status = 'rendering', provider_id = %s, provider_kind = %s,
                    credits_charged = %s, submitted_at = now(), lease_owner = NULL, lease_until = NULL,
                    last_polled_at = NULL, poll_interval_s = 2, last_error = NULL
                WHERE id = %s AND status = 'submitting' AND lease_owner = %s
                RETURNING *
                """,
                (provider_id, kind, credits, job["id"], owner),
            )
            row = await cur.fetchone()
    return Transition(applied=row is not None, job=row)


async def submit_failed(pool, job: dict, owner: str, error: str, delay_s: float | None,
                        count_try: bool = True) -> Transition:
    """delay_s=None means permanent. Retryable failures go back to queued with not_before."""
    give_up = delay_s is None or (count_try and job["tries"] >= settings.max_attempts)
    async with pool.connection() as conn:
        async with conn.transaction():
            if give_up:
                cur = await conn.execute(
                    """
                    UPDATE jobs SET status = 'failed', last_error = %s, finished_at = now(),
                        lease_owner = NULL, lease_until = NULL
                    WHERE id = %s AND status = 'submitting' AND lease_owner = %s RETURNING *
                    """,
                    (error[:500], job["id"], owner),
                )
                row = await cur.fetchone()
                if row:
                    await _outbox(conn, row, "job.failed")
                return Transition(applied=row is not None, job=row, released=row is not None)
            cur = await conn.execute(
                """
                UPDATE jobs SET status = 'queued', last_error = %s, lease_owner = NULL, lease_until = NULL,
                    not_before = now() + make_interval(secs => %s), enqueued_at = now(),
                    tries = tries - %s
                WHERE id = %s AND status = 'submitting' AND lease_owner = %s RETURNING *
                """,
                (error[:500], delay_s, 0 if count_try else 1, job["id"], owner),
            )
            row = await cur.fetchone()
    return Transition(applied=row is not None, job=row, requeue_delay_s=delay_s, released=row is not None)


# --------------------------------------------------------------------------- completion side

async def render_finished(pool, provider_id: str, status: str, result: dict | None, error: str | None,
                          backoff_s: float) -> Transition:
    """Called by the webhook handler and by the poller; whichever arrives first wins the CAS."""
    async with pool.connection() as conn:
        async with conn.transaction():
            cur = await conn.execute(
                "SELECT * FROM jobs WHERE provider_id = %s AND status = 'rendering' FOR UPDATE",
                (provider_id,),
            )
            job = await cur.fetchone()
            if job is None:
                return Transition(applied=False)
            await conn.execute(
                "UPDATE attempts SET outcome = %s WHERE provider_id = %s AND outcome IS NULL",
                ("complete" if status == "complete" else "error", provider_id),
            )
            if status == "complete":
                cur = await conn.execute(
                    """
                    UPDATE jobs SET status = 'succeeded', result = %s, finished_at = now()
                    WHERE id = %s RETURNING *
                    """,
                    (Jsonb(result or {}), job["id"]),
                )
                row = await cur.fetchone()
                await _outbox(conn, row, "job.succeeded")
                return Transition(applied=True, job=row, released=True)

            # Render error: Magic Hour refunds the credits; mirror that in the ledger, then retry
            # with a fresh attempt (new provider key) or dead-letter the job.
            await conn.execute(
                """
                INSERT INTO ledger (tenant_id, job_id, attempt, kind, credits)
                SELECT tenant_id, job_id, attempt, 'refund', -credits FROM ledger
                WHERE job_id = %s AND attempt = %s AND kind = 'charge'
                ON CONFLICT DO NOTHING
                """,
                (job["id"], job["attempt"]),
            )
            if job["attempt"] >= settings.max_attempts:
                cur = await conn.execute(
                    """
                    UPDATE jobs SET status = 'failed', last_error = %s, finished_at = now()
                    WHERE id = %s RETURNING *
                    """,
                    ((error or "render error")[:500], job["id"]),
                )
                row = await cur.fetchone()
                await _outbox(conn, row, "job.failed")
                return Transition(applied=True, job=row, released=True)
            cur = await conn.execute(
                """
                UPDATE jobs SET status = 'queued', last_error = %s, provider_key = NULL, provider_id = NULL,
                    not_before = now() + make_interval(secs => %s), enqueued_at = now()
                WHERE id = %s RETURNING *
                """,
                ((error or "render error")[:500], backoff_s, job["id"]),
            )
            row = await cur.fetchone()
            return Transition(applied=True, job=row, requeue_delay_s=backoff_s, released=True)


async def _outbox(conn, job: dict, event: str) -> None:
    body = {
        "event": event,
        "job_id": str(job["id"]),
        "tenant_id": job["tenant_id"],
        "idempotency_key": job["idempotency_key"],
        "status": job["status"],
        "result": job.get("result"),
        "error": job.get("last_error"),
    }
    await conn.execute(
        "INSERT INTO outbox (tenant_id, job_id, event, body) VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (job["tenant_id"], job["id"], event, Jsonb(body)),
    )


# --------------------------------------------------------------------------- reconciler side

async def reap_expired_leases(pool) -> list[dict]:
    """A worker died (or hung) mid-submit. The job goes back to queued with its provider key intact,
    so the replay is deduped upstream instead of creating a second paid render."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE jobs SET status = 'queued', lease_owner = NULL, lease_until = NULL,
                last_error = 'lease expired (worker lost)', enqueued_at = now()
            WHERE status = 'submitting' AND lease_until < now()
            RETURNING *
            """
        )
        return await cur.fetchall()


async def due_for_poll(pool, limit: int) -> list[dict]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            UPDATE jobs SET last_polled_at = now(),
                poll_interval_s = LEAST(poll_interval_s * 2, %(maxi)s)
            WHERE id IN (
                SELECT id FROM jobs
                WHERE status = 'rendering'
                  AND COALESCE(last_polled_at + make_interval(secs => poll_interval_s),
                               submitted_at + make_interval(secs => %(after)s)) <= now()
                ORDER BY submitted_at
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED)
            RETURNING id, provider_id, provider_kind, submitted_at, tenant_id
            """,
            {"maxi": settings.poll_max_interval_s, "after": settings.poll_after_s, "limit": limit},
        )
        return await cur.fetchall()


async def stale_queued(pool, older_than_s: float, limit: int = 20000) -> list[dict]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT id, tenant_id, priority, EXTRACT(EPOCH FROM not_before) AS not_before
            FROM jobs WHERE status = 'queued' AND enqueued_at < now() - make_interval(secs => %s)
            LIMIT %s
            """,
            (older_than_s, limit),
        )
        return await cur.fetchall()


async def inflight_counts(pool) -> dict[str, int]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT tenant_id, count(*) AS n FROM jobs WHERE status IN ('submitting','rendering') GROUP BY 1"
        )
        return {r["tenant_id"]: r["n"] for r in await cur.fetchall()}


async def claim_outbox(conn, limit: int) -> list[dict]:
    cur = await conn.execute(
        """
        SELECT * FROM outbox WHERE published_at IS NULL ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED
        """,
        (limit,),
    )
    return await cur.fetchall()


async def usage(pool, tenant_id: str) -> dict[str, Any]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            """
            SELECT COALESCE(SUM(credits) FILTER (WHERE kind = 'charge'), 0) AS charged,
                   COALESCE(SUM(credits) FILTER (WHERE kind = 'refund'), 0) AS refunded,
                   COALESCE(SUM(credits), 0) AS net
            FROM ledger WHERE tenant_id = %s
            """,
            (tenant_id,),
        )
        return await cur.fetchone()
