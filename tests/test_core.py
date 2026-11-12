"""Invariant tests for the state machine, ledger and scheduler. Needs Postgres and Redis.

    TEST_DATABASE_URL=postgresql://postgres@localhost:5432/orch_test pytest -q
Uses Redis db 15 and wipes it.
"""
from __future__ import annotations

import asyncio
import os
import uuid

os.environ.setdefault("DATABASE_URL", os.environ.get("TEST_DATABASE_URL",
                                                     "postgresql://postgres@localhost:5432/orch_test"))
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/15")
os.environ.setdefault("MAX_ATTEMPTS", "3")

import psycopg  # noqa: E402
import pytest  # noqa: E402
import redis.asyncio as aioredis  # noqa: E402

from orchestrator import db  # noqa: E402
from orchestrator.provider import backoff, classify, parse_retry_after  # noqa: E402
from orchestrator.scheduler import Scheduler  # noqa: E402

DB = os.environ["DATABASE_URL"]


def setup_module():
    admin = DB.rsplit("/", 1)[0] + "/postgres"
    with psycopg.connect(admin, autocommit=True) as c:
        if not c.execute("SELECT 1 FROM pg_database WHERE datname='orch_test'").fetchone():
            c.execute("CREATE DATABASE orch_test")
    with psycopg.connect(DB, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS ledger, attempts, outbox, jobs CASCADE")


@pytest.fixture
def env():
    async def make():
        pool = await db.make_pool(2, 20)
        await db.init_schema(pool)
        r = aioredis.from_url(os.environ["REDIS_URL"])
        await r.flushdb()
        s = Scheduler(r)
        await s.init()
        return pool, r, s
    return make


def run(coro):
    return asyncio.run(coro)


async def new_job(pool, tenant="t1", key=None):
    job, _ = await db.create_job(pool, tenant, key or uuid.uuid4().hex, "/v1/ai-image-generator",
                                 {"model": "z-image-turbo"}, 1)
    return job


# ----------------------------------------------------------------------------- idempotency

def test_idempotent_intake(env):
    async def t():
        pool, r, _ = await env()
        a, created_a = await db.create_job(pool, "t1", "same", "/v1/ai-image-generator", {"x": 1}, 1)
        b, created_b = await db.create_job(pool, "t1", "same", "/v1/ai-image-generator", {"x": 1}, 1)
        assert created_a and not created_b and a["id"] == b["id"]
        with pytest.raises(db.IdempotencyConflict):
            await db.create_job(pool, "t1", "same", "/v1/ai-image-generator", {"x": 2}, 1)
        c, created_c = await db.create_job(pool, "t2", "same", "/v1/ai-image-generator", {"x": 1}, 1)
        assert created_c and c["id"] != a["id"]  # keys are scoped per tenant
        await pool.close(); await r.aclose()
    run(t())


def test_concurrent_claims_have_one_winner(env):
    async def t():
        pool, r, _ = await env()
        job = await new_job(pool)
        wins = await asyncio.gather(*(db.claim(pool, job["id"], f"w{i}") for i in range(10)))
        assert sum(1 for w in wins if w) == 1
        await pool.close(); await r.aclose()
    run(t())


def test_ambiguous_failure_reuses_provider_key(env):
    async def t():
        pool, r, _ = await env()
        job = await new_job(pool)
        c1 = await db.claim(pool, job["id"], "w1")
        await db.submit_failed(pool, c1, "w1", "timeout", 0.0)
        c2 = await db.claim(pool, job["id"], "w2")
        assert c2["provider_key"] == c1["provider_key"] and c2["attempt"] == 1
        await pool.close(); await r.aclose()
    run(t())


# ----------------------------------------------------------------------------- exactly-once money

def test_webhook_and_poll_race_charges_once(env):
    async def t():
        pool, r, _ = await env()
        job = await new_job(pool)
        c = await db.claim(pool, job["id"], "w1")
        await db.submit_succeeded(pool, c, "w1", "p-1", 5, "image")
        # a late duplicate success from a worker whose lease was reaped
        await db.submit_succeeded(pool, c, "w-old", "p-1", 5, "image")
        res = await asyncio.gather(*(db.render_finished(pool, "p-1", "complete", {}, None, 0) for _ in range(8)))
        assert sum(1 for x in res if x.applied) == 1
        async with pool.connection() as conn:
            rows = await (await conn.execute("SELECT kind, credits FROM ledger WHERE job_id=%s", (job["id"],))).fetchall()
            ev = await (await conn.execute("SELECT count(*) AS n FROM outbox WHERE job_id=%s", (job["id"],))).fetchone()
        assert rows == [{"kind": "charge", "credits": -5}] and ev["n"] == 1
        await pool.close(); await r.aclose()
    run(t())


def test_render_error_refunds_and_retries_with_new_key(env):
    async def t():
        pool, r, _ = await env()
        job = await new_job(pool, tenant="refund-tenant")
        c = await db.claim(pool, job["id"], "w1")
        await db.submit_succeeded(pool, c, "w1", "p-err", 50, "image")
        tr = await db.render_finished(pool, "p-err", "error", None, "boom", 0)
        await db.render_finished(pool, "p-err", "error", None, "boom", 0)  # duplicate webhook
        assert tr.job["status"] == "queued" and tr.job["provider_key"] is None
        c2 = await db.claim(pool, job["id"], "w1")
        assert c2["attempt"] == 2 and c2["provider_key"] != c["provider_key"]
        await db.submit_succeeded(pool, c2, "w1", "p-ok", 50, "image")
        await db.render_finished(pool, "p-ok", "complete", {}, None, 0)
        u = await db.usage(pool, "refund-tenant")
        assert (u["charged"], u["refunded"], u["net"]) == (-100, 50, -50)
        await pool.close(); await r.aclose()
    run(t())


def test_expired_lease_is_reaped(env):
    async def t():
        pool, r, _ = await env()
        job = await new_job(pool)
        await db.claim(pool, job["id"], "dead-worker")
        async with pool.connection() as conn:
            await conn.execute("UPDATE jobs SET lease_until = now() - interval '1 s' WHERE id=%s", (job["id"],))
        reaped = await db.reap_expired_leases(pool)
        assert [str(x["id"]) for x in reaped] == [str(job["id"])]
        late = await db.submit_succeeded(pool, {**reaped[0], "attempt": 1, "provider_key": reaped[0]["provider_key"]},
                                         "dead-worker", "p-late", 5, "image")
        assert not late.applied  # the dead worker can no longer move the job, but its charge is recorded
        await pool.close(); await r.aclose()
    run(t())


# ----------------------------------------------------------------------------- scheduler

def test_fair_queue_interleaves_tenants(env):
    async def t():
        pool, r, s = await env()
        for i in range(50):
            await s.enqueue(f"big-{i}", "big", 1)
        for i in range(5):
            await s.enqueue(f"small-{i}", "small", 1)
        order = []
        for _ in range(10):
            _, item = await s.dequeue()
            order.append(item["tenant"])
            await s.release(item["tenant"])
        # the 5 small jobs are not stuck behind 50 big ones: they alternate
        assert order[:10].count("small") == 5
        await pool.close(); await r.aclose()
    run(t())


def test_priority_lanes_with_starvation_guard(env):
    async def t():
        pool, r, s = await env()
        for i in range(20):
            await s.enqueue(f"b{i}", "t", 2)
            await s.enqueue(f"i{i}", "u", 0)
        lanes = []
        for _ in range(10):
            _, item = await s.dequeue()
            lanes.append(item["lane"])
            await s.release(item["tenant"])
        assert lanes.count(0) == 8 and lanes.count(2) == 2  # every 5th pick serves the batch lane
        await pool.close(); await r.aclose()
    run(t())


def test_tenant_cap_and_dedup(env):
    async def t():
        pool, r, s = await env()
        assert await s.enqueue("j1", "t", 1) and not await s.enqueue("j1", "t", 1)
        await r.hset("rq:inflight", "t", 10_000)
        kind, _ = await s.dequeue()
        assert kind == "empty"  # tenant at its cap is skipped
        await pool.close(); await r.aclose()
    run(t())


def test_aimd_and_pause(env):
    async def t():
        pool, r, s = await env()
        start = float(await r.get("rq:limit"))
        lim = await s.on_upstream(throttled=True, retry_after=1)
        assert lim == pytest.approx(start * 0.75)
        assert await s.on_upstream(throttled=True, retry_after=1) == lim  # one decrease per second
        kind, wait = await s.dequeue()
        assert kind == "pause" and 0 < wait <= 1
        await pool.close(); await r.aclose()
    run(t())


# ----------------------------------------------------------------------------- pure functions

def test_classify_and_backoff():
    import httpx
    mk = lambda code, h=None: httpx.Response(code, json={}, headers=h or {})
    assert classify(mk(200), None).kind == "ok"
    assert classify(mk(429, {"retry-after": "3"}), None).retry_after == 3
    assert classify(mk(503), None).kind == "retryable"
    assert classify(mk(422), None).kind == "permanent"
    assert classify(None, httpx.ReadTimeout("t")).kind == "retryable"
    assert parse_retry_after("garbage", 2.0) == 2.0
    assert all(0 <= backoff(n, 0.5, 30) <= min(30, 0.5 * 2 ** (n - 1)) for n in range(1, 12))
