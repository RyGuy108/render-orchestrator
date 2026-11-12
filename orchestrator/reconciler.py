"""Reconciler: everything that makes the system converge after failures.

  * lease reaper     - jobs whose worker died mid-submit go back to the queue (same upstream key)
  * poller           - renders we have not heard about by webhook get polled, with backoff
  * outbox relay     - job events go out at-least-once (Redis stream + optional tenant callback)
  * index resync     - rebuilds Redis queues and in-flight counters from Postgres, so losing Redis
                       entirely (FLUSHALL, failover) only costs a few seconds

Only one reconciler is active at a time (Postgres advisory lock); start several for HA.
Run:  python -m orchestrator.reconciler
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time

import httpx
import psycopg
import redis.asyncio as aioredis

from . import db
from .config import settings
from .provider import MagicHour, backoff
from .scheduler import Scheduler, incr

log = logging.getLogger("reconciler")
LEADER_LOCK = 777_001


async def apply_render_result(pool, sched: Scheduler, r, provider_id: str, status: str,
                              result: dict | None, error: str | None) -> bool:
    """Shared by the webhook endpoint and the poller."""
    if status not in ("complete", "error", "canceled"):
        return False
    t = await db.render_finished(pool, provider_id, "complete" if status == "complete" else "error",
                                 result, error, backoff(2))
    if not t.applied:
        return False
    if t.released:
        await sched.release(t.job["tenant_id"])
    if t.requeue_delay_s is not None:
        await sched.enqueue(str(t.job["id"]), t.job["tenant_id"], t.job["priority"], t.requeue_delay_s)
        await incr(r, "render_retries")
    await incr(r, f"jobs_{t.job['status']}")
    return True


class Reconciler:
    def __init__(self):
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        # Leader election on a dedicated session-level connection.
        self.lock_conn = await psycopg.AsyncConnection.connect(settings.database_url, autocommit=True)
        while not self.stopping.is_set():
            cur = await self.lock_conn.execute("SELECT pg_try_advisory_lock(%s)", (LEADER_LOCK,))
            if (await cur.fetchone())[0]:
                break
            log.info("standby: another reconciler is leader")
            await asyncio.sleep(2)

        self.pool = await db.make_pool(2, 12)
        await db.init_schema(self.pool)
        self.r = aioredis.from_url(settings.redis_url)
        self.sched = Scheduler(self.r)
        await self.sched.init()
        self.mh = MagicHour(max_connections=32)
        self.http = httpx.AsyncClient(timeout=5)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stopping.set)

        log.info("reconciler is leader")
        tasks = [
            asyncio.create_task(self.every(1.0, self.reap)),
            asyncio.create_task(self.every(0.25, self.poll)),
            asyncio.create_task(self.every(0.1, self.relay)),
            asyncio.create_task(self.every(3.0, self.resync)),
        ]
        await self.stopping.wait()
        for t in tasks:
            t.cancel()
        await self.mh.aclose()
        await self.http.aclose()
        await self.pool.close()

    async def every(self, period: float, fn) -> None:
        while not self.stopping.is_set():
            t0 = time.monotonic()
            try:
                await fn()
            except Exception:
                log.exception("%s failed", fn.__name__)
            await asyncio.sleep(max(0.0, period - (time.monotonic() - t0)))

    # ------------------------------------------------------------------ lease reaper
    async def reap(self) -> None:
        for job in await db.reap_expired_leases(self.pool):
            await self.sched.release(job["tenant_id"])
            await self.sched.enqueue(str(job["id"]), job["tenant_id"], job["priority"])
            await incr(self.r, "leases_reaped")

    # ------------------------------------------------------------------ polling fallback
    async def poll(self) -> None:
        jobs = await db.due_for_poll(self.pool, limit=50)
        if not jobs:
            return

        async def one(job):
            if time.time() - job["submitted_at"].timestamp() > settings.render_timeout_s:
                await apply_render_result(self.pool, self.sched, self.r, job["provider_id"], "error", None,
                                          "render timed out")
                return
            await self.sched.acquire_token()
            out = await self.mh.get_project(job["provider_kind"], job["provider_id"])
            await incr(self.r, f"upstream_get_{out.status or 'exc'}")
            if out.kind == "throttled":
                await self.sched.on_upstream(throttled=True, retry_after=out.retry_after)
                return
            if out.kind != "ok":
                return  # next poll (with a longer interval) will try again
            body = out.body or {}
            status = body.get("status")
            if status in ("complete", "error", "canceled"):
                err = (body.get("error") or {}).get("message") if isinstance(body.get("error"), dict) else body.get("error")
                if await apply_render_result(self.pool, self.sched, self.r, job["provider_id"], status,
                                             {"downloads": body.get("downloads", [])}, err):
                    await incr(self.r, "completed_by_poll")

        await asyncio.gather(*(one(j) for j in jobs))

    # ------------------------------------------------------------------ outbox relay
    async def relay(self) -> None:
        async with self.pool.connection() as conn:
            async with conn.transaction():
                rows = await db.claim_outbox(conn, 200)
                if not rows:
                    return
                pipe = self.r.pipeline(transaction=False)
                for row in rows:
                    # Consumers dedupe on outbox id (at-least-once delivery).
                    pipe.xadd(f"events:{row['tenant_id']}", {"id": row["id"], "event": row["event"],
                                                             "job_id": str(row["job_id"])}, maxlen=100_000)
                await pipe.execute()
                callback = os.environ.get("TENANT_CALLBACK_URL")
                if callback:
                    await asyncio.gather(*(self.http.post(callback, json={"outbox_id": row["id"], **row["body"]})
                                           for row in rows), return_exceptions=True)
                await conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)",
                                   ([row["id"] for row in rows],))
        await incr(self.r, "events_published", len(rows))

    # ------------------------------------------------------------------ Redis index repair
    async def resync(self) -> None:
        await self.sched.resync_inflight(await db.inflight_counts(self.pool))
        await self.sched.init()  # restores rq:limit if Redis was wiped
        stale = await db.stale_queued(self.pool, older_than_s=5)
        if not stale:
            return
        missing = await self.sched.missing([str(j["id"]) for j in stale])
        now = time.time()
        n = 0
        for job, gone in zip(stale, missing):
            if gone:
                delay = max(0.0, float(job["not_before"]) - now)
                await self.sched.enqueue(str(job["id"]), job["tenant_id"], job["priority"], delay)
                n += 1
        if n:
            log.info("resync re-indexed %d queued jobs", n)
            await incr(self.r, "reindexed", n)


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(message)s")
    asyncio.run(Reconciler().run())


if __name__ == "__main__":
    main()
