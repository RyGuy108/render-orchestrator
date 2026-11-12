"""Worker process: pull from the fair scheduler, claim in Postgres, submit to Magic Hour.

A worker only owns a job for the length of one submit call (guarded by a lease). Once Magic Hour
has accepted the render, completion is handled by webhooks and the reconciler, so killing a worker
never strands a render that is already running.

Run:  python -m orchestrator.worker --id w1
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import socket

import redis.asyncio as aioredis

from . import db
from .config import settings
from .provider import MagicHour, backoff, project_kind
from .scheduler import Scheduler, incr

log = logging.getLogger("worker")


class Worker:
    def __init__(self, wid: str):
        self.id = f"{socket.gethostname()}:{wid}:{os.getpid()}"
        self.stopping = asyncio.Event()

    async def run(self) -> None:
        self.pool = await db.make_pool(2, settings.worker_concurrency + 2)
        await db.init_schema(self.pool)
        self.r = aioredis.from_url(settings.redis_url)
        self.sched = Scheduler(self.r)
        await self.sched.init()
        self.mh = MagicHour(max_connections=settings.worker_concurrency * 2)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self.stopping.set)

        slots = [asyncio.create_task(self.slot(i)) for i in range(settings.worker_concurrency)]
        log.info("worker %s up with %d slots", self.id, len(slots))
        await self.stopping.wait()
        log.info("worker %s draining", self.id)
        # Graceful drain: slots finish their current submit (bounded by the HTTP timeout) and exit.
        await asyncio.wait(slots, timeout=settings.mh_timeout_s + 5)
        await self.mh.aclose()
        await self.pool.close()
        await self.r.aclose()

    async def slot(self, n: int) -> None:
        idle = 0.02
        while not self.stopping.is_set():
            try:
                kind, item = await self.sched.dequeue()
            except Exception as exc:  # Redis blip: back off, the reconciler will repair state
                log.warning("dequeue failed: %s", exc)
                await asyncio.sleep(1)
                continue
            if kind != "job":
                wait = item if kind == "pause" else idle
                idle = min(idle * 2, 0.25) if kind == "empty" else idle
                await asyncio.sleep(min(max(wait, 0.01), 2.0))
                continue
            idle = 0.02
            try:
                await self.process(item)
            except Exception:
                log.exception("slot %d crashed on %s", n, item)
                # Capacity is resynced from Postgres by the reconciler; the lease reaper recovers the job.

    async def process(self, item: dict) -> None:
        job = await db.claim(self.pool, item["job_id"], self.id)
        if job is None:  # stale index entry (already claimed, finished, or not yet due)
            await self.sched.release(item["tenant"])
            await incr(self.r, "stale_dequeues")
            return

        await self.sched.acquire_token()
        out = await self.mh.create(job["endpoint"], self._payload(job), job["provider_key"])
        await incr(self.r, f"upstream_create_{out.status or 'exc'}")

        if out.kind == "ok":
            await self.sched.on_upstream(throttled=False)
            body = out.body or {}
            t = await db.submit_succeeded(self.pool, job, self.id, str(body["id"]),
                                          int(body.get("credits_charged", 0)), project_kind(job["endpoint"]))
            await incr(self.r, "submits_ok" if t.applied else "submits_ok_late")
            return

        if out.kind == "throttled":
            # Global back-off: one 429 pauses every worker for Retry-After and shrinks the in-flight
            # limit, instead of N workers each hammering the API on their own schedule.
            await self.sched.on_upstream(throttled=True, retry_after=out.retry_after)
            delay = out.retry_after + backoff(1, 0.25, 1.0)
            t = await db.submit_failed(self.pool, job, self.id, "429 throttled", delay, count_try=False)
            await incr(self.r, "throttled")
        elif out.kind == "retryable":
            delay = max(out.retry_after, backoff(job["tries"]))
            t = await db.submit_failed(self.pool, job, self.id, out.error, delay)
            await incr(self.r, "retries")
        else:
            t = await db.submit_failed(self.pool, job, self.id, out.error, None)
            await incr(self.r, "permanent_failures")

        if t.released:
            await self.sched.release(job["tenant_id"])
        if t.applied and t.requeue_delay_s is not None:
            await self.sched.enqueue(str(job["id"]), job["tenant_id"], job["priority"], t.requeue_delay_s)

    @staticmethod
    def _payload(job: dict) -> dict:
        payload = dict(job["payload"])
        # Name the project after the job so it can be found in the Magic Hour dashboard / audited.
        payload.setdefault("name", f"orch-{job['id']}-a{job['attempt']}")
        return payload


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", default="w0")
    args = ap.parse_args()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(name)s %(message)s")
    asyncio.run(Worker(args.id).run())


if __name__ == "__main__":
    main()
