"""HTTP front door.

  POST /v1/jobs                      submit a render (Idempotency-Key header required)
  GET  /v1/jobs/{id}                 job status / result
  GET  /v1/tenants/{tenant}/usage    credits from the ledger
  POST /webhooks/magic-hour          Magic Hour completion webhooks (HMAC-verified)
  GET  /metrics                      Prometheus text format
Run:  uvicorn orchestrator.api:app
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from contextlib import asynccontextmanager

import redis.asyncio as aioredis
from fastapi import FastAPI, Header, HTTPException, Request, Response
from pydantic import BaseModel, Field

from . import db
from .config import settings
from .reconciler import apply_render_result
from .scheduler import Scheduler, incr


class JobIn(BaseModel):
    endpoint: str = Field(examples=["/v1/ai-image-generator"], pattern=r"^/v1/[a-z0-9\-]+$")
    payload: dict
    priority: int = Field(1, ge=0, le=2, description="0 interactive, 1 standard, 2 batch")


def job_out(row: dict) -> dict:
    return {
        "id": str(row["id"]), "tenant_id": row["tenant_id"], "idempotency_key": row["idempotency_key"],
        "status": row["status"], "priority": row["priority"], "attempt": row["attempt"],
        "provider_id": row["provider_id"], "credits_charged": row["credits_charged"],
        "result": row["result"], "error": row["last_error"],
        "created_at": row["created_at"].isoformat(),
        "finished_at": row["finished_at"].isoformat() if row["finished_at"] else None,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.pool = await db.make_pool(4, 40)
    await db.init_schema(app.state.pool)
    app.state.r = aioredis.from_url(settings.redis_url)
    app.state.sched = Scheduler(app.state.r)
    await app.state.sched.init()
    yield
    await app.state.pool.close()
    await app.state.r.aclose()


app = FastAPI(title="Magic Hour render orchestrator", lifespan=lifespan)


@app.post("/v1/jobs", status_code=201)
async def submit(job: JobIn, response: Response, request: Request,
                 x_tenant_id: str = Header(..., description="stand-in for real auth"),
                 idempotency_key: str = Header(..., max_length=200)):
    try:
        row, created = await db.create_job(request.app.state.pool, x_tenant_id, idempotency_key,
                                           job.endpoint, job.payload, job.priority)
    except db.IdempotencyConflict as exc:
        raise HTTPException(409, f"Idempotency-Key reused with a different body (job {exc})")
    if created:
        await request.app.state.sched.enqueue(str(row["id"]), x_tenant_id, job.priority)
        await incr(request.app.state.r, "jobs_accepted")
    else:
        response.status_code = 200
        await incr(request.app.state.r, "idempotent_replays")
    return job_out(row)


@app.get("/v1/jobs/{job_id}")
async def get_job(job_id: str, request: Request):
    row = await db.get_job(request.app.state.pool, job_id)
    if not row:
        raise HTTPException(404)
    return job_out(row)


@app.get("/v1/tenants/{tenant_id}/usage")
async def usage(tenant_id: str, request: Request):
    return await db.usage(request.app.state.pool, tenant_id)


@app.post("/webhooks/magic-hour")
async def webhook(request: Request, x_mh_signature: str | None = Header(None)):
    raw = await request.body()
    if settings.webhook_secret and x_mh_signature is not None:
        expected = hmac.new(settings.webhook_secret.encode(), raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, x_mh_signature):
            raise HTTPException(401, "bad signature")
    evt = json.loads(raw)
    r = request.app.state.r
    await incr(r, "webhooks_received")
    etype = evt.get("type", "")                  # e.g. image.completed / video.errored / image.started
    data = evt.get("payload") or {}
    status = data.get("status") or {"completed": "complete", "errored": "error"}.get(etype.split(".")[-1])
    if status not in ("complete", "error", "canceled"):
        return {"ok": True, "ignored": etype}
    err = data.get("error")
    err = err.get("message") if isinstance(err, dict) else err
    applied = await apply_render_result(request.app.state.pool, request.app.state.sched, r, str(data.get("id")),
                                        status, {"downloads": data.get("downloads", [])}, err)
    # Duplicates, late and out-of-order deliveries are expected and harmless: the CAS just misses.
    await incr(r, "webhooks_applied" if applied else "webhooks_noop")
    return {"ok": True, "applied": applied}


@app.get("/metrics")
async def metrics(request: Request):
    r = request.app.state.r
    counters = {k.decode(): int(v) for k, v in (await r.hgetall("metrics")).items()}
    lines = []
    for k, v in sorted(counters.items()):
        lines.append(f"orch_{k}_total {v}")
    async with request.app.state.pool.connection() as conn:
        cur = await conn.execute("SELECT status, count(*) AS n FROM jobs GROUP BY 1")
        for row in await cur.fetchall():
            lines.append(f'orch_jobs{{status="{row["status"]}"}} {row["n"]}')
        cur = await conn.execute(
            """
            SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (
                     ORDER BY EXTRACT(EPOCH FROM finished_at - created_at)) AS p
            FROM jobs WHERE status = 'succeeded' AND finished_at > now() - interval '5 minutes'
            """
        )
        p = (await cur.fetchone())["p"]
        if p:
            for q, v in zip(("0.5", "0.95", "0.99"), p):
                lines.append(f'orch_job_latency_seconds{{quantile="{q}"}} {v:.3f}')
    lines.append(f"orch_inflight_limit {float(await r.get('rq:limit') or 0):.2f}")
    lines.append(f"orch_inflight {int(await r.get('rq:inflight_total') or 0)}")
    lines.append(f"orch_paused_seconds {max(0.0, float(await r.get('rq:pause_until') or 0) - time.time()):.2f}")
    return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")


@app.get("/healthz")
async def healthz():
    return {"ok": True}
