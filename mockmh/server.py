"""Fault-injecting mock of the Magic Hour API, for load tests at a scale (and failure rate) you
would not want to pay for.

Models the behaviour that matters to a client:
  * request-rate limit (token bucket) and a cap on concurrent renders, both answered with 429 + Retry-After
  * 5xx before creating a project (clean failure) and 5xx/timeouts AFTER creating it (ambiguous failure:
    the project exists and is billed, but the client never learns its id)
  * render latency (lognormal), render errors with credit refunds
  * webhooks (`image.started` / `image.completed` / `image.errored`) that are dropped, duplicated and reordered
  * optional Idempotency-Key support, to show what it buys
  * a billing log, so a run can be audited for double charges

Run:  uvicorn mockmh.server:app --port 9000
Admin: POST /_admin/reset {config overrides}, GET /_admin/stats
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import math
import os
import random
import time
import uuid
from collections import Counter, defaultdict

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

DEFAULTS = {
    "rps": 40.0,                 # account-wide request rate
    "burst": 40,
    "max_active": 64,            # concurrent queued+rendering projects per account
    "p_5xx_clean": 0.02,         # 503 before anything is created
    "p_5xx_ambiguous": 0.02,     # project created + billed, then 502 returned
    "p_hang": 0.01,              # project created + billed, response never arrives (client times out)
    "hang_s": 30.0,
    "p_get_5xx": 0.02,
    "p_render_error": 0.03,
    "render_median_s": 1.5,
    "render_sigma": 0.5,
    "queue_delay_s": 0.2,
    "webhook_url": "",
    "webhook_secret": "dev-secret",
    "p_webhook_drop": 0.10,
    "p_webhook_dup": 0.05,
    "webhook_max_delay_s": 1.0,
    "honor_idempotency": True,
    "seed": 7,
}

CREDITS = {"z-image-turbo": 5, "flux-schnell": 5, "flux-2-klein": 5, "nano-banana-2": 50, "seedream-v4.5": 50}


class State:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.rng = random.Random(cfg["seed"])
        self.tokens = cfg["burst"]
        self.ts = time.monotonic()
        self.projects: dict[str, dict] = {}
        self.idem: dict[str, dict] = {}
        self.active = 0
        self.counts = Counter()
        self.by_name: dict[str, list[str]] = defaultdict(list)
        self.tasks: set[asyncio.Task] = set()

    def take_token(self) -> bool:
        now = time.monotonic()
        self.tokens = min(self.cfg["burst"], self.tokens + (now - self.ts) * self.cfg["rps"])
        self.ts = now
        if self.tokens >= 1:
            self.tokens -= 1
            return True
        return False


app = FastAPI(title="mock Magic Hour")
S = State(dict(DEFAULTS, webhook_url=os.environ.get("MOCK_WEBHOOK_URL", "")))
_http: httpx.AsyncClient | None = None


def http() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=5, limits=httpx.Limits(max_connections=200))
    return _http


def throttled(reason: str) -> JSONResponse:
    S.counts["429"] += 1
    S.counts[f"429_{reason}"] += 1
    return JSONResponse({"message": "Too many requests"}, status_code=429, headers={"Retry-After": "1"})


@app.post("/_admin/reset")
async def reset(request: Request):
    global S
    for t in list(S.tasks):
        t.cancel()
    overrides = await request.json() if (await request.body()) else {}
    S = State(dict(DEFAULTS, webhook_url=os.environ.get("MOCK_WEBHOOK_URL", ""), **overrides))
    return S.cfg


@app.get("/_admin/stats")
async def stats():
    charged = sum(p["credits"] for p in S.projects.values())
    refunded = sum(p["credits"] for p in S.projects.values() if p["status"] == "error")
    # A "duplicate paid render" = more than one non-refunded project for the same logical request.
    dup_names = {n: ids for n, ids in S.by_name.items()
                 if sum(1 for i in ids if S.projects[i]["status"] != "error") > 1}
    statuses = Counter(p["status"] for p in S.projects.values())
    return {
        "counts": dict(S.counts), "projects": len(S.projects), "statuses": dict(statuses),
        "credits_charged": charged, "credits_refunded": refunded, "credits_net": charged - refunded,
        "duplicate_paid_renders": sum(len(v) - 1 for v in dup_names.values()),
        "logical_requests_double_charged": len(dup_names),
        "wasted_credits": sum((len(ids) - 1) * S.projects[ids[0]]["credits"] for ids in dup_names.values()),
        "active": S.active,
        "billing_by_name": None,
    }


@app.get("/_admin/billing")
async def billing():
    """Per-project billing log, for reconciling the orchestrator's ledger."""
    return [{"id": p["id"], "name": p["name"], "credits": p["credits"], "status": p["status"]}
            for p in S.projects.values()]


def _auth_ok(request: Request) -> bool:
    return request.headers.get("authorization", "").startswith("Bearer ")


@app.get("/v1/{kind}-projects/{pid}")
async def get_project(kind: str, pid: str, request: Request):
    S.counts["get"] += 1
    if not S.take_token():
        return throttled("rate")
    if S.rng.random() < S.cfg["p_get_5xx"]:
        S.counts["get_5xx"] += 1
        return JSONResponse({"message": "upstream error"}, status_code=500)
    p = S.projects.get(pid)
    if not p:
        return JSONResponse({"message": "not found"}, status_code=404)
    return _project_view(p)


def _project_view(p: dict) -> dict:
    return {
        "id": p["id"], "name": p["name"], "status": p["status"], "credits_charged": p["credits"],
        "type": p["kind"].upper(),
        "downloads": [{"url": f"https://mock.magichour.ai/{p['id']}.png", "expires_at": "2030-01-01T00:00:00Z"}]
        if p["status"] == "complete" else [],
        "error": {"code": "render_failed", "message": "Render failed"} if p["status"] == "error" else None,
    }


@app.post("/v1/{tool}")
async def create(tool: str, request: Request):
    S.counts["create"] += 1
    if not _auth_ok(request):
        return JSONResponse({"message": "unauthorized"}, status_code=401)
    if not S.take_token():
        return throttled("rate")
    body = await request.json()
    key = request.headers.get("idempotency-key")
    if key and S.cfg["honor_idempotency"] and key in S.idem:
        S.counts["idempotent_replay"] += 1
        return S.idem[key]
    if S.active >= S.cfg["max_active"]:
        return throttled("concurrency")
    if S.rng.random() < S.cfg["p_5xx_clean"]:
        S.counts["5xx_clean"] += 1
        return JSONResponse({"message": "service unavailable"}, status_code=503)

    kind = "image" if "image" in tool or tool in ("face-swap-photo", "head-swap") else "video"
    pid = "cm" + uuid.uuid4().hex[:22]
    p = {"id": pid, "name": body.get("name", ""), "status": "queued", "kind": kind,
         "credits": CREDITS.get(body.get("model"), 50), "created": time.time()}
    S.projects[pid] = p
    S.by_name[p["name"]].append(pid)
    S.active += 1
    resp = {"id": pid, "credits_charged": p["credits"], "frame_cost": p["credits"]}
    if key and S.cfg["honor_idempotency"]:
        S.idem[key] = resp
    t = asyncio.create_task(_render(p))
    S.tasks.add(t)
    t.add_done_callback(S.tasks.discard)

    roll = S.rng.random()
    if roll < S.cfg["p_5xx_ambiguous"]:
        S.counts["5xx_ambiguous"] += 1
        return JSONResponse({"message": "bad gateway"}, status_code=502)
    if roll < S.cfg["p_5xx_ambiguous"] + S.cfg["p_hang"]:
        S.counts["hang"] += 1
        await asyncio.sleep(S.cfg["hang_s"])
    return resp


async def _render(p: dict) -> None:
    cfg = S.cfg
    try:
        await asyncio.sleep(cfg["queue_delay_s"] * S.rng.random())
        p["status"] = "rendering"
        await _webhook(p, "started")
        dur = cfg["render_median_s"] * math.exp(cfg["render_sigma"] * S.rng.gauss(0, 1))
        await asyncio.sleep(dur)
        failed = S.rng.random() < cfg["p_render_error"]
        p["status"] = "error" if failed else "complete"
        p["finished"] = time.time()
        S.active -= 1
        S.counts["render_error" if failed else "render_complete"] += 1
        await _webhook(p, "errored" if failed else "completed")
    except asyncio.CancelledError:
        pass


async def _webhook(p: dict, what: str) -> None:
    cfg = S.cfg
    if not cfg["webhook_url"]:
        return
    if S.rng.random() < cfg["p_webhook_drop"]:
        S.counts["webhook_dropped"] += 1
        return
    n = 2 if S.rng.random() < cfg["p_webhook_dup"] else 1
    raw = json.dumps({"type": f"{p['kind']}.{what}", "payload": _project_view(p)}).encode()
    sig = hmac.new(cfg["webhook_secret"].encode(), raw, hashlib.sha256).hexdigest()

    async def deliver():
        await asyncio.sleep(S.rng.random() * cfg["webhook_max_delay_s"])  # reordering
        try:
            await http().post(cfg["webhook_url"], content=raw,
                              headers={"content-type": "application/json", "x-mh-signature": sig})
            S.counts["webhook_sent"] += 1
        except Exception:
            S.counts["webhook_failed"] += 1

    for _ in range(n):
        t = asyncio.create_task(deliver())
        S.tasks.add(t)
        t.add_done_callback(S.tasks.discard)
