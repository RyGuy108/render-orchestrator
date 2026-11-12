"""Small run against the real Magic Hour API (spend is capped: N x 5 credits with z-image-turbo @ 640px).

Webhooks cannot reach a dev box, so this run exercises the polling fallback end to end.
It also replays every submission with the same Idempotency-Key to prove replays never create a
second paid render, and reconciles the ledger against what Magic Hour reports per project.

    MAGIC_HOUR_API_KEY=... python -m bench.real_run --jobs 12
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import statistics
import sys
import time

import httpx
import psycopg

from bench.loadtest import API, DB, LOGS, Procs, pct, reset_state, wait_http

PROMPTS = [
    "studio product photo of a ceramic coffee mug, soft light", "isometric pixel-art bakery storefront",
    "a red fox in fresh snow, telephoto", "minimal poster of a mountain at dawn, risograph",
    "macro photo of a dew-covered leaf", "a cozy reading nook with a cat, film photo",
]


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--jobs", type=int, default=12)
    a = ap.parse_args()
    key = os.environ.get("MAGIC_HOUR_API_KEY")
    if not key or key.startswith("mock"):
        sys.exit("set MAGIC_HOUR_API_KEY to a real key")
    os.makedirs(LOGS, exist_ok=True)
    reset_state()
    procs = Procs({
        "MH_BASE_URL": "https://api.magichour.ai", "MAGIC_HOUR_API_KEY": key,
        "MH_IDEMPOTENCY_HEADER": "false", "UPSTREAM_RPS": "2", "UPSTREAM_BURST": "4", "MAX_INFLIGHT": "6",
        "WORKERS": "2", "WORKER_CONCURRENCY": "3", "POLL_AFTER_S": "3", "POLL_MAX_INTERVAL_S": "8",
        "MH_TIMEOUT_S": "30", "LEASE_S": "45", "LOG_LEVEL": "WARNING",
    })
    procs.start("real-api", ["uvicorn", "orchestrator.api:app", "--port", "8000", "--log-level", "warning"])
    wait_http(f"{API}/healthz")
    procs.start("real-reconciler", [sys.executable, "-m", "orchestrator.reconciler"])
    procs.start("real-supervisor", [sys.executable, "-m", "orchestrator.supervisor"])
    await asyncio.sleep(2)

    t0 = time.time()
    ids, replays = [], 0
    async with httpx.AsyncClient(base_url=API, timeout=10) as c:
        for i in range(a.jobs):
            body = {"endpoint": "/v1/ai-image-generator", "priority": i % 2,
                    "payload": {"model": "z-image-turbo", "image_count": 1, "aspect_ratio": "1:1",
                                "resolution": "640px", "style": {"prompt": PROMPTS[i % len(PROMPTS)]}}}
            hdr = {"x-tenant-id": f"tenant-{i % 2}", "idempotency-key": f"real-{i}"}
            r = await c.post("/v1/jobs", json=body, headers=hdr)
            ids.append(r.json()["id"])
            r2 = await c.post("/v1/jobs", json=body, headers=hdr)   # client retry / double click
            replays += r2.status_code == 200 and r2.json()["id"] == ids[-1]

        while True:
            await asyncio.sleep(3)
            js = [(await c.get(f"/v1/jobs/{i}")).json() for i in ids]
            if all(j["status"] in ("succeeded", "failed") for j in js) or time.time() - t0 > 600:
                break
        metrics = (await c.get("/metrics")).text

    # Ground truth from Magic Hour itself
    with psycopg.connect(DB, autocommit=True) as conn:
        rows = conn.execute("""SELECT id, status, provider_id, provider_kind, credits_charged,
                                      EXTRACT(EPOCH FROM finished_at - created_at) AS lat, result
                               FROM jobs""").fetchall()
        ledger_net = -conn.execute("SELECT COALESCE(SUM(credits),0) FROM ledger").fetchone()[0]
        attempts = conn.execute("SELECT count(*) FROM attempts").fetchone()[0]
    provider_credits = 0
    async with httpx.AsyncClient(base_url="https://api.magichour.ai", timeout=30,
                                 headers={"Authorization": f"Bearer {key}"}) as mh:
        for _id, status, pid, kind, credits, lat, result in rows:
            if pid:
                p = (await mh.get(f"/v1/{kind}-projects/{pid}")).json()
                provider_credits += int(p.get("credits_charged") or 0)
    procs.stop_all()

    lat = [float(r[5]) for r in rows if r[1] == "succeeded"]
    out = {
        "run": "real-api", "jobs": a.jobs, "succeeded": sum(r[1] == "succeeded" for r in rows),
        "success_rate_pct": round(100 * sum(r[1] == "succeeded" for r in rows) / a.jobs, 1),
        "idempotent_replays_deduped": replays, "provider_projects_created": attempts,
        "latency_s": {"p50": pct(lat, .5), "p99": pct(lat, .99), "mean": round(statistics.mean(lat), 2) if lat else None},
        "wall_clock_s": round(time.time() - t0, 1),
        "ledger_net_credits": ledger_net, "provider_net_credits": provider_credits,
        "ledger_matches_provider": ledger_net == provider_credits,
        "sample_outputs": [r[6]["downloads"][0]["url"] for r in rows if r[6] and r[6].get("downloads")][:3],
        "metrics": [m for m in metrics.splitlines() if "upstream" in m or "poll" in m or "latency" in m],
    }
    os.makedirs("results", exist_ok=True)
    with open("results/real-api.json", "w") as f:
        json.dump(out, f, indent=2)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
