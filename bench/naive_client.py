"""The "before": a typical direct integration with the Magic Hour API.

What a reasonable first version looks like (and what most API customers ship):
  * one FIFO queue, a fixed pool of 64 concurrent jobs (tuned to the provider's concurrency cap)
  * create -> poll every 2 s until complete/error
  * up to 3 retries on any failed create, exponential backoff (1, 2, 4 s), Retry-After ignored
  * a render error is reported as a failure
  * progress lives in memory and in an append-only results file; after a crash it restarts and
    re-runs every job that has no result yet

Run by bench/loadtest.py, which also kills it mid-run in the crash scenario.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import time

import httpx

from bench.workload import scaled

CONCURRENCY = int(os.environ.get("NAIVE_CONCURRENCY", 64))


async def run(base: str, results_path: str, t0: float, scale: float) -> None:
    done = set()
    if os.path.exists(results_path):
        with open(results_path) as f:
            done = {json.loads(line)["key"] for line in f if line.strip()}
    jobs = [j for j in scaled(scale) if j["key"] not in done]
    out = open(results_path, "a", buffering=1)
    client = httpx.AsyncClient(base_url=base, headers={"Authorization": "Bearer mock"},
                               timeout=httpx.Timeout(float(os.environ.get("MH_TIMEOUT_S", 10)), connect=5),
                               limits=httpx.Limits(max_connections=CONCURRENCY * 2))
    q: asyncio.Queue = asyncio.Queue()

    async def feeder():
        for j in jobs:
            delay = t0 + j["at"] - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            await q.put(j)
        for _ in range(CONCURRENCY):
            await q.put(None)

    async def one(j) -> tuple[str, str]:
        payload = dict(j["payload"], name=f"naive-{j['key']}-a1")
        pid = None
        for attempt in range(4):
            try:
                r = await client.post(j["endpoint"], json=payload)
                if r.status_code == 200:
                    pid = r.json()["id"]
                    break
                err = f"HTTP {r.status_code}"
                if 400 <= r.status_code < 500 and r.status_code != 429:
                    return "failed", err
            except httpx.HTTPError as e:
                err = type(e).__name__
            if attempt < 3:
                await asyncio.sleep(2 ** attempt)
        if pid is None:
            return "failed", err
        while True:
            await asyncio.sleep(2)
            try:
                r = await client.get(f"/v1/image-projects/{pid}")
            except httpx.HTTPError:
                continue
            if r.status_code != 200:
                continue
            st = r.json()["status"]
            if st == "complete":
                return "succeeded", ""
            if st in ("error", "canceled"):
                return "failed", "render error"

    async def consumer():
        while True:
            j = await q.get()
            if j is None:
                return
            status, err = await one(j)
            out.write(json.dumps({"key": j["key"], "tenant": j["tenant"], "status": status, "error": err,
                                  "arrival": t0 + j["at"], "finished": time.time()}) + "\n")

    await asyncio.gather(feeder(), *(consumer() for _ in range(CONCURRENCY)))
    await client.aclose()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://127.0.0.1:9000")
    ap.add_argument("--results", required=True)
    ap.add_argument("--t0", type=float, required=True)
    ap.add_argument("--scale", type=float, default=1.0)
    a = ap.parse_args()
    asyncio.run(run(a.base, a.results, a.t0, a.scale))
