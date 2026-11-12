"""Load test + chaos harness. Starts a fresh mock Magic Hour, Postgres schema and Redis per run.

  python -m bench.loadtest --system naive              # baseline
  python -m bench.loadtest --system orch               # orchestrator
  python -m bench.loadtest --system orch --chaos       # + kill -9 workers / reconciler / API, FLUSHALL Redis
  python -m bench.loadtest --system naive --chaos      # + kill -9 the client once and restart it

Writes results/<name>.json with success rate, throughput, latency percentiles per tenant class,
upstream 429s, duplicate paid renders and the ledger-vs-provider reconciliation.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import statistics
import subprocess
import sys
import time

import httpx
import psycopg
import redis

from bench.workload import scaled

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB = os.environ.get("DATABASE_URL", "postgresql://postgres@localhost:5432/orch")
LOGS = os.environ.get("LOG_DIR", "/tmp/orch-logs")
MOCK = "http://127.0.0.1:9000"
API = "http://127.0.0.1:8000"

ORCH_ENV = {
    "MH_BASE_URL": MOCK,
    "MAGIC_HOUR_API_KEY": "mock-key",   # never send the real key to the mock
    "MAX_INFLIGHT": "96",               # deliberately above the provider's cap: AIMD has to find it
    "UPSTREAM_RPS": "38",
    "UPSTREAM_BURST": "20",
    "MH_TIMEOUT_S": "10",
    "POLL_AFTER_S": "5",
    "LEASE_S": "20",
    "WORKERS": "4",
    "WORKER_CONCURRENCY": "16",
    "TENANT_MAX_INFLIGHT": "48",
    "LOG_LEVEL": "WARNING",
}


def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = (len(xs) - 1) * q
    lo, hi = int(k), min(int(k) + 1, len(xs) - 1)
    return round(xs[lo] + (xs[hi] - xs[lo]) * (k - lo), 2)


class Procs:
    def __init__(self, env):
        self.env = dict(os.environ, **env)
        self.p: dict[str, subprocess.Popen] = {}

    def start(self, name, args, extra_env=None):
        log = open(f"{LOGS}/{name}.log", "a")
        self.p[name] = subprocess.Popen(args, cwd=ROOT, env=dict(self.env, **(extra_env or {})),
                                        stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
        return self.p[name]

    def kill9(self, name):
        p = self.p.get(name)
        if p and p.poll() is None:
            os.killpg(p.pid, signal.SIGKILL)   # whole process group: supervisor + its workers
            p.wait()

    def stop_all(self):
        for name in list(self.p):
            p = self.p[name]
            if p.poll() is None:
                os.killpg(p.pid, signal.SIGTERM)
        time.sleep(1)
        for name in list(self.p):
            self.kill9(name)


def wait_http(url, timeout=20):
    t = time.time()
    while time.time() - t < timeout:
        try:
            if httpx.get(url, timeout=1).status_code < 500:
                return
        except Exception:
            pass
        time.sleep(0.2)
    raise RuntimeError(f"{url} did not come up")


def reset_state():
    with psycopg.connect(DB, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS ledger, attempts, outbox, jobs CASCADE")
    redis.Redis().flushall()


def worker_pids(sup_pid):
    out = subprocess.run(["pgrep", "-P", str(sup_pid)], capture_output=True, text=True).stdout.split()
    return [int(x) for x in out]


# ---------------------------------------------------------------------------- orchestrator run

async def submit_all(jobs, t0, keymap):
    """Client side: submit on schedule, retry submits with the same Idempotency-Key (API restarts are safe)."""
    async with httpx.AsyncClient(base_url=API, timeout=5, limits=httpx.Limits(max_connections=200)) as c:
        # Each tenant is its own client with its own connections, so the 7,000-job backfill does
        # not head-of-line block the small tenants' submissions inside this harness.
        sems: dict[str, asyncio.Semaphore] = {}

        async def one(j):
            delay = t0 + j["at"] - time.time()
            if delay > 0:
                await asyncio.sleep(delay)
            sem = sems.setdefault(j["tenant"], asyncio.Semaphore(32 if j["tenant"] == "bulk-co" else 4))
            async with sem:
                for i in range(60):
                    try:
                        r = await c.post("/v1/jobs", json={"endpoint": j["endpoint"], "payload": j["payload"],
                                                           "priority": j["priority"]},
                                         headers={"x-tenant-id": j["tenant"], "idempotency-key": j["key"]})
                        if r.status_code in (200, 201):
                            keymap[(j["tenant"], j["key"])] = j
                            return
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(min(0.25 * 2 ** i, 2))
        await asyncio.gather(*(one(j) for j in jobs))


async def chaos_orch(procs: Procs, t0, stop: asyncio.Event, log):
    rng = random.Random(3)
    events = [(60, "kill_reconciler"), (100, "restart_api"), (130, "flush_redis"), (170, "kill_pool")]
    next_worker_kill = 15
    while not stop.is_set():
        el = time.time() - t0
        if events and el >= events[0][0]:
            _, what = events.pop(0)
            log.append({"t": round(el, 1), "event": what})
            if what == "kill_reconciler":
                procs.kill9("reconciler")
                await asyncio.sleep(5)
                procs.start("reconciler", [sys.executable, "-m", "orchestrator.reconciler"])
            elif what == "restart_api":
                procs.kill9("api")
                procs.start("api", ["uvicorn", "orchestrator.api:app", "--port", "8000", "--log-level", "warning"])
            elif what == "flush_redis":
                redis.Redis().flushall()   # lose every queue, counter and rate-limit key
            elif what == "kill_pool":
                procs.kill9("supervisor")  # all workers die mid-submit at once
                await asyncio.sleep(3)
                procs.start("supervisor", [sys.executable, "-m", "orchestrator.supervisor"])
        if el >= next_worker_kill and "supervisor" in procs.p:
            pids = worker_pids(procs.p["supervisor"].pid)
            if pids:
                victim = rng.choice(pids)
                os.kill(victim, signal.SIGKILL)
                log.append({"t": round(el, 1), "event": "kill_worker", "pid": victim})
            next_worker_kill += 15
        await asyncio.sleep(0.5)


async def run_orch(jobs, chaos: bool):
    procs = Procs(ORCH_ENV)
    procs.start("mock", ["uvicorn", "mockmh.server:app", "--port", "9000", "--log-level", "warning"],
                {"MOCK_WEBHOOK_URL": f"{API}/webhooks/magic-hour"})
    wait_http(f"{MOCK}/_admin/stats")
    httpx.post(f"{MOCK}/_admin/reset", json={"webhook_url": f"{API}/webhooks/magic-hour", "hang_s": 15})
    procs.start("api", ["uvicorn", "orchestrator.api:app", "--port", "8000", "--log-level", "warning"])
    wait_http(f"{API}/healthz")
    procs.start("reconciler", [sys.executable, "-m", "orchestrator.reconciler"])
    procs.start("supervisor", [sys.executable, "-m", "orchestrator.supervisor"])
    await asyncio.sleep(3)

    t0 = time.time() + 1
    keymap: dict = {}
    chaos_log: list = []
    stop = asyncio.Event()
    chaos_task = asyncio.create_task(chaos_orch(procs, t0, stop, chaos_log)) if chaos else None
    sub = asyncio.create_task(submit_all(jobs, t0, keymap))

    limit_trace = []
    with psycopg.connect(DB, autocommit=True) as c:
        while True:
            await asyncio.sleep(2)
            n = c.execute("SELECT count(*) FROM jobs WHERE status IN ('succeeded','failed')").fetchone()[0]
            try:
                lim = float(redis.Redis().get("rq:limit") or 0)
            except Exception:
                lim = None
            limit_trace.append({"t": round(time.time() - t0, 1), "done": n, "limit": lim})
            if sub.done() and n >= len(jobs):
                break
            if time.time() - t0 > 3600:
                break
    stop.set()
    if chaos_task:
        await chaos_task
    t_end = time.time()

    with psycopg.connect(DB, autocommit=True) as c:
        rows = c.execute("""SELECT tenant_id, idempotency_key, status, EXTRACT(EPOCH FROM finished_at) AS fin,
                                   attempt, tries FROM jobs""").fetchall()
        ledger = c.execute("SELECT kind, COALESCE(SUM(credits),0), count(*) FROM ledger GROUP BY 1").fetchall()
        dup_charges = c.execute("""SELECT count(*) FROM (SELECT job_id, attempt, kind FROM ledger
                                   GROUP BY 1,2,3 HAVING count(*) > 1) x""").fetchone()[0]
        outbox = c.execute("SELECT count(*), count(*) FILTER (WHERE published_at IS NOT NULL) FROM outbox").fetchone()
    counters = {k.decode(): int(v) for k, v in redis.Redis().hgetall("metrics").items()}
    mock = httpx.get(f"{MOCK}/_admin/stats").json()
    procs.stop_all()

    results = []
    for tenant, key, status, fin, attempt, tries in rows:
        j = keymap.get((tenant, key))
        results.append({"tenant": tenant, "status": status, "arrival": t0 + j["at"],
                        "finished": float(fin) if fin else None})
    led = {k: {"credits": int(s), "rows": n} for k, s, n in ledger}
    ledger_net = -sum(v["credits"] for v in led.values())
    extra = {
        "ledger": led, "ledger_net_credits": ledger_net, "provider_net_credits": mock["credits_net"],
        "ledger_matches_provider": ledger_net == mock["credits_net"],
        "duplicate_ledger_charges": dup_charges,
        "outbox_events": outbox[0], "outbox_published": outbox[1],
        "counters": counters, "chaos": chaos_log, "limit_trace": limit_trace,
    }
    return results, mock, t0, t_end, extra


# ---------------------------------------------------------------------------- baseline run

async def run_naive(jobs, chaos: bool, scale: float, name: str):
    procs = Procs({"MH_TIMEOUT_S": "10"})
    procs.start("mock", ["uvicorn", "mockmh.server:app", "--port", "9000", "--log-level", "warning"])
    wait_http(f"{MOCK}/_admin/stats")
    httpx.post(f"{MOCK}/_admin/reset", json={"webhook_url": "", "hang_s": 15})
    res_path = f"{LOGS}/{name}.jsonl"
    if os.path.exists(res_path):
        os.remove(res_path)
    t0 = time.time() + 1
    args = [sys.executable, "-m", "bench.naive_client", "--results", res_path, "--t0", str(t0),
            "--scale", str(scale)]
    procs.start("naive", args)
    chaos_log = []
    while True:
        await asyncio.sleep(2)
        el = time.time() - t0
        if chaos and not chaos_log and el >= 90:
            procs.kill9("naive")
            chaos_log.append({"t": round(el, 1), "event": "kill_client"})
            await asyncio.sleep(3)
            procs.start("naive", args)
        if procs.p["naive"].poll() is not None:
            break
    t_end = time.time()
    with open(res_path) as f:
        results = [json.loads(line) for line in f if line.strip()]
    mock = httpx.get(f"{MOCK}/_admin/stats").json()
    procs.stop_all()
    return results, mock, t0, t_end, {"chaos": chaos_log}


# ---------------------------------------------------------------------------- report

def summarize(name, jobs, results, mock, t0, t_end, extra):
    total = len(jobs)
    ok = [r for r in results if r["status"] == "succeeded"]
    lat = lambda rs: [r["finished"] - r["arrival"] for r in rs if r.get("finished")]
    small = [r for r in ok if r["tenant"].startswith("small")]
    bulk = [r for r in ok if r["tenant"] == "bulk-co"]
    last = max((r["finished"] for r in ok), default=t_end)
    makespan = last - t0
    summary = {
        "run": name,
        "jobs": total,
        "succeeded": len(ok),
        "failed": sum(1 for r in results if r["status"] == "failed"),
        "lost": total - len(results),
        "success_rate_pct": round(100 * len(ok) / total, 2),
        "makespan_s": round(makespan, 1),
        "throughput_jobs_per_min": round(60 * len(ok) / makespan, 1),
        "throughput_jobs_per_hour": round(3600 * len(ok) / makespan),
        "latency_s_all": {"p50": pct(lat(ok), .5), "p95": pct(lat(ok), .95), "p99": pct(lat(ok), .99)},
        "latency_s_small_tenants": {"p50": pct(lat(small), .5), "p95": pct(lat(small), .95),
                                    "p99": pct(lat(small), .99)},
        "latency_s_bulk_tenant": {"p50": pct(lat(bulk), .5), "p99": pct(lat(bulk), .99)},
        "upstream_creates": mock["counts"].get("create", 0),
        "upstream_gets": mock["counts"].get("get", 0),
        "upstream_429s": mock["counts"].get("429", 0),
        "upstream_429_rate": mock["counts"].get("429_rate", 0),
        "upstream_429_concurrency": mock["counts"].get("429_concurrency", 0),
        "provider_projects": mock["projects"],
        "provider_credits_net": mock["credits_net"],
        "duplicate_paid_renders": mock["duplicate_paid_renders"],
        "wasted_credits": mock["wasted_credits"],
        "faults_injected": {k: mock["counts"].get(k, 0) for k in
                            ("5xx_clean", "5xx_ambiguous", "hang", "get_5xx", "render_error", "webhook_dropped")},
    }
    summary.update(extra)
    return summary


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", choices=["naive", "orch"], required=True)
    ap.add_argument("--chaos", action="store_true")
    ap.add_argument("--scale", type=float, default=1.0)
    ap.add_argument("--name")
    a = ap.parse_args()
    os.makedirs(LOGS, exist_ok=True)
    name = a.name or f"{a.system}{'-chaos' if a.chaos else ''}"
    jobs = scaled(a.scale)
    reset_state()
    if a.system == "orch":
        results, mock, t0, t_end, extra = await run_orch(jobs, a.chaos)
    else:
        results, mock, t0, t_end, extra = await run_naive(jobs, a.chaos, a.scale, name)
    s = summarize(name, jobs, results, mock, t0, t_end, extra)
    os.makedirs(f"{ROOT}/results", exist_ok=True)
    with open(f"{ROOT}/results/{name}.json", "w") as f:
        json.dump(s, f, indent=2)
    brief = {k: v for k, v in s.items() if k not in ("limit_trace", "counters", "chaos")}
    print(json.dumps(brief, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
