"""Deterministic workload shared by the baseline and the orchestrator runs.

Default ("mixed", 10,000 jobs): one tenant dumps a 7,000-job backfill at t=0 on the batch lane,
while 30 small tenants each send 100 jobs spread over the first 240 s on the standard lane.
That is the case FIFO handles worst: small customers queue behind the backfill.
"""
from __future__ import annotations

import random


def mixed(n_bulk: int = 7000, n_small_tenants: int = 30, per_small: int = 100, spread_s: float = 240.0,
          seed: int = 1) -> list[dict]:
    rng = random.Random(seed)
    jobs = []
    for i in range(n_bulk):
        jobs.append({"key": f"bulk-{i}", "tenant": "bulk-co", "priority": 2, "at": 0.0})
    for t in range(n_small_tenants):
        for i in range(per_small):
            jobs.append({"key": f"s{t}-{i}", "tenant": f"small-{t:02d}", "priority": 1,
                         "at": rng.uniform(0, spread_s)})
    for j in jobs:
        j["endpoint"] = "/v1/ai-image-generator"
        j["payload"] = {"model": "z-image-turbo", "image_count": 1, "aspect_ratio": "1:1", "resolution": "640px",
                        "style": {"prompt": f"product shot #{j['key']}"}}
    jobs.sort(key=lambda j: j["at"])
    return jobs


def scaled(scale: float) -> list[dict]:
    return mixed(n_bulk=int(7000 * scale), per_small=max(1, int(100 * scale)), spread_s=240.0 * max(scale, 0.1))
