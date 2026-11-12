"""Redis-side scheduling: weighted fair queuing across tenants, priority lanes, delayed retries,
a global token bucket and an AIMD concurrency limit.

Everything that must be atomic is a Lua script, so any number of worker processes can share it.
Redis is an index, not the source of truth: the reconciler can rebuild all of it from Postgres
(see `resync`), and a job popped twice is harmless because the Postgres claim is a CAS.
"""
from __future__ import annotations

import time

import redis.asyncio as aioredis

from .config import settings

LANES = (0, 1, 2)  # 0 interactive, 1 standard, 2 batch

# KEYS: none (dynamic keys are fine on a single Redis; documented limitation for Cluster)
# ARGV: lane, tenant, job_id
ENQUEUE = """
local lane, tenant, job = ARGV[1], ARGV[2], ARGV[3]
if redis.call('SADD', 'rq:member', job) == 0 then return 0 end
redis.call('RPUSH', 'rq:q:' .. lane .. ':' .. tenant, job)
if not redis.call('ZSCORE', 'rq:lane:' .. lane, tenant) then
  -- A tenant that (re)joins starts at the current virtual time, never in the past: it cannot
  -- bank credit while idle and then monopolise the lane (start-time fair queuing).
  local vt = tonumber(redis.call('GET', 'rq:vt:' .. lane) or '0')
  local last = tonumber(redis.call('HGET', 'rq:vft:' .. lane, tenant) or '0')
  redis.call('ZADD', 'rq:lane:' .. lane, math.max(vt, last), tenant)
end
return 1
"""

# ARGV: lane, tenant, job_id, ready_at
DELAY = """
if redis.call('SADD', 'rq:member', ARGV[3]) == 0 then return 0 end
redis.call('ZADD', 'rq:delayed', ARGV[4], ARGV[1] .. '|' .. ARGV[2] .. '|' .. ARGV[3])
return 1
"""

# ARGV: now, tenant_cap, starvation_every
DEQUEUE = """
local now = tonumber(ARGV[1])
local tenant_cap = tonumber(ARGV[2])
local every = tonumber(ARGV[3])

-- 1) promote due retries
local due = redis.call('ZRANGEBYSCORE', 'rq:delayed', '-inf', now, 'LIMIT', 0, 200)
for _, m in ipairs(due) do
  redis.call('ZREM', 'rq:delayed', m)
  local lane, tenant, job = string.match(m, '([^|]+)|([^|]+)|(.+)')
  redis.call('RPUSH', 'rq:q:' .. lane .. ':' .. tenant, job)
  if not redis.call('ZSCORE', 'rq:lane:' .. lane, tenant) then
    local vt = tonumber(redis.call('GET', 'rq:vt:' .. lane) or '0')
    local last = tonumber(redis.call('HGET', 'rq:vft:' .. lane, tenant) or '0')
    redis.call('ZADD', 'rq:lane:' .. lane, math.max(vt, last), tenant)
  end
end

-- 2) global gates: 429 back-off window and the adaptive in-flight limit
local pause = tonumber(redis.call('GET', 'rq:pause_until') or '0')
if pause > now then return {'pause', tostring(pause - now)} end
local limit = math.floor(tonumber(redis.call('GET', 'rq:limit') or '1'))
local total = tonumber(redis.call('GET', 'rq:inflight_total') or '0')
if total >= limit then return {'full', '0'} end

-- 3) lane order: strict priority, except every Nth pick goes lowest-first (anti-starvation)
local picks = redis.call('INCR', 'rq:picks')
local order = {'0', '1', '2'}
if every > 0 and picks % every == 0 then order = {'2', '1', '0'} end

for _, lane in ipairs(order) do
  local lkey = 'rq:lane:' .. lane
  local tenants = redis.call('ZRANGE', lkey, 0, 63, 'WITHSCORES')
  for i = 1, #tenants, 2 do
    local tenant, score = tenants[i], tonumber(tenants[i + 1])
    local w = tonumber(redis.call('HGET', 'rq:weight', tenant) or '1')
    local cap = math.max(1, math.floor(tenant_cap * w))
    local busy = tonumber(redis.call('HGET', 'rq:inflight', tenant) or '0')
    if busy < cap then
      local qkey = 'rq:q:' .. lane .. ':' .. tenant
      local job = redis.call('LPOP', qkey)
      if job then
        redis.call('SREM', 'rq:member', job)
        local finish = score + 1 / w
        redis.call('SET', 'rq:vt:' .. lane, score)
        if redis.call('LLEN', qkey) == 0 then
          redis.call('ZREM', lkey, tenant)
          redis.call('HSET', 'rq:vft:' .. lane, tenant, finish)
        else
          redis.call('ZADD', lkey, finish, tenant)
        end
        redis.call('HINCRBY', 'rq:inflight', tenant, 1)
        redis.call('INCR', 'rq:inflight_total')
        return {'job', job, tenant, lane}
      else
        redis.call('ZREM', lkey, tenant)
      end
    end
  end
end
return {'empty', '0'}
"""

# ARGV: tenant
RELEASE = """
local v = redis.call('HINCRBY', 'rq:inflight', ARGV[1], -1)
if v < 0 then redis.call('HSET', 'rq:inflight', ARGV[1], 0) end
local t = redis.call('DECR', 'rq:inflight_total')
if t < 0 then redis.call('SET', 'rq:inflight_total', 0) end
return 1
"""

# Token bucket. ARGV: now, rate, burst, cost. Returns seconds to wait (0 = token taken).
TOKEN = """
local now, rate, burst, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), tonumber(ARGV[4])
local b = redis.call('HMGET', 'rl:bucket', 'tokens', 'ts')
local tokens = tonumber(b[1] or burst)
local ts = tonumber(b[2] or now)
tokens = math.min(burst, tokens + (now - ts) * rate)
local wait = 0
if tokens >= cost then tokens = tokens - cost else wait = (cost - tokens) / rate end
redis.call('HSET', 'rl:bucket', 'tokens', tokens, 'ts', now)
return tostring(wait)
"""

# AIMD on the in-flight limit. ARGV: now, kind ('ok' | 'throttled'), min, max, retry_after
AIMD = """
local now, kind = tonumber(ARGV[1]), ARGV[2]
local lo, hi = tonumber(ARGV[3]), tonumber(ARGV[4])
local limit = tonumber(redis.call('GET', 'rq:limit') or hi)
if kind == 'ok' then
  limit = math.min(hi, limit + 1 / math.max(limit, 1))
else
  -- at most one multiplicative decrease per second: a burst of 429s is one congestion event
  local last = tonumber(redis.call('GET', 'rq:last_md') or '0')
  if now - last >= 1 then
    limit = math.max(lo, limit * 0.75)
    redis.call('SET', 'rq:last_md', now)
  end
  local until_ = now + tonumber(ARGV[5])
  local cur = tonumber(redis.call('GET', 'rq:pause_until') or '0')
  if until_ > cur then redis.call('SET', 'rq:pause_until', until_) end
end
redis.call('SET', 'rq:limit', limit)
return tostring(limit)
"""


class Scheduler:
    def __init__(self, r: aioredis.Redis):
        self.r = r
        self._enqueue = r.register_script(ENQUEUE)
        self._delay = r.register_script(DELAY)
        self._dequeue = r.register_script(DEQUEUE)
        self._release = r.register_script(RELEASE)
        self._token = r.register_script(TOKEN)
        self._aimd = r.register_script(AIMD)

    async def init(self) -> None:
        await self.r.set("rq:limit", settings.max_inflight, nx=True)

    async def enqueue(self, job_id: str, tenant: str, lane: int, delay_s: float = 0.0) -> bool:
        if delay_s and delay_s > 0:
            return bool(await self._delay(args=[lane, tenant, job_id, time.time() + delay_s]))
        return bool(await self._enqueue(args=[lane, tenant, job_id]))

    async def dequeue(self):
        res = await self._dequeue(args=[time.time(), settings.tenant_max_inflight, settings.starvation_guard_every])
        kind = res[0].decode() if isinstance(res[0], bytes) else res[0]
        if kind == "job":
            dec = [x.decode() if isinstance(x, bytes) else x for x in res[1:]]
            return "job", {"job_id": dec[0], "tenant": dec[1], "lane": int(dec[2])}
        return kind, float(res[1])

    async def release(self, tenant: str) -> None:
        await self._release(args=[tenant])

    async def acquire_token(self, cost: float = 1.0) -> None:
        """Block until the shared upstream token bucket allows one request."""
        import asyncio

        while True:
            wait = float(await self._token(args=[time.time(), settings.upstream_rps, settings.upstream_burst, cost]))
            if wait <= 0:
                return
            await asyncio.sleep(wait)

    async def on_upstream(self, throttled: bool, retry_after: float = 0.0) -> float:
        return float(await self._aimd(args=[time.time(), "throttled" if throttled else "ok",
                                            settings.min_inflight, settings.max_inflight, retry_after]))

    async def pause_remaining(self) -> float:
        p = float(await self.r.get("rq:pause_until") or 0)
        return max(0.0, p - time.time())

    async def set_weight(self, tenant: str, weight: float) -> None:
        await self.r.hset("rq:weight", tenant, weight)

    async def resync_inflight(self, counts: dict[str, int]) -> None:
        pipe = self.r.pipeline(transaction=True)
        pipe.delete("rq:inflight")
        if counts:
            pipe.hset("rq:inflight", mapping=counts)
        pipe.set("rq:inflight_total", sum(counts.values()))
        await pipe.execute()

    async def missing(self, job_ids: list[str]) -> list[bool]:
        if not job_ids:
            return []
        res = await self.r.smismember("rq:member", job_ids)
        return [not bool(x) for x in res]


async def incr(r: aioredis.Redis, name: str, n: int = 1) -> None:
    await r.hincrby("metrics", name, n)
