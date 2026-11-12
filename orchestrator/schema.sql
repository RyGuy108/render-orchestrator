-- Postgres is the source of truth. Redis only holds a rebuildable index of what is runnable.

CREATE TABLE IF NOT EXISTS jobs (
    id               uuid PRIMARY KEY,
    tenant_id        text        NOT NULL,
    idempotency_key  text        NOT NULL,
    request_hash     text        NOT NULL,
    priority         smallint    NOT NULL DEFAULT 1,       -- 0 = interactive, 1 = standard, 2 = batch
    endpoint         text        NOT NULL,                 -- e.g. /v1/ai-image-generator
    payload          jsonb       NOT NULL,
    status           text        NOT NULL DEFAULT 'queued',
        -- queued -> submitting -> rendering -> succeeded
        --    ^          |            |
        --    +--- retry-+------------+  (backoff)          -> failed (dead letter)
    attempt          int         NOT NULL DEFAULT 0,       -- render attempts that reached the provider
    tries            int         NOT NULL DEFAULT 0,       -- submit tries (transport errors, 5xx, ...)
    provider_key     text,                                 -- idempotency key sent upstream for the current attempt
    provider_id      text,                                 -- Magic Hour project id for the current attempt
    provider_kind    text,                                 -- image | video | audio
    credits_charged  int,
    not_before       timestamptz NOT NULL DEFAULT now(),
    lease_owner      text,
    lease_until      timestamptz,
    last_polled_at   timestamptz,
    poll_interval_s  real        NOT NULL DEFAULT 2,
    last_error       text,
    result           jsonb,
    created_at       timestamptz NOT NULL DEFAULT now(),
    first_submit_at  timestamptz,
    submitted_at     timestamptz,
    finished_at      timestamptz,
    enqueued_at      timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, idempotency_key)
);
CREATE UNIQUE INDEX IF NOT EXISTS jobs_provider_id ON jobs (provider_id) WHERE provider_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_status ON jobs (status, not_before);
CREATE INDEX IF NOT EXISTS jobs_rendering ON jobs (last_polled_at) WHERE status = 'rendering';
CREATE INDEX IF NOT EXISTS jobs_submitting ON jobs (lease_until) WHERE status = 'submitting';

-- Every provider project we ever created, one row per attempt. The ledger keys on this.
CREATE TABLE IF NOT EXISTS attempts (
    job_id       uuid  NOT NULL REFERENCES jobs(id),
    attempt      int   NOT NULL,
    provider_key text  NOT NULL UNIQUE,
    provider_id  text  UNIQUE,
    outcome      text,                                    -- complete | error | NULL while open
    created_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, attempt)
);

-- Append-only credit ledger. Unique (job, attempt, kind) makes double charging impossible
-- even if a webhook and the poller race, or a worker replays a submit after a crash.
CREATE TABLE IF NOT EXISTS ledger (
    id          bigserial PRIMARY KEY,
    tenant_id   text   NOT NULL,
    job_id      uuid   NOT NULL REFERENCES jobs(id),
    attempt     int    NOT NULL,
    kind        text   NOT NULL CHECK (kind IN ('charge', 'refund')),
    credits     int    NOT NULL,                          -- negative = debit from the tenant
    created_at  timestamptz NOT NULL DEFAULT now(),
    UNIQUE (job_id, attempt, kind)
);
CREATE INDEX IF NOT EXISTS ledger_tenant ON ledger (tenant_id);

-- Transactional outbox: written in the same transaction as the state change, relayed later.
CREATE TABLE IF NOT EXISTS outbox (
    id           bigserial PRIMARY KEY,
    tenant_id    text  NOT NULL,
    job_id       uuid  NOT NULL,
    event        text  NOT NULL,
    body         jsonb NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz,
    UNIQUE (job_id, event)
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox (id) WHERE published_at IS NULL;
