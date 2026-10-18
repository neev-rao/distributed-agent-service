CREATE TABLE service_settings (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    queue_capacity integer NOT NULL CHECK (queue_capacity > 0),
    max_records integer NOT NULL CHECK (max_records > 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    max_result_bytes integer NOT NULL CHECK (max_result_bytes > 0),
    execution_timeout_us bigint NOT NULL CHECK (execution_timeout_us > 0),
    lease_duration_us bigint NOT NULL CHECK (lease_duration_us > 0),
    retry_backoff_us bigint NOT NULL CHECK (retry_backoff_us > 0),
    retention_us bigint NOT NULL CHECK (retention_us > 0)
);

CREATE TABLE jobs (
    id text PRIMARY KEY,
    scenario_id text NOT NULL,
    idempotency_key text UNIQUE,
    state text NOT NULL CHECK (state IN ('queued', 'running', 'succeeded', 'failed')),
    created_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    available_at timestamptz NOT NULL,
    execution_timeout_us bigint NOT NULL CHECK (execution_timeout_us > 0),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
    attempt_token text,
    worker_id text,
    lease_until timestamptz,
    execution_deadline timestamptz,
    result jsonb,
    error jsonb,
    CHECK ((state = 'running') = (attempt_token IS NOT NULL)),
    CHECK ((state = 'running') = (lease_until IS NOT NULL)),
    CHECK ((state IN ('succeeded', 'failed')) = (finished_at IS NOT NULL)),
    CHECK (state != 'succeeded' OR result IS NOT NULL),
    CHECK (state != 'failed' OR error IS NOT NULL)
);

CREATE INDEX jobs_waiting ON jobs (available_at, created_at, id) WHERE state = 'queued';
CREATE INDEX jobs_expired ON jobs (lease_until, id) WHERE state = 'running';
CREATE INDEX jobs_terminal ON jobs (finished_at, id) WHERE state IN ('succeeded', 'failed');

CREATE TABLE job_attempts (
    token text PRIMARY KEY,
    job_id text NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt integer NOT NULL,
    worker_id text NOT NULL,
    started_at timestamptz NOT NULL,
    deadline timestamptz NOT NULL,
    lease_until timestamptz NOT NULL,
    finished_at timestamptz,
    outcome text CHECK (outcome IN ('succeeded', 'failed', 'lease_expired')),
    UNIQUE (job_id, attempt),
    CHECK ((finished_at IS NULL) = (outcome IS NULL))
);
