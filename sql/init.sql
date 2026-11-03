-- schema init
-- tables, enums, etc.

CREATE TABLE IF NOT EXISTS events (
    event_id     UUID PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    user_id      TEXT        NOT NULL,
    session_id   TEXT        NOT NULL,
    event_ts     TIMESTAMPTZ NOT NULL,
    properties   JSONB       NOT NULL DEFAULT '{}',
    ingested_at  TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS idx_events_ts      ON events (event_ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events (event_type, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_user    ON events (user_id);
