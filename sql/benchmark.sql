-- Index for properties->>'run_id' lookups (used by latency_report.py)
CREATE INDEX IF NOT EXISTS idx_events_run ON events ((properties->>'run_id'));