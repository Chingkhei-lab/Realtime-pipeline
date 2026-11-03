-- Throughput per minute and event type
CREATE OR REPLACE VIEW v_events_per_minute AS
SELECT date_trunc('minute', event_ts) AS minute,
       event_type,
       count(*) AS events
FROM events
GROUP BY 1, 2;

-- Revenue per day
CREATE OR REPLACE VIEW v_daily_revenue AS
SELECT date_trunc('day', event_ts) AS day,
       count(*) AS purchases,
       sum((properties->>'amount')::numeric) AS revenue,
       round(avg((properties->>'amount')::numeric), 2) AS avg_order_value
FROM events
WHERE event_type = 'purchase'
GROUP BY 1;

-- Session-level conversion funnel per day
-- (counts sessions that did each step; it does not enforce ordering)
CREATE OR REPLACE VIEW v_conversion_funnel AS
SELECT date_trunc('day', event_ts) AS day,
       count(DISTINCT session_id) FILTER (WHERE event_type = 'page_view')   AS viewed,
       count(DISTINCT session_id) FILTER (WHERE event_type = 'add_to_cart') AS carted,
       count(DISTINCT session_id) FILTER (WHERE event_type = 'purchase')    AS purchased
FROM events
GROUP BY 1;

-- Most visited pages
CREATE OR REPLACE VIEW v_top_pages AS
SELECT properties->>'url' AS url,
       count(*) AS views,
       count(DISTINCT user_id) AS unique_users
FROM events
WHERE event_type = 'page_view'
GROUP BY 1
ORDER BY views DESC;

-- Pipeline health: latency per minute (this is your monitoring view)
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT date_trunc('minute', processed_at) AS minute,
       count(*) AS events,
       round(percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - event_ts))::numeric, 3) AS p50_sec,
       round(percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - event_ts))::numeric, 3) AS p95_sec,
       round(max(extract(epoch FROM processed_at - event_ts))::numeric, 3) AS max_sec
FROM events
GROUP BY 1;

-- Pre-aggregated hourly stats for fast dashboards
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_hourly_stats AS
SELECT date_trunc('hour', event_ts) AS hour,
       event_type,
       count(*) AS events,
       count(DISTINCT user_id) AS users
FROM events
GROUP BY 1, 2;

-- Required for REFRESH ... CONCURRENTLY (refresh without blocking readers)
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_hourly ON mv_hourly_stats (hour, event_type);