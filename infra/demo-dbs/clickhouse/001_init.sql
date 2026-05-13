CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.events
(
    event_time DateTime,
    user_id UInt64,
    event_name String,
    page String,
    revenue Float64
)
ENGINE = MergeTree
ORDER BY (event_time, user_id);

INSERT INTO analytics.events (event_time, user_id, event_name, page, revenue) VALUES
    ('2026-05-10 10:00:00', 101, 'page_view', '/catalog', 0),
    ('2026-05-10 10:05:00', 101, 'add_to_cart', '/product/1', 0),
    ('2026-05-10 10:12:00', 101, 'checkout', '/checkout', 1290.50),
    ('2026-05-11 12:00:00', 205, 'search', '/search', 0),
    ('2026-05-12 18:45:00', 309, 'checkout', '/checkout', 10200.00);
