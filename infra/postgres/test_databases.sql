SELECT 'CREATE DATABASE ai_de_playground'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'ai_de_playground')
\gexec

SELECT 'CREATE DATABASE marketing_playground'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'marketing_playground')
\gexec

SELECT 'CREATE DATABASE ops_playground'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'ops_playground')
\gexec

\connect ai_de_playground

CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS quality;
DROP VIEW IF EXISTS analytics.hourly_order_anomalies;
DROP VIEW IF EXISTS analytics.customer_ltv;
DROP VIEW IF EXISTS quality.order_quality_checks;
DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS payments;
DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE customers (
    id integer PRIMARY KEY,
    email text NOT NULL UNIQUE,
    segment text NOT NULL,
    city text NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE products (
    sku text PRIMARY KEY,
    name text NOT NULL,
    category text NOT NULL,
    base_price numeric(12, 2) NOT NULL
);

CREATE TABLE orders (
    id bigint PRIMARY KEY,
    created_at timestamptz NOT NULL,
    customer_id integer NOT NULL REFERENCES customers(id),
    channel text NOT NULL,
    status text NOT NULL,
    total_amount numeric(12, 2) NOT NULL,
    currency text NOT NULL DEFAULT 'RUB'
);

CREATE TABLE order_items (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES orders(id),
    sku text NOT NULL REFERENCES products(sku),
    quantity integer NOT NULL,
    unit_price numeric(12, 2) NOT NULL
);

CREATE TABLE payments (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id bigint NOT NULL REFERENCES orders(id),
    provider text NOT NULL,
    status text NOT NULL,
    paid_at timestamptz,
    amount numeric(12, 2) NOT NULL
);

CREATE TABLE events (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    occurred_at timestamptz NOT NULL,
    customer_id integer NOT NULL REFERENCES customers(id),
    event_name text NOT NULL,
    session_id text NOT NULL,
    properties jsonb NOT NULL DEFAULT '{}'::jsonb
);

SELECT setseed(0.42);

INSERT INTO customers (id, email, segment, city, created_at)
SELECT
    idx,
    'customer' || idx || '@test.dev',
    (ARRAY['retail', 'b2b', 'vip', 'new'])[1 + (idx % 4)],
    (ARRAY['Moscow', 'Saint Petersburg', 'Kazan', 'Novosibirsk', 'Yekaterinburg'])[1 + (idx % 5)],
    now() - ((idx % 180) || ' days')::interval
FROM generate_series(1, 500) AS idx;

INSERT INTO products (sku, name, category, base_price)
SELECT
    'SKU-' || lpad(idx::text, 3, '0'),
    'Product ' || idx,
    (ARRAY['electronics', 'books', 'home', 'sports', 'beauty'])[1 + (idx % 5)],
    round((150 + random() * 12000)::numeric, 2)
FROM generate_series(1, 30) AS idx;

WITH hours AS (
    SELECT generate_series(
        date_trunc('hour', now() - interval '30 days'),
        date_trunc('hour', now()),
        interval '1 hour'
    ) AS hour_ts
),
expanded AS (
    SELECT
        hour_ts,
        generate_series(
            1,
            CASE
                WHEN hour_ts::date = current_date - 7 AND extract(hour from hour_ts) = 20 THEN 420
                WHEN hour_ts::date = current_date - 3 AND extract(hour from hour_ts) IN (3, 4) THEN 1
                ELSE greatest(8, 28 + (extract(hour from hour_ts)::integer * 2) + (random() * 15)::integer)
            END
        ) AS n
    FROM hours
),
numbered AS (
    SELECT
        row_number() OVER () AS id,
        hour_ts
    FROM expanded
)
INSERT INTO orders (id, created_at, customer_id, channel, status, total_amount, currency)
SELECT
    id,
    hour_ts + (random() * interval '59 minutes'),
    1 + floor(random() * 500)::integer,
    (ARRAY['web', 'mobile', 'marketplace', 'retail_store'])[1 + floor(random() * 4)::integer],
    CASE
        WHEN random() < 0.04 THEN 'cancelled'
        WHEN random() < 0.07 THEN 'refunded'
        ELSE 'paid'
    END,
    round((300 + random() * 25000)::numeric, 2),
    'RUB'
FROM numbered;

INSERT INTO order_items (order_id, sku, quantity, unit_price)
SELECT
    orders.id,
    'SKU-' || lpad((1 + floor(random() * 30)::integer)::text, 3, '0'),
    1 + floor(random() * 4)::integer,
    round((150 + random() * 12000)::numeric, 2)
FROM orders
CROSS JOIN LATERAL generate_series(1, 1 + floor(random() * 3)::integer);

INSERT INTO payments (order_id, provider, status, paid_at, amount)
SELECT
    id,
    (ARRAY['tinkoff', 'sber', 'yookassa', 'stripe'])[1 + floor(random() * 4)::integer],
    CASE WHEN status = 'paid' THEN 'captured' ELSE status END,
    CASE WHEN status = 'paid' THEN created_at + interval '3 minutes' ELSE NULL END,
    total_amount
FROM orders;

INSERT INTO events (occurred_at, customer_id, event_name, session_id, properties)
SELECT
    now() - ((idx * 3) || ' minutes')::interval,
    1 + floor(random() * 500)::integer,
    (ARRAY['page_view', 'search', 'add_to_cart', 'checkout', 'payment_started'])[1 + floor(random() * 5)::integer],
    md5((idx || '-' || random())::text),
    jsonb_build_object('source', (ARRAY['ads', 'direct', 'email', 'seo'])[1 + floor(random() * 4)::integer])
FROM generate_series(1, 15000) AS idx;

CREATE INDEX idx_orders_created_at ON orders(created_at);
CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_events_occurred_at ON events(occurred_at);
CREATE INDEX idx_events_customer_id ON events(customer_id);

CREATE VIEW analytics.hourly_order_anomalies AS
WITH hourly AS (
    SELECT
        date_trunc('hour', created_at) AS hour,
        count(*) AS order_count,
        round(avg(total_amount), 2) AS avg_amount
    FROM orders
    GROUP BY 1
),
stats AS (
    SELECT
        avg(order_count) AS avg_count,
        stddev_pop(order_count) AS std_count
    FROM hourly
)
SELECT
    h.hour,
    h.order_count,
    h.avg_amount,
    round(((h.order_count - s.avg_count) / nullif(s.std_count, 0))::numeric, 2) AS z_score
FROM hourly h
CROSS JOIN stats s
WHERE abs((h.order_count - s.avg_count) / nullif(s.std_count, 0)) >= 2.5
ORDER BY abs((h.order_count - s.avg_count) / nullif(s.std_count, 0)) DESC;

CREATE VIEW analytics.customer_ltv AS
SELECT
    c.id AS customer_id,
    c.email,
    c.segment,
    count(o.id) AS orders,
    round(coalesce(sum(o.total_amount) FILTER (WHERE o.status = 'paid'), 0), 2) AS paid_revenue
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
GROUP BY c.id, c.email, c.segment
ORDER BY paid_revenue DESC;

CREATE VIEW quality.order_quality_checks AS
SELECT 'orders_without_items' AS check_name, count(*) AS failed_rows
FROM orders o
WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id)
UNION ALL
SELECT 'payments_amount_mismatch' AS check_name, count(*) AS failed_rows
FROM payments p
JOIN orders o ON o.id = p.order_id
WHERE p.amount <> o.total_amount
UNION ALL
SELECT 'future_orders' AS check_name, count(*) AS failed_rows
FROM orders
WHERE created_at > now();

\connect marketing_playground

DROP TABLE IF EXISTS conversions;
DROP TABLE IF EXISTS leads;
DROP TABLE IF EXISTS ad_spend;
DROP TABLE IF EXISTS campaigns;

CREATE TABLE campaigns (
    id integer PRIMARY KEY,
    name text NOT NULL,
    channel text NOT NULL,
    started_at date NOT NULL,
    ended_at date
);

CREATE TABLE ad_spend (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    campaign_id integer NOT NULL REFERENCES campaigns(id),
    spend_date date NOT NULL,
    impressions integer NOT NULL,
    clicks integer NOT NULL,
    spend_amount numeric(12, 2) NOT NULL
);

CREATE TABLE leads (
    id bigint PRIMARY KEY,
    campaign_id integer NOT NULL REFERENCES campaigns(id),
    created_at timestamptz NOT NULL,
    email text NOT NULL,
    score integer NOT NULL,
    status text NOT NULL
);

CREATE TABLE conversions (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lead_id bigint NOT NULL REFERENCES leads(id),
    converted_at timestamptz NOT NULL,
    revenue numeric(12, 2) NOT NULL
);

INSERT INTO campaigns (id, name, channel, started_at, ended_at)
SELECT
    idx,
    'Campaign ' || idx,
    (ARRAY['search', 'social', 'email', 'partner'])[1 + (idx % 4)],
    current_date - (90 - idx * 3),
    NULL
FROM generate_series(1, 12) AS idx;

INSERT INTO ad_spend (campaign_id, spend_date, impressions, clicks, spend_amount)
SELECT
    c.id,
    day::date,
    10000 + floor(random() * 80000)::integer,
    300 + floor(random() * 4000)::integer,
    round((5000 + random() * 90000)::numeric, 2)
FROM campaigns c
CROSS JOIN generate_series(current_date - interval '60 days', current_date, interval '1 day') AS day;

INSERT INTO leads (id, campaign_id, created_at, email, score, status)
SELECT
    idx,
    1 + floor(random() * 12)::integer,
    now() - ((idx * 31) || ' minutes')::interval,
    'lead' || idx || '@test.dev',
    1 + floor(random() * 100)::integer,
    (ARRAY['new', 'qualified', 'lost', 'won'])[1 + floor(random() * 4)::integer]
FROM generate_series(1, 5000) AS idx;

INSERT INTO conversions (lead_id, converted_at, revenue)
SELECT
    id,
    created_at + interval '2 days',
    round((2000 + random() * 150000)::numeric, 2)
FROM leads
WHERE status = 'won' OR random() < 0.08;

\connect ops_playground

DROP TABLE IF EXISTS incidents;
DROP TABLE IF EXISTS data_quality_results;
DROP TABLE IF EXISTS pipeline_runs;
DROP TABLE IF EXISTS source_systems;

CREATE TABLE source_systems (
    id integer PRIMARY KEY,
    name text NOT NULL,
    owner_team text NOT NULL,
    criticality text NOT NULL
);

CREATE TABLE pipeline_runs (
    id bigint PRIMARY KEY,
    dag_id text NOT NULL,
    source_system_id integer NOT NULL REFERENCES source_systems(id),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    status text NOT NULL,
    rows_processed bigint NOT NULL,
    duration_seconds integer NOT NULL
);

CREATE TABLE data_quality_results (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_run_id bigint NOT NULL REFERENCES pipeline_runs(id),
    check_name text NOT NULL,
    status text NOT NULL,
    failed_rows integer NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE incidents (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    opened_at timestamptz NOT NULL,
    closed_at timestamptz,
    severity text NOT NULL,
    title text NOT NULL,
    related_dag_id text
);

INSERT INTO source_systems (id, name, owner_team, criticality)
VALUES
    (1, 'orders_api', 'data-platform', 'high'),
    (2, 'payments_gateway', 'fintech', 'high'),
    (3, 'clickstream', 'growth', 'medium'),
    (4, 'crm', 'sales-ops', 'medium'),
    (5, 'inventory', 'supply', 'low');

INSERT INTO pipeline_runs (id, dag_id, source_system_id, started_at, finished_at, status, rows_processed, duration_seconds)
SELECT
    idx,
    (ARRAY['orders_sync', 'payments_reconciliation', 'clickstream_aggregation', 'crm_daily_load', 'inventory_snapshot'])[1 + (idx % 5)],
    1 + (idx % 5),
    now() - ((idx * 2) || ' hours')::interval,
    now() - ((idx * 2) || ' hours')::interval + ((120 + floor(random() * 2400)::integer) || ' seconds')::interval,
    CASE WHEN idx % 17 = 0 THEN 'failed' WHEN idx % 13 = 0 THEN 'running' ELSE 'success' END,
    1000 + floor(random() * 800000)::bigint,
    120 + floor(random() * 2400)::integer
FROM generate_series(1, 600) AS idx;

INSERT INTO data_quality_results (pipeline_run_id, check_name, status, failed_rows, details)
SELECT
    id,
    (ARRAY['not_null_primary_key', 'freshness_sla', 'row_count_delta', 'duplicate_key'])[1 + floor(random() * 4)::integer],
    CASE WHEN status = 'failed' OR random() < 0.08 THEN 'failed' ELSE 'passed' END,
    CASE WHEN status = 'failed' OR random() < 0.08 THEN floor(random() * 2000)::integer ELSE 0 END,
    jsonb_build_object('threshold', 0.05)
FROM pipeline_runs;

INSERT INTO incidents (opened_at, closed_at, severity, title, related_dag_id)
SELECT
    now() - ((idx * 19) || ' hours')::interval,
    CASE WHEN idx % 5 = 0 THEN NULL ELSE now() - ((idx * 19 - 3) || ' hours')::interval END,
    (ARRAY['sev1', 'sev2', 'sev3'])[1 + (idx % 3)],
    'Pipeline incident ' || idx,
    (ARRAY['orders_sync', 'payments_reconciliation', 'clickstream_aggregation'])[1 + (idx % 3)]
FROM generate_series(1, 30) AS idx;
