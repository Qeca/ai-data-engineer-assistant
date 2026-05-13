CREATE SCHEMA IF NOT EXISTS sales;

CREATE TABLE IF NOT EXISTS sales.customers (
    customer_id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    segment TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS sales.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL REFERENCES sales.customers(customer_id),
    order_ts TIMESTAMP NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status TEXT NOT NULL
);

INSERT INTO sales.customers (customer_id, email, segment, created_at) VALUES
    (1, 'anna@example.com', 'retail', '2026-05-01 09:00:00'),
    (2, 'ivan@example.com', 'b2b', '2026-05-02 10:00:00'),
    (3, 'maria@example.com', 'vip', '2026-05-03 11:00:00')
ON CONFLICT DO NOTHING;

INSERT INTO sales.orders (order_id, customer_id, order_ts, amount, status) VALUES
    (1001, 1, '2026-05-10 10:15:00', 1290.50, 'paid'),
    (1002, 2, '2026-05-10 11:40:00', 5600.00, 'paid'),
    (1003, 3, '2026-05-11 12:20:00', 990.00, 'cancelled'),
    (1004, 1, '2026-05-11 13:05:00', 2400.30, 'paid'),
    (1005, 3, '2026-05-12 18:45:00', 10200.00, 'paid')
ON CONFLICT DO NOTHING;
