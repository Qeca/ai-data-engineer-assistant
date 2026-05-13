CREATE TABLE IF NOT EXISTS retail_products (
    product_id INT PRIMARY KEY,
    sku VARCHAR(64) NOT NULL,
    category VARCHAR(64) NOT NULL,
    price DECIMAL(12, 2) NOT NULL
);

CREATE TABLE IF NOT EXISTS retail_sales (
    sale_id INT PRIMARY KEY,
    product_id INT NOT NULL,
    sold_at DATETIME NOT NULL,
    quantity INT NOT NULL,
    channel VARCHAR(32) NOT NULL,
    CONSTRAINT fk_retail_sales_product FOREIGN KEY (product_id) REFERENCES retail_products(product_id)
);

INSERT IGNORE INTO retail_products (product_id, sku, category, price) VALUES
    (1, 'SKU-COFFEE-1', 'grocery', 419.90),
    (2, 'SKU-LAPTOP-1', 'electronics', 72990.00),
    (3, 'SKU-SHIRT-1', 'apparel', 1899.00);

INSERT IGNORE INTO retail_sales (sale_id, product_id, sold_at, quantity, channel) VALUES
    (1, 1, '2026-05-10 10:00:00', 12, 'store'),
    (2, 2, '2026-05-10 11:30:00', 1, 'online'),
    (3, 3, '2026-05-11 15:10:00', 4, 'marketplace'),
    (4, 1, '2026-05-12 09:45:00', 8, 'online');
