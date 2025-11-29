-- ============================================
--   E-COMMERCE DATA WAREHOUSE (STAR SCHEMA)
-- ============================================

-- -------------------------------
-- DIMENSION TABLES
-- -------------------------------

CREATE TABLE dim_customer (
    customer_key   SERIAL PRIMARY KEY,
    customer_id    VARCHAR(20),
    customer_name  VARCHAR(100),
    email          VARCHAR(100),
    phone          VARCHAR(20),
    created_date   DATE
);

CREATE TABLE dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_id    VARCHAR(20),
    product_name  VARCHAR(100),
    category      VARCHAR(50),
    brand         VARCHAR(50),
    price         NUMERIC(10,2)
);

CREATE TABLE dim_date (
    date_key      INT PRIMARY KEY,     -- YYYYMMDD
    full_date     DATE,
    year          INT,
    month         INT,
    day           INT,
    quarter       INT
);

CREATE TABLE dim_payment (
    payment_key    SERIAL PRIMARY KEY,
    payment_method VARCHAR(50)
);

-- -------------------------------
-- FACT TABLE
-- -------------------------------

CREATE TABLE fact_orders (
    order_key      SERIAL PRIMARY KEY,
    order_id       VARCHAR(20),
    customer_key   INT REFERENCES dim_customer(customer_key),
    product_key    INT REFERENCES dim_product(product_key),
    payment_key    INT REFERENCES dim_payment(payment_key),
    date_key       INT REFERENCES dim_date(date_key),
    quantity       INT,
    price          NUMERIC(10,2),
    discount       NUMERIC(10,2),
    total_amount   NUMERIC(10,2)
);