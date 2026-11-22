-- =====================================================================
-- E-Commerce Profit Leakage & RFM Engine
-- 01_schema.sql  |  PostgreSQL 16
-- Normalised warehouse for the cleaned Olist data.
-- Re-runnable: drops and recreates schema "olist".
-- Load order (respects foreign keys):
--   assumptions -> customers, sellers, products -> orders
--   -> order_items, payments, reviews -> customer_rfm
-- =====================================================================

DROP SCHEMA IF EXISTS olist CASCADE;
CREATE SCHEMA olist;
SET search_path TO olist;

-- ---------------------------------------------------------------------
-- Parameters shared by Excel, Python, SQL and Power BI
-- ---------------------------------------------------------------------
CREATE TABLE assumptions (
    name        VARCHAR(40)  PRIMARY KEY,
    value       NUMERIC(12,4) NOT NULL,
    reason      TEXT
);

-- ---------------------------------------------------------------------
-- Dimensions
-- ---------------------------------------------------------------------
CREATE TABLE customers (
    customer_id         CHAR(32)     PRIMARY KEY,      -- one per order in Olist
    customer_unique_id  CHAR(32)     NOT NULL,         -- the real person
    zip_code_prefix     VARCHAR(5),
    city                VARCHAR(60),
    state               CHAR(2)      NOT NULL,
    region              VARCHAR(15)  NOT NULL
);
CREATE INDEX ix_customers_unique ON customers (customer_unique_id);
CREATE INDEX ix_customers_state  ON customers (state);

CREATE TABLE sellers (
    seller_id        CHAR(32)    PRIMARY KEY,
    zip_code_prefix  VARCHAR(5),
    city             VARCHAR(60),
    state            CHAR(2)     NOT NULL,
    region           VARCHAR(15) NOT NULL
);

CREATE TABLE products (
    product_id     CHAR(32)     PRIMARY KEY,
    category_pt    VARCHAR(60)  NOT NULL,              -- 'unknown' when missing
    category_en    VARCHAR(60)  NOT NULL,
    photos_qty     SMALLINT,
    weight_g       INTEGER,
    length_cm      SMALLINT,
    height_cm      SMALLINT,
    width_cm       SMALLINT
);
CREATE INDEX ix_products_category ON products (category_en);

-- ---------------------------------------------------------------------
-- Facts
-- ---------------------------------------------------------------------
CREATE TABLE orders (
    order_id              CHAR(32)      PRIMARY KEY,
    customer_id           CHAR(32)      NOT NULL REFERENCES customers (customer_id),
    order_status          VARCHAR(15)   NOT NULL,
    status_group          VARCHAR(15)   NOT NULL,      -- Delivered / In progress / Failed
    purchase_ts           TIMESTAMP     NOT NULL,
    approved_ts           TIMESTAMP,
    carrier_ts            TIMESTAMP,
    delivered_ts          TIMESTAMP,
    estimated_date        DATE,
    purchase_month        DATE          NOT NULL,      -- first day of month, for trends
    delay_days            INTEGER,                     -- delivered - estimated (days)
    delay_bucket          VARCHAR(20),
    is_late               BOOLEAN,
    date_sequence_issue   BOOLEAN       NOT NULL DEFAULT FALSE,
    status_conflict       BOOLEAN       NOT NULL DEFAULT FALSE,
    item_count            SMALLINT      NOT NULL DEFAULT 0,
    items_value           NUMERIC(12,2) NOT NULL DEFAULT 0,   -- sum of price
    freight_value         NUMERIC(12,2) NOT NULL DEFAULT 0,   -- sum of freight
    billed_value          NUMERIC(12,2) NOT NULL DEFAULT 0,   -- price + freight
    paid_value            NUMERIC(12,2) NOT NULL DEFAULT 0,   -- sum of payments
    recon_gap             NUMERIC(12,2) NOT NULL DEFAULT 0,   -- paid - billed
    recon_exception       BOOLEAN       NOT NULL DEFAULT FALSE
);
CREATE INDEX ix_orders_customer ON orders (customer_id);
CREATE INDEX ix_orders_status   ON orders (status_group);
CREATE INDEX ix_orders_month    ON orders (purchase_month);

CREATE TABLE order_items (
    order_id          CHAR(32)      NOT NULL REFERENCES orders (order_id),
    order_item_id     SMALLINT      NOT NULL,
    product_id        CHAR(32)      NOT NULL REFERENCES products (product_id),
    seller_id         CHAR(32)      NOT NULL REFERENCES sellers (seller_id),
    shipping_limit_ts TIMESTAMP,
    price             NUMERIC(10,2) NOT NULL CHECK (price > 0),
    freight_value     NUMERIC(10,2) NOT NULL CHECK (freight_value >= 0),
    freight_ratio     NUMERIC(8,4),
    high_freight      BOOLEAN       NOT NULL DEFAULT FALSE,
    is_price_outlier  BOOLEAN       NOT NULL DEFAULT FALSE,
    late_handover     BOOLEAN,
    PRIMARY KEY (order_id, order_item_id)
);
CREATE INDEX ix_items_product ON order_items (product_id);
CREATE INDEX ix_items_seller  ON order_items (seller_id);

CREATE TABLE payments (
    order_id            CHAR(32)      NOT NULL REFERENCES orders (order_id),
    payment_sequential  SMALLINT      NOT NULL,
    payment_type        VARCHAR(15)   NOT NULL,
    payment_label       VARCHAR(25)   NOT NULL,
    installments        SMALLINT      NOT NULL CHECK (installments >= 1),
    payment_value       NUMERIC(12,2) NOT NULL,
    PRIMARY KEY (order_id, payment_sequential)
);
CREATE INDEX ix_payments_type ON payments (payment_type);

CREATE TABLE reviews (
    order_id         CHAR(32)    PRIMARY KEY REFERENCES orders (order_id),  -- latest review per order
    review_id        CHAR(32)    NOT NULL,
    review_score     SMALLINT    NOT NULL CHECK (review_score BETWEEN 1 AND 5),
    review_band      VARCHAR(10) NOT NULL,
    has_comment      BOOLEAN     NOT NULL,
    creation_date    TIMESTAMP,
    answer_ts        TIMESTAMP
);

-- ---------------------------------------------------------------------
-- Customer value (one row per real customer)
-- ---------------------------------------------------------------------
CREATE TABLE customer_rfm (
    customer_unique_id  CHAR(32)      PRIMARY KEY,
    state               CHAR(2),
    region              VARCHAR(15),
    first_purchase_ts   TIMESTAMP     NOT NULL,
    last_purchase_ts    TIMESTAMP     NOT NULL,
    recency_days        INTEGER       NOT NULL,
    frequency           INTEGER       NOT NULL,
    monetary            NUMERIC(12,2) NOT NULL,
    r_score             SMALLINT      NOT NULL,
    f_score             SMALLINT      NOT NULL,
    m_score             SMALLINT      NOT NULL,
    rfm_cell            CHAR(3)       NOT NULL,
    segment             VARCHAR(20)   NOT NULL,
    snapshot_date       DATE          NOT NULL
);
CREATE INDEX ix_rfm_segment ON customer_rfm (segment);
