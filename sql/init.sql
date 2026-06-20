-- ──────────────────────────────────────────────────────────────────────────
-- sdd-kafka-databricks — PostgreSQL initialization
-- Platform: Uber Eats food delivery (Brazilian market)
-- Tables: 20 domains from Kafka, MongoDB, MySQL, MSSQL, PostgreSQL sources
-- Total records across 100 files: ~129,353
-- WAL level is set via docker-compose command flags (-c wal_level=logical)
-- Schema adapted from sdd-kafka-snowflake/scripts/init.sql — verified field by
-- field against contracts/*.yml (types, nullability, special cases).
-- ──────────────────────────────────────────────────────────────────────────

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Debezium requires a replication slot and publication.
-- The slot is created by the connector on first connect; the publication is pre-created here.
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput')
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name = 'debezium_slot'
);

-- ════════════════════════════════════════════════════════════════════════════
-- 1. PAYMENT EVENTS (kafka_events) — event sourcing, 2,208 records
--    Nested event object: {event_name, timestamp (int or float)}
--    CDC lifecycle: created→authorized→captured→succeeded→settled→closed
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS payment_events (
    event_id             UUID        NOT NULL,
    payment_id           UUID        NOT NULL,
    event                JSONB       NOT NULL,  -- {event_name, timestamp}
    dt_current_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_payment_events PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_payment_events_payment_id
    ON payment_events (payment_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 2. ORDERS (kafka_orders) — entity, 405 records
--    Hub table — links all domains via *_key foreign references
--    Keys use business identifiers: CPF (user), CNPJ (restaurant), driver_id
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS orders (
    order_id             UUID           NOT NULL,
    order_date           TIMESTAMPTZ,
    total_amount         NUMERIC(10,2),
    user_key             VARCHAR(20),   -- CPF format: 000.000.000-00
    restaurant_key       VARCHAR(20),   -- CNPJ format: 00.000.000/0000-00
    driver_key           VARCHAR(20),   -- driver_id string
    payment_key          UUID,          -- payment_id reference
    rating_key           UUID,          -- rating_id reference
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);
CREATE INDEX IF NOT EXISTS idx_orders_user_key       ON orders (user_key);
CREATE INDEX IF NOT EXISTS idx_orders_restaurant_key ON orders (restaurant_key);
CREATE INDEX IF NOT EXISTS idx_orders_driver_key     ON orders (driver_key);
CREATE INDEX IF NOT EXISTS idx_orders_date           ON orders (order_date);

-- ════════════════════════════════════════════════════════════════════════════
-- 3. PAYMENTS (kafka_payments) — entity, 260 records
--    Full payment details: card, method, amounts, provider
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS payments (
    payment_id           UUID           NOT NULL,
    invoice_id           VARCHAR(20),
    order_key            UUID,          -- order_id reference
    method               VARCHAR(50),   -- Credit Card, Boleto, PIX, Wallet
    provider             VARCHAR(50),   -- Adyen, Stripe, etc.
    status               VARCHAR(30),   -- succeeded, failed, refunded
    amount               NUMERIC(10,2),
    net_amount           NUMERIC(10,2),
    tax_amount           NUMERIC(10,2),
    platform_fee         NUMERIC(10,2),
    provider_fee         NUMERIC(10,2),
    refund_amount        NUMERIC(10,2),
    currency             VARCHAR(10),
    country              VARCHAR(10),
    captured             BOOLEAN,
    refunded              BOOLEAN,
    card_brand           VARCHAR(30),
    card_last4           VARCHAR(4),
    card_exp_month       SMALLINT,
    card_exp_year        SMALLINT,
    wallet_provider      VARCHAR(50),
    failure_reason       VARCHAR(200),
    receipt_url          TEXT,
    ip_address           VARCHAR(45),
    user_agent           TEXT,
    timestamp             TIMESTAMPTZ,
    capture_timestamp    TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_payments PRIMARY KEY (payment_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 4. ORDER ITEMS (mongodb_items) — fact, 110,001 records (largest table)
--    Line items per order: product, quantity, price, modifiers
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id        UUID           NOT NULL,
    order_id             UUID,
    product_id           VARCHAR(20),   -- PRD-XXXXX format
    restaurant_id        INTEGER,
    product_name         VARCHAR(200),
    product_type         VARCHAR(50),
    cuisine_type         VARCHAR(50),
    unit_price           NUMERIC(10,2),
    quantity             INTEGER,
    subtotal             NUMERIC(10,2),
    discount_applied     NUMERIC(10,2),
    modifiers            VARCHAR(500),  -- plain string e.g. "no ice"
    is_combo             BOOLEAN,
    is_vegetarian        BOOLEAN,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_order_items PRIMARY KEY (order_item_id)
);
CREATE INDEX IF NOT EXISTS idx_order_items_order_id      ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id    ON order_items (product_id);
CREATE INDEX IF NOT EXISTS idx_order_items_restaurant_id ON order_items (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 5. GPS EVENTS (kafka_gps) — event sourcing, 7,350 records
--    High-frequency location tracking linked to order_id
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS gps_events (
    gps_id               UUID           NOT NULL,
    order_id             UUID,
    lat                  DOUBLE PRECISION,
    lon                  DOUBLE PRECISION,
    altitude             DOUBLE PRECISION,
    speed_kph            NUMERIC(6,2),
    direction_deg        DOUBLE PRECISION,
    accuracy_m           NUMERIC(6,2),
    duration_ms          DOUBLE PRECISION,
    timestamp             TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_gps_events PRIMARY KEY (gps_id)
);
CREATE INDEX IF NOT EXISTS idx_gps_events_order_id  ON gps_events (order_id);
CREATE INDEX IF NOT EXISTS idx_gps_events_timestamp ON gps_events (timestamp);

-- ════════════════════════════════════════════════════════════════════════════
-- 6. ORDER STATUS (kafka_status) — event sourcing, 4,176 records
--    Nested status object: {status_name, timestamp}; status_id is INTEGER (not UUID)
--    Tracks order lifecycle: placed, preparing, dispatched, delivered
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS order_status (
    status_id            INTEGER        NOT NULL,
    order_identifier     UUID,          -- order_id reference
    status               JSONB          NOT NULL,  -- {status_name, timestamp}
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_order_status PRIMARY KEY (status_id)
);
CREATE INDEX IF NOT EXISTS idx_order_status_order_id ON order_status (order_identifier);

-- ════════════════════════════════════════════════════════════════════════════
-- 7. ROUTES (kafka_route) — entity, 410 records
--    Delivery route per order: start/end coordinates, distance, duration
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS routes (
    route_id                 UUID           NOT NULL,
    order_id                 UUID,
    driver_id                VARCHAR(20),
    start_lat                DOUBLE PRECISION,
    start_lon                DOUBLE PRECISION,
    end_lat                  DOUBLE PRECISION,
    end_lon                  DOUBLE PRECISION,
    distance_km              NUMERIC(8,2),
    estimated_duration_min   NUMERIC(6,2),
    start_time               TIMESTAMPTZ,
    end_time                 TIMESTAMPTZ,
    dt_current_timestamp     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_routes PRIMARY KEY (route_id)
);
CREATE INDEX IF NOT EXISTS idx_routes_order_id  ON routes (order_id);
CREATE INDEX IF NOT EXISTS idx_routes_driver_id ON routes (driver_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 8. RECEIPTS (kafka_receipts) — entity, 377 records
--    Financial receipt per order (no dt_current_timestamp — uses receipt_generated_at)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS receipts (
    receipt_id           UUID           NOT NULL,
    order_id             UUID,
    payment_id           UUID,
    total_amount         NUMERIC(10,2),
    item_count           INTEGER,
    receipt_generated_at TIMESTAMPTZ,
    CONSTRAINT pk_receipts PRIMARY KEY (receipt_id)
);
CREATE INDEX IF NOT EXISTS idx_receipts_order_id   ON receipts (order_id);
CREATE INDEX IF NOT EXISTS idx_receipts_payment_id ON receipts (payment_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 9. DRIVER SHIFTS (kafka_shift) — entity, 468 records
--    Shift performance: earnings, distance, orders, ratings
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS driver_shifts (
    shift_id             UUID           NOT NULL,
    driver_id            VARCHAR(20),
    city                 VARCHAR(100),
    region               VARCHAR(100),
    shift_type           VARCHAR(20),   -- full-time, part-time
    login_method         VARCHAR(30),
    device_os            VARCHAR(30),
    start_time           TIMESTAMPTZ,
    end_time             TIMESTAMPTZ,
    shift_duration_min   INTEGER,
    num_orders           INTEGER,
    distance_covered_km  NUMERIC(8,2),
    earnings_brl         NUMERIC(10,2),
    shift_rating         NUMERIC(3,2),
    issues_reported      VARCHAR(100),  -- categorical: 'Late Start', 'App Crash', 'None', etc.
    available            BOOLEAN,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_driver_shifts PRIMARY KEY (shift_id)
);
CREATE INDEX IF NOT EXISTS idx_driver_shifts_driver_id  ON driver_shifts (driver_id);
CREATE INDEX IF NOT EXISTS idx_driver_shifts_start_time ON driver_shifts (start_time);

-- ════════════════════════════════════════════════════════════════════════════
-- 10. SEARCH EVENTS (kafka_search) — event sourcing, 202 records
--     User search queries (filters is plain string, not nested)
--     No dt_current_timestamp — uses timestamp instead
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS search_events (
    search_id            UUID           NOT NULL,
    user_id              INTEGER,
    query_text           TEXT,
    filters              VARCHAR(200),
    result_count         INTEGER,
    clicked_product_id   VARCHAR(20),
    timestamp             TIMESTAMPTZ,
    CONSTRAINT pk_search_events PRIMARY KEY (search_id)
);
CREATE INDEX IF NOT EXISTS idx_search_events_user_id ON search_events (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 11. RECOMMENDATIONS (mongodb_recommendations) — event sourcing, 254 records
--     ML recommendation events: view, click, purchase
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS recommendations (
    event_id             UUID           NOT NULL,
    user_id              INTEGER,
    product_id           VARCHAR(20),
    event_type           VARCHAR(50),   -- view, click, purchase, dismiss
    timestamp             TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_recommendations PRIMARY KEY (event_id)
);
CREATE INDEX IF NOT EXISTS idx_recommendations_user_id    ON recommendations (user_id);
CREATE INDEX IF NOT EXISTS idx_recommendations_product_id ON recommendations (product_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 12. SUPPORT TICKETS (mongodb_support) — entity, 410 records
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS support_tickets (
    ticket_id            UUID           NOT NULL,
    user_id              INTEGER,
    order_id             UUID,
    category             VARCHAR(100),
    description          TEXT,
    status               VARCHAR(30),
    opened_at            TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_support_tickets PRIMARY KEY (ticket_id)
);
CREATE INDEX IF NOT EXISTS idx_support_tickets_user_id  ON support_tickets (user_id);
CREATE INDEX IF NOT EXISTS idx_support_tickets_order_id ON support_tickets (order_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 13. USERS — MongoDB source (mongodb_users) — 411 records
--     CPF is the user_key used in orders table
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS users_mongo (
    uuid                 UUID           NOT NULL,
    user_id              INTEGER,
    cpf                  VARCHAR(20),   -- user_key in orders
    email                VARCHAR(200),
    phone_number         VARCHAR(30),
    city                 VARCHAR(100),
    country              VARCHAR(10),
    delivery_address     TEXT,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_users_mongo PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_users_mongo_cpf     ON users_mongo (cpf);
CREATE INDEX IF NOT EXISTS idx_users_mongo_user_id ON users_mongo (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 14. USERS — MSSQL source (mssql_users) — 288 records
--     Extended profile: birthday, job, company — same CPF key as users_mongo
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS users_mssql (
    uuid                 UUID           NOT NULL,
    user_id              INTEGER,
    cpf                  VARCHAR(20),
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    phone_number         VARCHAR(30),
    birthday             DATE,
    job                  VARCHAR(200),
    company_name         VARCHAR(200),
    country              VARCHAR(10),
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_users_mssql PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_users_mssql_cpf     ON users_mssql (cpf);
CREATE INDEX IF NOT EXISTS idx_users_mssql_user_id ON users_mssql (user_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 15. RESTAURANTS (mysql_restaurants) — entity, 461 records
--     CNPJ is the restaurant_key used in orders table
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS restaurants (
    uuid                 UUID           NOT NULL,
    restaurant_id        INTEGER,
    cnpj                 VARCHAR(20),   -- restaurant_key in orders
    name                 VARCHAR(200),
    address              TEXT,
    city                 VARCHAR(100),
    country              VARCHAR(10),
    phone_number         VARCHAR(30),
    cuisine_type         VARCHAR(100),
    opening_time         TIME,
    closing_time         TIME,
    average_rating       NUMERIC(3,2),
    num_reviews          INTEGER,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_restaurants PRIMARY KEY (uuid)
);
CREATE INDEX IF NOT EXISTS idx_restaurants_cnpj          ON restaurants (cnpj);
CREATE INDEX IF NOT EXISTS idx_restaurants_restaurant_id ON restaurants (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 16. DRIVERS (postgres_drivers) — entity, 354 records
--     driver_id (string) is the driver_key used in orders and shifts
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS drivers (
    uuid                 UUID           NOT NULL,
    driver_id            VARCHAR(20),   -- driver_key in orders
    first_name           VARCHAR(100),
    last_name            VARCHAR(100),
    phone_number         VARCHAR(30),
    city                 VARCHAR(100),
    country              VARCHAR(10),
    date_birth           DATE,
    license_number       VARCHAR(50),
    vehicle_type         VARCHAR(50),
    vehicle_make         VARCHAR(50),
    vehicle_model        VARCHAR(50),
    vehicle_year         INTEGER,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_drivers PRIMARY KEY (uuid)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_drivers_driver_id ON drivers (driver_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 17. PRODUCTS (mysql_products) — entity, 368 records
--     product_id (PRD-XXXXX) is the FK in order_items and recommendations
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS products (
    product_id           VARCHAR(20)    NOT NULL,
    restaurant_id        INTEGER,
    name                 VARCHAR(200),
    product_type         VARCHAR(50),
    cuisine_type         VARCHAR(100),
    flavor_profile       VARCHAR(200),
    tags                 VARCHAR(500),
    price                NUMERIC(10,2),
    unit_cost            NUMERIC(10,2),
    calories             INTEGER,
    prep_time_min        INTEGER,
    is_vegetarian        BOOLEAN,
    is_gluten_free       BOOLEAN,
    created_at           TIMESTAMPTZ,
    updated_at           TIMESTAMPTZ,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_products PRIMARY KEY (product_id)
);
CREATE INDEX IF NOT EXISTS idx_products_restaurant_id ON products (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 18. MENU SECTIONS (mysql_menu) — entity, 362 records
--     Organizes products into restaurant menu sections
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS menu_sections (
    menu_section_id      UUID           NOT NULL,
    restaurant_id        INTEGER,
    name                 VARCHAR(200),
    description          TEXT,
    active               BOOLEAN,
    dt_current_timestamp TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_menu_sections PRIMARY KEY (menu_section_id)
);
CREATE INDEX IF NOT EXISTS idx_menu_sections_restaurant_id ON menu_sections (restaurant_id);

-- ════════════════════════════════════════════════════════════════════════════
-- 19. RATINGS (mysql_ratings) — entity, 327 records
--     Restaurant ratings — rating_key in orders references uuid here
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ratings (
    rating_id             INTEGER        NOT NULL,
    uuid                  UUID,          -- rating_key in orders = this uuid
    restaurant_identifier VARCHAR(20),   -- CNPJ or restaurant_id
    rating                NUMERIC(3,2),
    timestamp             TIMESTAMPTZ,
    dt_current_timestamp  TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_ratings PRIMARY KEY (rating_id)
);

-- ════════════════════════════════════════════════════════════════════════════
-- 20. INVENTORY (postgres_inventory) — entity, 261 records
--     Stock levels per product per restaurant
--     No dt_current_timestamp — uses last_updated instead
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS inventory (
    stock_id             UUID           NOT NULL,
    restaurant_id        INTEGER,
    product_id           VARCHAR(20),
    quantity_available   INTEGER,
    last_updated         TIMESTAMPTZ,
    CONSTRAINT pk_inventory PRIMARY KEY (stock_id)
);
CREATE INDEX IF NOT EXISTS idx_inventory_restaurant_id ON inventory (restaurant_id);
CREATE INDEX IF NOT EXISTS idx_inventory_product_id    ON inventory (product_id);

-- ════════════════════════════════════════════════════════════════════════════
-- REPLICA IDENTITY FULL on all 20 tables — required for correct DELETE
-- propagation (DESIGN_DELETE_HANDLING.md Decision 1). Without this, Postgres's
-- default REPLICA IDENTITY only logs the primary key in a DELETE's
-- before-image; every other column arrives NULL in Kafka, which either gets
-- swallowed by an unrelated quarantine quality-rule (every one of the 10
-- generic Silver domains has at least one) or — for users_mongo/users_mssql,
-- whose join key `cpf` is not the Postgres PK `uuid` — breaks cpf_key
-- derivation entirely. FULL makes every column survive into the delete event,
-- so create_auto_cdc_flow's apply_as_deletes can act on real data instead of
-- NULLs. See kb/anti-patterns.md (C08) and CLAUDE.md for the original gap.
-- ════════════════════════════════════════════════════════════════════════════
ALTER TABLE payment_events   REPLICA IDENTITY FULL;
ALTER TABLE orders           REPLICA IDENTITY FULL;
ALTER TABLE payments         REPLICA IDENTITY FULL;
ALTER TABLE order_items      REPLICA IDENTITY FULL;
ALTER TABLE gps_events       REPLICA IDENTITY FULL;
ALTER TABLE order_status     REPLICA IDENTITY FULL;
ALTER TABLE routes           REPLICA IDENTITY FULL;
ALTER TABLE receipts         REPLICA IDENTITY FULL;
ALTER TABLE driver_shifts    REPLICA IDENTITY FULL;
ALTER TABLE search_events    REPLICA IDENTITY FULL;
ALTER TABLE recommendations  REPLICA IDENTITY FULL;
ALTER TABLE support_tickets  REPLICA IDENTITY FULL;
ALTER TABLE users_mongo      REPLICA IDENTITY FULL;
ALTER TABLE users_mssql      REPLICA IDENTITY FULL;
ALTER TABLE restaurants      REPLICA IDENTITY FULL;
ALTER TABLE drivers          REPLICA IDENTITY FULL;
ALTER TABLE products         REPLICA IDENTITY FULL;
ALTER TABLE menu_sections    REPLICA IDENTITY FULL;
ALTER TABLE ratings          REPLICA IDENTITY FULL;
ALTER TABLE inventory        REPLICA IDENTITY FULL;

-- ════════════════════════════════════════════════════════════════════════════
-- Publication for Debezium CDC — FOR ALL TABLES covers all 20 domains above
-- and any future table without editing this publication (DESIGN Decision 1/2).
-- connectors/debezium.json.table.include.list still needs the explicit table
-- name when a new domain is added — FOR ALL TABLES only covers the Postgres side.
-- ════════════════════════════════════════════════════════════════════════════
CREATE PUBLICATION debezium_publication FOR ALL TABLES;
