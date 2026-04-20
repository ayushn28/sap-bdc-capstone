-- =============================================================
-- models/sql/fact_sales.sql
-- SAP BDC (Datasphere) — Fact Table: Sales Orders
-- Analytical Dataset view for the O2C Revenue Analytics space
-- =============================================================

CREATE OR REPLACE VIEW "BDC_CAPSTONE"."FACT_SALES" AS
SELECT
    -- Surrogate Key
    FACT_KEY,

    -- Order Identifiers
    ORDER_ID,
    ORDER_DATE,

    -- Date Dimensions (inline for Datasphere performance)
    YEAR(ORDER_DATE)          AS ORDER_YEAR,
    MONTH(ORDER_DATE)         AS ORDER_MONTH,
    QUARTER(ORDER_DATE)       AS ORDER_QUARTER,
    DAYOFWEEK(ORDER_DATE)     AS ORDER_DOW,

    -- Foreign Keys (Dimension references)
    CUSTOMER_ID,
    PRODUCT_ID,

    -- Degenerate Dimensions
    REGION,
    STATUS,
    STATUS_GROUP,
    SALES_REP,
    CURRENCY,

    -- Measures
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_PCT,
    DISCOUNT_AMOUNT,
    GROSS_VALUE,
    NET_VALUE,
    REVENUE,                   -- 0 for Cancelled orders

    -- Flags
    REVENUE_ELIGIBLE,
    IS_WEEKEND,

    -- Audit
    LOAD_TIMESTAMP

FROM "BDC_CAPSTONE"."STG_SALES_ORDERS"
WHERE LOAD_TIMESTAMP = (
    SELECT MAX(LOAD_TIMESTAMP) FROM "BDC_CAPSTONE"."STG_SALES_ORDERS"
);
