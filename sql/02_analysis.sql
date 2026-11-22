-- =====================================================================
-- E-Commerce Profit Leakage & RFM Engine
-- 02_analysis.sql  |  PostgreSQL 16  |  run after src/etl_pipeline.py
-- Eight business queries. Thresholds come from olist.assumptions, so
-- the SQL, Python and Power BI always use the same numbers.
-- =====================================================================
SET search_path TO olist;


-- ---------------------------------------------------------------------
-- Q1  Executive KPI scorecard (all problems)            CTEs, FILTER
--     Cross-checks the Python KPI summary.
-- ---------------------------------------------------------------------
WITH item_status AS (
    SELECT i.*, o.status_group
    FROM order_items i
    JOIN orders o USING (order_id)
),
pay_ok AS (
    SELECT p.*
    FROM payments p
    JOIN orders o USING (order_id)
    WHERE o.status_group <> 'Failed' AND p.payment_value > 0
)
SELECT
    ROUND(SUM(price + freight_value) FILTER (WHERE status_group <> 'Failed'), 2)          AS gmv_brl,
    ROUND(SUM(price) FILTER (WHERE status_group = 'Delivered'), 2)                        AS product_revenue_brl,
    ROUND(SUM(freight_value) FILTER (WHERE status_group = 'Delivered')
        / SUM(price) FILTER (WHERE status_group = 'Delivered'), 4)                        AS freight_to_price,
    (SELECT ROUND(AVG((status_group = 'Failed')::int), 4) FROM orders)                    AS failure_rate,
    (SELECT ROUND(1 - AVG(is_late::int), 4) FROM orders WHERE is_late IS NOT NULL)        AS on_time_rate,
    (SELECT ROUND(AVG((review_score <= 2)::int), 4) FROM reviews)                         AS negative_review_rate,
    (SELECT ROUND(AVG(recon_exception::int), 4) FROM orders
      WHERE item_count > 0 AND paid_value > 0)                                            AS recon_exception_rate,
    (SELECT ROUND(SUM(payment_value) FILTER (WHERE payment_type = 'voucher')
        / SUM(payment_value), 4) FROM pay_ok)                                             AS voucher_dependency,
    (SELECT ROUND(AVG((frequency >= 2)::int), 4) FROM customer_rfm)                       AS repeat_rate
FROM item_status;


-- ---------------------------------------------------------------------
-- Q2  P1 Freight leakage by customer state             window RANK, share
--     Which states pay the most freight per BRL of product?
-- ---------------------------------------------------------------------
WITH by_state AS (
    SELECT c.region, c.state,
           COUNT(*)                         AS lines,
           SUM(i.price)                     AS price_brl,
           SUM(i.freight_value)             AS freight_brl,
           AVG(i.high_freight::int)         AS high_freight_share
    FROM order_items i
    JOIN orders o    USING (order_id)
    JOIN customers c USING (customer_id)
    WHERE o.status_group = 'Delivered'
    GROUP BY c.region, c.state
)
SELECT region, state, lines,
       ROUND(freight_brl / price_brl, 4)                                   AS freight_ratio,
       ROUND(freight_brl / price_brl
             - SUM(freight_brl) OVER () / SUM(price_brl) OVER (), 4)       AS vs_national,
       ROUND(high_freight_share, 4)                                        AS high_freight_share,
       ROUND(freight_brl, 0)                                               AS freight_brl,
       RANK() OVER (ORDER BY freight_brl / price_brl DESC)                 AS leakage_rank
FROM by_state
ORDER BY leakage_rank;


-- ---------------------------------------------------------------------
-- Q3  P1 Freight leakage by category                    Pareto, running total
--     Which categories create most of the high-freight lines?
--     (categories with >= 100 delivered lines)
-- ---------------------------------------------------------------------
WITH by_cat AS (
    SELECT p.category_en,
           COUNT(*)                                AS lines,
           SUM(i.freight_value) / SUM(i.price)     AS freight_ratio,
           SUM(i.high_freight::int)                AS high_freight_lines
    FROM order_items i
    JOIN orders o   USING (order_id)
    JOIN products p USING (product_id)
    WHERE o.status_group = 'Delivered'
    GROUP BY p.category_en
    HAVING COUNT(*) >= 100
)
SELECT category_en, lines,
       ROUND(freight_ratio, 4)                                              AS freight_ratio,
       high_freight_lines,
       ROUND(100.0 * high_freight_lines / lines, 1)                         AS pct_lines_high_freight,
       ROUND(100.0 * SUM(high_freight_lines) OVER (ORDER BY high_freight_lines DESC
                                                   ROWS UNBOUNDED PRECEDING)
             / SUM(high_freight_lines) OVER (), 1)                          AS cum_pct_of_high_freight
FROM by_cat
ORDER BY high_freight_lines DESC
LIMIT 10;


-- ---------------------------------------------------------------------
-- Q4  P2 Late delivery -> review score                  CASE, join
--     The core story: how fast does satisfaction fall with each day late?
-- ---------------------------------------------------------------------
SELECT o.delay_bucket,
       COUNT(*)                                          AS orders,
       ROUND(AVG(r.review_score), 2)                     AS avg_score,
       ROUND(100.0 * AVG((r.review_score <= 2)::int), 1) AS pct_negative,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pct_of_orders
FROM orders o
JOIN reviews r USING (order_id)
WHERE o.delay_bucket IS NOT NULL
GROUP BY o.delay_bucket
ORDER BY MIN(o.delay_days);


-- ---------------------------------------------------------------------
-- Q5  P2 Seller risk list                               CTE, NTILE, params
--     Sellers that hand over late often enough to hurt reviews.
-- ---------------------------------------------------------------------
WITH params AS (
    SELECT MAX(value) FILTER (WHERE name = 'seller_late_flag') AS late_flag,
           MAX(value) FILTER (WHERE name = 'min_seller_items') AS min_items
    FROM assumptions
),
seller_stats AS (
    SELECT i.seller_id, s.state,
           COUNT(*)                          AS items,
           AVG(i.late_handover::int)         AS late_rate,
           SUM(i.price)                      AS revenue_brl,
           AVG(r.review_score)               AS avg_review
    FROM order_items i
    JOIN sellers s      USING (seller_id)
    LEFT JOIN reviews r USING (order_id)
    WHERE i.late_handover IS NOT NULL
    GROUP BY i.seller_id, s.state
)
SELECT seller_id, state, items,
       ROUND(late_rate, 3)                               AS late_rate,
       ROUND(revenue_brl, 0)                             AS revenue_brl,
       ROUND(avg_review, 2)                              AS avg_review,
       NTILE(4) OVER (ORDER BY revenue_brl DESC)         AS revenue_quartile   -- 1 = biggest sellers
FROM seller_stats, params
WHERE items >= min_items AND late_rate > late_flag
ORDER BY revenue_brl DESC
LIMIT 15;


-- ---------------------------------------------------------------------
-- Q6  P3 Reconciliation exceptions by cause             CASE classification
--     Positive gaps on instalment cards look like interest; negative gaps
--     mean the customer paid less than billed.
-- ---------------------------------------------------------------------
WITH pay_profile AS (
    SELECT order_id,
           MAX(installments)                         AS max_installments,
           BOOL_OR(payment_type = 'voucher')         AS used_voucher,
           COUNT(*)                                  AS payment_rows
    FROM payments
    GROUP BY order_id
)
SELECT CASE
           WHEN o.recon_gap > 0 AND p.max_installments > 1 THEN 'Overpaid - instalment card'
           WHEN o.recon_gap > 0                            THEN 'Overpaid - single payment'
           WHEN p.used_voucher                             THEN 'Underpaid - voucher order'
           ELSE                                                 'Underpaid - other'
       END                                   AS gap_type,
       COUNT(*)                              AS orders,
       ROUND(SUM(o.recon_gap), 2)            AS total_gap_brl,
       ROUND(AVG(o.recon_gap), 2)            AS avg_gap_brl,
       ROUND(AVG(p.payment_rows), 2)         AS avg_payment_rows
FROM orders o
JOIN pay_profile p USING (order_id)
WHERE o.recon_exception
GROUP BY 1
ORDER BY orders DESC;


-- ---------------------------------------------------------------------
-- Q7  P4 Fulfilment failures by month                   LAG, MoM change
-- ---------------------------------------------------------------------
WITH monthly AS (
    SELECT o.purchase_month,
           COUNT(*)                                                    AS orders,
           COUNT(*) FILTER (WHERE o.status_group = 'Failed')           AS failed_orders,
           COALESCE(SUM(o.billed_value) FILTER (WHERE o.status_group = 'Failed'), 0) AS lost_gmv_brl
    FROM orders o
    WHERE o.purchase_month BETWEEN DATE '2017-01-01' AND DATE '2018-08-01'  -- months with full data
    GROUP BY o.purchase_month
)
SELECT TO_CHAR(purchase_month, 'YYYY-MM')                              AS month,
       orders, failed_orders,
       ROUND(100.0 * failed_orders / orders, 2)                        AS failure_pct,
       ROUND(lost_gmv_brl, 0)                                          AS lost_gmv_brl,
       ROUND(100.0 * failed_orders / orders
             - LAG(100.0 * failed_orders / orders) OVER (ORDER BY purchase_month), 2) AS failure_pct_mom
FROM monthly
ORDER BY purchase_month;


-- ---------------------------------------------------------------------
-- Q8  P5 RFM segments and Revenue at Risk by region     GROUPING SETS
--     Share is of ALL customer revenue in the region (every segment).
-- ---------------------------------------------------------------------
WITH region_total AS (
    SELECT region, SUM(monetary) AS region_revenue FROM customer_rfm GROUP BY region
    UNION ALL
    SELECT NULL, SUM(monetary) FROM customer_rfm
),
seg AS (
    SELECT region, segment,
           COUNT(*)            AS customers,
           SUM(monetary)       AS revenue_brl,
           AVG(recency_days)   AS avg_recency_days
    FROM customer_rfm
    GROUP BY GROUPING SETS ((region, segment), (segment))
)
SELECT COALESCE(s.region, 'All regions')                                AS region,
       s.segment, s.customers,
       ROUND(s.revenue_brl, 0)                                          AS revenue_brl,
       ROUND(100.0 * s.revenue_brl / t.region_revenue, 1)               AS pct_of_region_revenue,
       ROUND(s.avg_recency_days, 0)                                     AS avg_recency_days
FROM seg s
JOIN region_total t ON t.region IS NOT DISTINCT FROM s.region
WHERE s.segment IN ('At Risk', 'Can''t Lose', 'Champions', 'Loyal', 'Big One-Timers')
ORDER BY s.region NULLS FIRST, s.revenue_brl DESC;
