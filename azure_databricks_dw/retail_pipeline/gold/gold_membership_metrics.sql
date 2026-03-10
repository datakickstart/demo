-- Gold Aggregate 2: Customer lifetime value and order metrics by membership tier.
-- Joins dim_customers (SCD2 current rows), orders_current, and line_items_current.
-- One row per membership level with full funnel metrics.
CREATE OR REFRESH MATERIALIZED VIEW main.gold_retail_dev.membership_metrics
  CLUSTER BY (membership_level)
  COMMENT 'LTV and order metrics by membership tier. Joins dim_customers (current), orders, and line items.'
AS
WITH current_customers AS (
  -- SCD Type 2: only current version of each customer record
  SELECT
    customer_id,
    name,
    membership_level,
    membership_tier_rank,
    region
  FROM main.gold_retail_dev.dim_customers
  WHERE __END_AT IS NULL
),
order_agg AS (
  SELECT
    c.membership_level,
    c.membership_tier_rank,
    COUNT(DISTINCT c.customer_id)                                             AS total_customers,
    COUNT(DISTINCT o.order_id)                                                AS total_orders,
    ROUND(SUM(o.total_amount),  2)                                            AS total_revenue,
    ROUND(AVG(o.total_amount),  2)                                            AS avg_order_value,
    ROUND(MIN(o.total_amount),  2)                                            AS min_order_value,
    ROUND(MAX(o.total_amount),  2)                                            AS max_order_value,
    SUM(CASE WHEN o.status = 'Delivered'  THEN 1 ELSE 0 END)                 AS delivered_orders,
    SUM(CASE WHEN o.status = 'Cancelled'  THEN 1 ELSE 0 END)                 AS cancelled_orders,
    SUM(CASE WHEN o.status = 'Returned'   THEN 1 ELSE 0 END)                 AS returned_orders
  FROM current_customers c
  LEFT JOIN main.gold_retail_dev.orders_current o ON c.customer_id = o.customer_id
  GROUP BY c.membership_level, c.membership_tier_rank
),
line_item_agg AS (
  SELECT
    c.membership_level,
    COUNT(li.line_item_id)                                                    AS total_line_items,
    ROUND(SUM(li.line_total),   2)                                            AS total_item_revenue,
    ROUND(AVG(li.unit_price),   2)                                            AS avg_unit_price,
    ROUND(AVG(li.quantity),     2)                                            AS avg_quantity_per_line
  FROM current_customers c
  LEFT JOIN main.gold_retail_dev.orders_current    o  ON c.customer_id = o.customer_id
  LEFT JOIN main.gold_retail_dev.line_items_current li ON o.order_id   = li.order_id
  GROUP BY c.membership_level
)
SELECT
  oa.membership_level,
  oa.membership_tier_rank,
  oa.total_customers,
  oa.total_orders,
  ROUND(oa.total_orders      / NULLIF(oa.total_customers, 0), 1)             AS orders_per_customer,
  oa.total_revenue,
  oa.avg_order_value,
  oa.min_order_value,
  oa.max_order_value,
  ROUND(oa.total_revenue     / NULLIF(oa.total_customers, 0), 2)             AS avg_ltv_per_customer,
  oa.delivered_orders,
  oa.cancelled_orders,
  oa.returned_orders,
  ROUND(oa.cancelled_orders * 100.0 / NULLIF(oa.total_orders, 0), 1)        AS cancellation_rate_pct,
  li.total_line_items,
  ROUND(li.total_line_items  / NULLIF(oa.total_orders, 0), 1)               AS avg_items_per_order,
  li.avg_unit_price,
  li.avg_quantity_per_line
FROM order_agg oa
JOIN line_item_agg li ON oa.membership_level = li.membership_level
ORDER BY oa.membership_tier_rank;
