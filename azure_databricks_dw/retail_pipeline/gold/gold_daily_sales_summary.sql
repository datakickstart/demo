-- Gold Aggregate 1: Daily sales KPIs.
-- Reads from orders_current (SCD1) — one row per calendar day.
-- Metrics: volume, revenue, avg/min/max order value, status breakdown, at-risk vs realized revenue.
CREATE OR REFRESH MATERIALIZED VIEW main.gold_retail_dev.daily_sales_summary
  CLUSTER BY (order_date)
  COMMENT 'Daily retail sales KPIs. Source: gold orders_current. Refreshes on each pipeline run.'
AS
SELECT
  order_date,
  COUNT(DISTINCT order_id)                                                    AS total_orders,
  COUNT(DISTINCT customer_id)                                                 AS unique_customers,
  ROUND(SUM(total_amount),  2)                                                AS total_revenue,
  ROUND(AVG(total_amount),  2)                                                AS avg_order_value,
  ROUND(MIN(total_amount),  2)                                                AS min_order_value,
  ROUND(MAX(total_amount),  2)                                                AS max_order_value,
  ROUND(STDDEV(total_amount), 2)                                              AS stddev_order_value,
  -- Order status counts
  SUM(CASE WHEN status = 'Delivered'  THEN 1 ELSE 0 END)                     AS delivered_count,
  SUM(CASE WHEN status = 'Shipped'    THEN 1 ELSE 0 END)                     AS shipped_count,
  SUM(CASE WHEN status = 'Processing' THEN 1 ELSE 0 END)                     AS processing_count,
  SUM(CASE WHEN status = 'Pending'    THEN 1 ELSE 0 END)                     AS pending_count,
  SUM(CASE WHEN status = 'Cancelled'  THEN 1 ELSE 0 END)                     AS cancelled_count,
  SUM(CASE WHEN status = 'Returned'   THEN 1 ELSE 0 END)                     AS returned_count,
  -- Revenue buckets
  ROUND(SUM(CASE WHEN status = 'Delivered'              THEN total_amount ELSE 0 END), 2) AS realized_revenue,
  ROUND(SUM(CASE WHEN status IN ('Cancelled','Returned') THEN total_amount ELSE 0 END), 2) AS at_risk_revenue,
  ROUND(SUM(CASE WHEN status IN ('Shipped','Processing','Pending') THEN total_amount ELSE 0 END), 2) AS pipeline_revenue
FROM main.gold_retail_dev.orders_current
GROUP BY order_date;
