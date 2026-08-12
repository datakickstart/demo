-- Unity Catalog Metric View over the gold star schema.
--
-- Deploy (profile DEFAULT) -- see deploy.sh next to this file:
--   ./pipelines/nyctaxi-medallion/resources/metric_views/deploy.sh <WAREHOUSE_ID>
--
-- Do NOT deploy with `databricks experimental aitools tools statement submit
-- --file`: it strips the YAML block's leading indentation, and the server then
-- rejects the definition with METRIC_VIEW_INVALID_VIEW_DEFINITION / "Failed to
-- parse YAML ... expected <block end>, but found '-'". Submitting the DDL as a
-- JSON-encoded statement to /api/2.0/sql/statements/ preserves whitespace
-- exactly, which is what deploy.sh does.
--
-- Databricks Asset Bundles (CLI v1.9.0) has no native `metric_views` resource
-- type, so the metric view is deployed as DDL rather than as a bundle resource.
-- It is kept here, next to the pipeline that produces its source tables, so the
-- semantic layer versions together with the gold schema it depends on.
--
-- Requires DBR 17.2+ for YAML version 1.1 (needed for per-column `comment`);
-- `window` measures are still flagged experimental but work on 1.1.
--
-- PERFORMANCE NOTES
-- Grain: one row per fact_trips row, with the dimensions joined at query time by
-- the metric view itself. Deliberately NOT pre-joined into a wide denormalized
-- table -- the joins are many-to-one on surrogate keys, so the engine prunes any
-- dimension a given query does not group by, and a single wide table would
-- instead force every query to scan the widest possible row.
-- * fact_trips is already liquid-clustered on (date_sk, pickup_zone_sk), the two
--   highest-cardinality join keys this view groups by, so date- and pickup-zone
--   filters get file skipping. The dimensions are small enough to broadcast.
-- * Serverless SQL warehouses run Photon and cache results, so repeated
--   dashboard reads of the same slice are served from cache.
-- * `materialization:` (experimental) would pre-compute chosen dimension/measure
--   combinations via a hidden Lakeflow pipeline. Left OFF deliberately: at 21,847
--   fact rows the joins are already sub-second, and enabling it creates an extra
--   always-on pipeline to own and pay for. Enable it if this grows by orders of
--   magnitude, e.g.:
--     materialization:
--       schedule: every 6 hours
--       mode: relaxed
--       materialized_views:
--         - name: month_time_of_day
--           type: aggregated
--           dimensions: [month, time_of_day]
--           measures: [total_revenue, trip_count]

CREATE OR REPLACE VIEW main.nyctaxi_gold.trips_metrics
WITH METRICS
LANGUAGE YAML
AS $$
version: 1.1
comment: "NYC taxi trip KPIs over the gold star schema (fact_trips + conformed dimensions)."
source: main.nyctaxi_gold.fact_trips

joins:
  - name: dim_date
    source: main.nyctaxi_gold.dim_date
    on: source.date_sk = dim_date.date_sk
  - name: pickup_zone
    source: main.nyctaxi_gold.dim_zone
    on: source.pickup_zone_sk = pickup_zone.zone_sk
  - name: dropoff_zone
    source: main.nyctaxi_gold.dim_zone
    on: source.dropoff_zone_sk = dropoff_zone.zone_sk
  - name: dim_time_of_day
    source: main.nyctaxi_gold.dim_time_of_day
    on: source.time_of_day_sk = dim_time_of_day.time_of_day_sk
  - name: dim_rider
    source: main.nyctaxi_gold.dim_rider
    on: source.rider_sk = dim_rider.rider_sk

dimensions:
  # --- date hierarchy: year > month > date, plus day-of-week rollup ---
  - name: date
    expr: dim_date.date
    comment: "Trip pickup date. Window-ordering dimension for trailing measures."
  - name: month
    expr: DATE_TRUNC('MONTH', dim_date.date)
    comment: "First day of the pickup month (month rollup level)."
  - name: year
    expr: dim_date.year
    comment: "Pickup year (top rollup level)."
  - name: day_of_week
    expr: dim_date.day_name
    comment: "Day-of-week name rollup (Monday..Sunday)."
  - name: day_of_week_number
    expr: dim_date.day_of_week
    comment: "Day-of-week ordinal, for sorting day_of_week correctly."
  - name: is_weekend
    expr: dim_date.is_weekend

  # --- pickup / dropoff location, from the two dim_zone joins ---
  - name: pickup_zip
    expr: pickup_zone.zip
  - name: pickup_borough
    expr: pickup_zone.borough
    comment: "ZIP-range-derived borough of the pickup ZIP."
  - name: dropoff_zip
    expr: dropoff_zone.zip
  - name: dropoff_borough
    expr: dropoff_zone.borough

  # --- time of day ---
  - name: time_of_day
    expr: dim_time_of_day.time_of_day
    comment: "morning (6-11), afternoon (12-17), evening (18-22), night (23-5)."
  - name: time_of_day_order
    expr: dim_time_of_day.sort_order
    comment: "Chronological sort order for time_of_day."

  # --- rider: rider_id is the grain. No name/email/phone/card exposed here. ---
  - name: rider_id
    expr: dim_rider.rider_id
    comment: "Synthetic rider surrogate/natural key. PII columns are intentionally not exposed."
  - name: rider_home_zip
    expr: dim_rider.home_zip
    comment: "Coarse geography only; safe to group by."

measures:
  - name: total_revenue
    expr: SUM(fare_amount)
    comment: "Total fare revenue."
  - name: trip_count
    expr: COUNT(1)
    comment: "Number of trips."
  - name: average_fare
    expr: AVG(fare_amount)
    comment: "Mean fare per trip."
  - name: total_miles
    expr: SUM(trip_distance)
    comment: "Total distance travelled."
  - name: revenue_per_mile
    # try_divide, not `/`: a slice with zero total miles yields NULL rather than
    # raising DIVIDE_BY_ZERO under ANSI mode. Ratio of sums (not a mean of
    # per-trip ratios) so it re-aggregates correctly at any grouping.
    expr: try_divide(SUM(fare_amount), SUM(trip_distance))
    comment: "Revenue per mile = SUM(fare_amount) / SUM(trip_distance). NULL when a slice has zero miles."
  - name: average_trip_duration
    expr: AVG(trip_duration_minutes)
    comment: "Mean trip duration in minutes."
  - name: cross_borough_share
    # is_cross_borough is NULL when the ZIP-range proxy cannot place an end of the
    # trip. NULL rows are excluded from BOTH numerator and denominator, so the
    # measure reads as "share of trips we could classify that crossed a borough"
    # rather than silently counting unknowns as same-borough. try_divide guards a
    # slice where every row is unclassified (denominator 0 -> NULL, not an error).
    expr: try_divide(
        COUNT(1) FILTER (WHERE is_cross_borough),
        COUNT(1) FILTER (WHERE is_cross_borough IS NOT NULL)
      )
    comment: "Fraction of classifiable trips that crossed a borough. NULL rows excluded from numerator and denominator."
  - name: trailing_7day_revenue
    expr: SUM(fare_amount)
    window:
      - order: date
        range: trailing 7 day
        semiadditive: last
    comment: "Rolling revenue over the 7 days preceding each date (trailing excludes the current date)."
$$
