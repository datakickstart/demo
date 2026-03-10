Generate synthetic retail order data using Databricks Connect with serverless.
Create 3 related tables with full referential integrity:
      - customers (10,000 rows): customer_id, name, email, membership_level (Bronze/Silver/Gold/Platinum weighted 50/30/15/5), region
      - orders (50,000 rows): order_id, customer_id (FK to customers), order_date, total_amount, status
      - line_items (150,000 rows): line_item_id, order_id (FK to orders), product_name, quantity, unit_price

Save as JSON files to Unity Catalog volume. Use catalog 'main'. Use schema name 'demo_dw_raw'.
Create realistic product names.
Higher membership levels should have more orders.
Order total_amount should equal sum of line_items.
