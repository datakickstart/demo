Generate a Serverless Spark Declarative Pipeline using SQL syntax when possible. This should read the raw JSON data in main.demo_dw_raw.raw_data volume, following the following instructions.
1. Use a separate UC schema for bronze, silver, and gold all within the main catalog. Use new schemas during development and if you need to iterate then cleanup last used schema before retrying.
2. Bronze tables should use schema evolution and add basic ingest time and source file metadata.
3. Build, validate, deploy, and test bronze first and show results before continuing to other steps.
4. Create silver tables as streaming tables that are cleaned and enriched, but do not deduplicate at this layer.
5. In gold tables, treat small tables as SCD type 2 and treat the larger tables as SCD type 1. 
6. Simulate 2 aggregate tables in gold demonstrating common metrics for this type of data set.
