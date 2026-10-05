# Conversion report: postcard_company

Converted from a dbt project on the `duckdb` adapter into a Dataform project for BigQuery by `dbt2dataform`.

## Summary

| | |
|---|---|
| Dataform `assertion` actions written | 15 |
| Dataform `incremental` actions written | 4 |
| Dataform `operations` actions written | 7 |
| Dataform `table` actions written | 19 |
| Needs manual work | 0 |
| Warnings | 10 |
| Notes | 20 |

## Variables to set in `workflow_settings.yaml`

| Variable | Came from | Value written |
|---|---|---|
| `calendar_end` | dbt var `calendar_end` | `2035-12-31` |
| `calendar_start` | dbt var `calendar_start` | `2010-01-01` |
| `input_files_path` | env var `INPUT_FILES_PATH` | `gs://your-bucket/parquet` |

## Warnings

- **parquet_input.channels, parquet_input.customers, parquet_input.main, parquet_input.products, parquet_input.resellers, parquet_input.resellers_type1, parquet_input.resellers_type2**: env_var('INPUT_FILES_PATH') became Dataform var `input_files_path`; Dataform reads vars from workflow_settings.yaml or `--vars`, not the environment.
- **staging_transactions_main, staging_reseller_type1_sales, staging_reseller_type2_sales**: cast to DECIMAL with scale 3 became NUMERIC, which keeps 9 decimal places; DuckDB rounds (a bare DuckDB NUMERIC is DECIMAL(18, 3))

## Notes

- **dbt parse**: env var `DUCKDB_FILE_PATH` was unset, so dbt parse ran with a placeholder for it.
- **dbt parse**: env var `INPUT_FILES_PATH` was unset, so dbt parse ran with a placeholder for it.
- **parquet_input.channels, parquet_input.customers, parquet_input.main, parquet_input.products, parquet_input.resellers, parquet_input.resellers_type1, parquet_input.resellers_type2**: BigQuery external tables over Parquet rename columns BigQuery cannot store (spaces, hyphens) unless flexible column names are enabled for the project; references were normalised to match.
- **parquet_input.main, parquet_input.resellers_type1, parquet_input.resellers_type2**: freshness warn_after dropped: Dataform assertions fail or pass, they do not warn.
- **dim_channel**: primary key constraint (channel_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **dim_sales_agent**: primary key constraint (sales_agent_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **dim_customer**: primary key constraint (customer_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **dim_date**: primary key constraint (date_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **dim_geography**: primary key constraint (geography_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **dim_product**: primary key constraint (product_key) added as an unenforced BigQuery constraint, as dbt-bigquery does.
- **grain_is_correct**: unnamed output column 2 named `count_1`; BigQuery requires a name.
- **tests**: 50 dbt tests became: 5 assertion files, 45 built-in assertions in the table's config.

## Renamed identifiers

An external table over Parquet renames these columns on read (BigQuery only keeps such names when flexible column names are enabled), so every reference was rewritten to match. `--keep-identifiers` turns this off.

| dbt | Dataform |
|---|---|
| `Created Date` | `Created_Date` |
| `Customer Address` | `Customer_Address` |
| `Customer City` | `Customer_City` |
| `Customer Country` | `Customer_Country` |
| `Customer Email` | `Customer_Email` |
| `Customer First Name` | `Customer_First_Name` |
| `Customer Last Name` | `Customer_Last_Name` |
| `Customer Phone` | `Customer_Phone` |
| `Customer Postal Code` | `Customer_Postal_Code` |
| `Product name` | `Product_name` |
| `Reseller ID` | `Reseller_ID` |
| `Sales Channel` | `Sales_Channel` |
| `Series City` | `Series_City` |
| `Total amount` | `Total_amount` |
| `Transaction ID` | `Transaction_ID` |
| `reseller-id` | `reseller_id` |

## File mapping

| dbt | Dataform | Type | Note |
|---|---|---|---|
| `tests/assert_commission_pct_valid.sql` | `definitions/assertions/assert_commission_pct_valid.sqlx` | assertion | singular test; overrides: `tags` |
| `tests/commissionpct_null_only_for_direct_sales.sql` | `definitions/assertions/commissionpct_null_only_for_direct_sales.sqlx` | assertion | singular test; overrides: `tags` |
| `models/core/dim/dim_channel.sql` | `definitions/assertions/contracts/dim_channel_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/dim/dim_customer.sql` | `definitions/assertions/contracts/dim_customer_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/dim/dim_date.sql` | `definitions/assertions/contracts/dim_date_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/dim/dim_geography.sql` | `definitions/assertions/contracts/dim_geography_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/dim/dim_product.sql` | `definitions/assertions/contracts/dim_product_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/dim/dim_sales_agent.sql` | `definitions/assertions/contracts/dim_sales_agent_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/core/fact/fact_sales.sql` | `definitions/assertions/contracts/fact_sales_contract.sqlx` | assertion | dbt contract; overrides: `tags` |
| `models/sources.yml` | `definitions/assertions/freshness/source_freshness_parquet_input_main.sqlx` | assertion | source freshness; overrides: `tags` |
| `models/sources.yml` | `definitions/assertions/freshness/source_freshness_parquet_input_resellers_type1.sqlx` | assertion | source freshness; overrides: `tags` |
| `models/sources.yml` | `definitions/assertions/freshness/source_freshness_parquet_input_resellers_type2.sqlx` | assertion | source freshness; overrides: `tags` |
| `models/schema.yml` | `definitions/assertions/generic/relationships_fact_sales_bought_date_key__date_key__ref_dim_date_.sqlx` | assertion | generic test `relationships`; overrides: `tags` |
| `tests/grain_is_correct.sql` | `definitions/assertions/grain_is_correct.sqlx` | assertion | singular test; overrides: `tags` |
| `tests/total_amount_is_non_negative.sql` | `definitions/assertions/total_amount_is_non_negative.sqlx` | assertion | singular test; overrides: `tags` |
| `models/core/dim/dim_channel.sql` | `definitions/core/dim/dim_channel.sqlx` | table | overrides: `tags` |
| `models/core/dim/dim_customer.sql` | `definitions/core/dim/dim_customer.sqlx` | table | overrides: `tags` |
| `models/core/dim/dim_date.sql` | `definitions/core/dim/dim_date.sqlx` | table | overrides: `tags` |
| `models/core/dim/dim_geography.sql` | `definitions/core/dim/dim_geography.sqlx` | table | overrides: `tags` |
| `models/core/dim/dim_product.sql` | `definitions/core/dim/dim_product.sqlx` | table | overrides: `tags` |
| `models/core/dim/dim_sales_agent.sql` | `definitions/core/dim/dim_sales_agent.sqlx` | table | overrides: `tags` |
| `models/core/fact/fact_sales.sql` | `definitions/core/fact/fact_sales.sqlx` | incremental | overrides: `bigquery`, `tags` |
| `models/raw/raw_channels.sql` | `definitions/raw/raw_channels.sqlx` | table | overrides: `tags` |
| `models/raw/raw_customers.sql` | `definitions/raw/raw_customers.sqlx` | table | overrides: `tags` |
| `models/raw/raw_products.sql` | `definitions/raw/raw_products.sqlx` | table | overrides: `tags` |
| `models/raw/raw_reseller_type1_sales.sql` | `definitions/raw/raw_reseller_type1_sales.sqlx` | table | overrides: `tags` |
| `models/raw/raw_reseller_type2_sales.sql` | `definitions/raw/raw_reseller_type2_sales.sqlx` | table | overrides: `tags` |
| `models/raw/raw_resellers.sql` | `definitions/raw/raw_resellers.sqlx` | table | overrides: `tags` |
| `models/raw/raw_transactions.sql` | `definitions/raw/raw_transactions.sqlx` | table | overrides: `tags` |
| `seeds/geography.csv` | `definitions/seeds/geography.sqlx` | table | 100 rows inlined; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/channels.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/customers.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/main.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/products.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/resellers.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/resellers_type1.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/sources.yml` | `definitions/sources/parquet_input/resellers_type2.sqlx` | operations | external table over the source files; overrides: `tags` |
| `models/staging/staging_channels.sql` | `definitions/staging/staging_channels.sqlx` | table | overrides: `tags` |
| `models/staging/staging_customers.sql` | `definitions/staging/staging_customers.sqlx` | table | overrides: `tags` |
| `models/staging/staging_products.sql` | `definitions/staging/staging_products.sqlx` | table | overrides: `tags` |
| `models/staging/staging_reseller_type1_sales.sql` | `definitions/staging/staging_reseller_type1_sales.sqlx` | incremental | overrides: `bigquery`, `tags` |
| `models/staging/staging_reseller_type2_sales.sql` | `definitions/staging/staging_reseller_type2_sales.sqlx` | incremental | overrides: `bigquery`, `tags` |
| `models/staging/staging_resellers.sql` | `definitions/staging/staging_resellers.sqlx` | table | overrides: `tags` |
| `models/staging/staging_transactions.sql` | `definitions/staging/staging_transactions.sqlx` | table | overrides: `tags` |
| `models/staging/staging_transactions_main.sql` | `definitions/staging/staging_transactions_main.sqlx` | incremental | overrides: `bigquery`, `tags` |
