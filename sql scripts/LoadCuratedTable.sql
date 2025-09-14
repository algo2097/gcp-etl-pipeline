CREATE OR REPLACE TABLE `enrichment-etl-jlr.curated.curated_data`
PARTITION BY run_date
CLUSTER BY vin, option_code AS
SELECT
  vin,
  model,
  model_name,
  brand,
  platform,
  nameplate_display,
  option_code,
  option_desc,
  CAST(net_quantity               AS INT64)     AS net_quantity,
  CAST(net_sales_price            AS NUMERIC)   AS net_sales_price,        
  CAST(production_cost            AS NUMERIC)   AS production_cost,
  CAST(profit_per_transaction     AS NUMERIC)   AS profit_per_transaction,
  PARSE_DATE('%Y%m%d', run_date)  AS run_date
FROM `enrichment-etl-jlr.curated.ext_curated_enriched`;