-- Base
CREATE OR REPLACE TABLE `enrichment-etl-jlr.transformed.base_data`
PARTITION BY run_date
CLUSTER BY vin, option_code AS
SELECT
  vin,
  option_code,
  option_desc,
  model,
  model_name,
  CAST(net_quantity AS INT64) AS net_quantity,
  CAST(net_sales_price AS NUMERIC)     AS net_sales_price,
  PARSE_DATE('%Y%m%d', run_date)   AS run_date
FROM `enrichment-etl-jlr.transformed.ext_base_data`;

-- OPTIONS
CREATE OR REPLACE TABLE `enrichment-etl-jlr.transformed.options_data`
PARTITION BY run_date
CLUSTER BY model, option_code AS
SELECT
  model,
  option_code,
  option_desc,
  CAST(material_cost AS NUMERIC)   AS material_cost,
  PARSE_DATE('%Y%m%d', run_date)   AS run_date
FROM `enrichment-etl-jlr.transformed.ext_options_data`;

-- VEHICLE MAP
CREATE OR REPLACE TABLE `enrichment-etl-jlr.transformed.vehicle_line_mapping`
PARTITION BY run_date
CLUSTER BY nameplate_code AS
SELECT
  nameplate_code,
  brand,
  platform,
  nameplate_display,
  PARSE_DATE('%Y%m%d', run_date) AS run_date
FROM `enrichment-etl-jlr.transformed.ext_vehicle_line_mapping`;