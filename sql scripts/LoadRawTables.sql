CREATE OR REPLACE TABLE `enrichment-etl-jlr.raw.base_data` AS
SELECT
  CAST(VIN                AS STRING)    AS vin,
  CAST(Option_Quantities  AS INT64)     AS option_quantities,
  CAST(Options_Code       AS STRING)    AS option_code,
  CAST(Option_Desc        AS STRING)    AS option_desc,
  CAST(Model_Text         AS STRING)    AS model_text,
  CAST(Sales_Price        AS NUMERIC)   AS sales_price,
FROM `enrichment-etl-jlr.raw.ext_base_data`;

CREATE OR REPLACE TABLE `enrichment-etl-jlr.raw.options_data` AS
SELECT
  CAST(Model          AS STRING)  AS model,
  CAST(Option_Code    AS STRING)  AS option_code,
  CAST(Option_Desc    AS STRING)  AS option_desc,
  CAST(Material_Cost  AS FLOAT64) AS material_cost,
FROM `enrichment-etl-jlr.raw.ext_options_data`;

CREATE OR REPLACE TABLE `enrichment-etl-jlr.raw.vehicle_line_mapping` AS
SELECT
  CAST(string_field_0   AS STRING)  AS nameplate_code,
  CAST(string_field_1   AS STRING)  AS brand,
  CAST(string_field_2   AS STRING)  AS platform,
  CAST(string_field_3   AS STRING)  AS nameplate_display,
FROM `enrichment-etl-jlr.raw.ext_vehicle_line_mapping`;