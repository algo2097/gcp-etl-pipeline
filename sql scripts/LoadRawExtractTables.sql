-- Pulling in the data as External Tables
CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.raw.ext_base_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://enrichment-etl-jlr/raw/base_data/*.csv'],
  skip_leading_rows = 1
);

CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.raw.ext_options_data`
OPTIONS (
  format = 'CSV',
  uris = ['gs://enrichment-etl-jlr/raw/options_data/*.csv'],
  skip_leading_rows = 1,
  field_delimiter = ',',
  quote = '"',
  allow_quoted_newlines = TRUE,
  allow_jagged_rows = TRUE
);

CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.raw.ext_vehicle_line_mapping`
OPTIONS (
  format = 'CSV',
  uris = ['gs://enrichment-etl-jlr/raw/vehicle_line_mapping/*.csv'],
  skip_leading_rows = 1
);