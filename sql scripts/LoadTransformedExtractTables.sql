-- BASE
CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.transformed.ext_base_data`
WITH PARTITION COLUMNS (run_date STRING)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://enrichment-etl-jlr/transformed/clean/base_data_parquet/*'],
  hive_partition_uri_prefix = 'gs://enrichment-etl-jlr/transformed/clean/base_data_parquet',
  require_hive_partition_filter = FALSE
);

-- OPTIONS
CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.transformed.ext_options_data`
WITH PARTITION COLUMNS (run_date STRING)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://enrichment-etl-jlr/transformed/clean/options_data_parquet/*'],
  hive_partition_uri_prefix = 'gs://enrichment-etl-jlr/transformed/clean/options_data_parquet',
  require_hive_partition_filter = FALSE
);

-- VEHICLE MAP
CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.transformed.ext_vehicle_line_mapping`
WITH PARTITION COLUMNS (run_date STRING)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://enrichment-etl-jlr/transformed/clean/vehicle_line_mapping_parquet/*'],
  hive_partition_uri_prefix = 'gs://enrichment-etl-jlr/transformed/clean/vehicle_line_mapping_parquet',
  require_hive_partition_filter = FALSE
);
