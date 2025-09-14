CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.transformed.ext_enriched_base_data`
WITH PARTITION COLUMNS (run_date STRING)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://enrichment-etl-jlr/transformed/enriched_parquet/*'],
  hive_partition_uri_prefix = 'gs://enrichment-etl-jlr/transformed/enriched_parquet',
  require_hive_partition_filter = FALSE
);