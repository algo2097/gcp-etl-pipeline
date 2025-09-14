-- External table over curated Parquet 
CREATE OR REPLACE EXTERNAL TABLE `enrichment-etl-jlr.curated.ext_curated_enriched`
WITH PARTITION COLUMNS (run_date STRING)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://enrichment-etl-jlr/curated/data_parquet/*'],
  hive_partition_uri_prefix = 'gs://enrichment-etl-jlr/curated/data_parquet',
  require_hive_partition_filter = FALSE
);

