CREATE SCHEMA IF NOT EXISTS `enrichment-etl-jlr.raw`
OPTIONS (
  location = 'europe-west2'
);

CREATE SCHEMA IF NOT EXISTS `enrichment-etl-jlr.transformed`
OPTIONS (
  location = 'europe-west2'
);

CREATE SCHEMA IF NOT EXISTS `enrichment-etl-jlr.curated`
OPTIONS (
  location = 'europe-west2'
);