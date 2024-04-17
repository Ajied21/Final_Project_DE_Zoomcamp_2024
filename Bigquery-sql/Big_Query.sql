CREATE OR REPLACE EXTERNAL TABLE marine-aria-419404.final_project.socio_economic_of_indonesia
OPTIONS (
  format = "PARQUET",
  uris = ["gs://final-project-zoomcamp/socio_economic_of_indonesia.parquet"] 
);

SELECT
*
FROM
marine-aria-419404.final_project.socio_economic_of_indonesia;