{{ config(materialized='table') }}

WITH 

TIMESTAMP_TABLE AS (
  SELECT *
  FROM _SILVER_LAYER.TIMESTAMP
)


SELECT DISTINCT
  seconds AS ID_timestamp,
  minutes AS minutes_total_timestamp,
  hours AS hours_timestamp,
  minutes_component AS minutes_component_timestamp,
  seconds_component AS seconds_component_timestamp
FROM TIMESTAMP_TABLE
