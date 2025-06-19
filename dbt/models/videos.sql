{{ config(materialized='table') }}

WITH VIDEOS AS (
  SELECT
    VIDEO_ID
  FROM BRONZE_LAYER.VIDEOS  
  LIMIT 100
)


SELECT * 
FROM VIDEOS