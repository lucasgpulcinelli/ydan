{{ config(materialized='table') }}

WITH 

DATE_TABLE AS (
  SELECT *
  FROM _SILVER_LAYER.DATE_TABLE
)


SELECT
  ROW_NUMBER() OVER(ORDER BY 1) AS ID_date,
  date AS string_date,
  day AS day_component_date,
  day_of_the_week AS day_of_the_week_component_date,
  month AS month_component_date,
  month_text AS month_text_component_date,
  quarter AS quarter_component_date,
  semester AS semester_component_date,
  year AS year_component_date

FROM DATE_TABLE