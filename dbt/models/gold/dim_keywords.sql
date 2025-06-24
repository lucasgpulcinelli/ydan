{{ config(materialized='table') }}

WITH unique_keywords AS (
    SELECT DISTINCT KEYWORD_TEXT
    FROM _SILVER_LAYER.KEYWORDS
    LIMIT 1000
)
SELECT
    ROW_NUMBER() OVER(ORDER BY KEYWORD_TEXT) AS id_keyword,
    KEYWORD_TEXT AS keyword_text
FROM unique_keywords
ORDER BY keyword_text