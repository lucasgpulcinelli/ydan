{{ config(materialized='table') }}

WITH 

unique_keywords AS (
    SELECT DISTINCT KEYWORD_TEXT
    FROM {{ ref('video_keywords') }}
)

SELECT
    ROW_NUMBER() OVER(ORDER BY KEYWORD_TEXT) AS ID_keyword,
    KEYWORD_TEXT AS text_keyword
FROM unique_keywords
ORDER BY KEYWORD_TEXT