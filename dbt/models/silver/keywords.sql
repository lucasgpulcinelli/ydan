{{ config(materialized='table') }}

WITH VIDEOS AS (
  SELECT *
  FROM {{ ref('videos') }}
),

KEYWORDS AS (
  SELECT
    KEYWORD_VIDEO_ID,
    KEYWORD_POSITION_SERIAL_NUM,
    KEYWORD_TEXT
  FROM BRONZE_LAYER.KEYWORDS
  LIMIT 1000
)

SELECT
    k.KEYWORD_VIDEO_ID,
    k.KEYWORD_POSITION_SERIAL_NUM,
    k.KEYWORD_TEXT
FROM KEYWORDS k
LEFT JOIN VIDEOS v
  ON v.VIDEO_ID = k.KEYWORD_VIDEO_ID

WHERE 
    k.KEYWORD_POSITION_SERIAL_NUM > 0 AND 
    k.KEYWORD_POSITION_SERIAL_NUM < 40 AND
    k.KEYWORD_TEXT RLIKE '^[\\x00-\\x7F]+$' --select only ASCII character, I made this line to avoid keywords in another languages like Japanese, Korean or Arabic
