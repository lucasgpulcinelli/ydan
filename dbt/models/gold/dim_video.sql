{{ config(materialized='table') }}

WITH VIDEOS AS (
  SELECT *
  FROM {{ ref('videos') }}
)


SELECT
  ROW_NUMBER() OVER(ORDER BY 1) AS ID_video,
  VIDEO_ID AS youtube_id_video,
  VIDEO_IS_FAMILY_FRIENDLY_BOOL AS is_family_friendly_video,
  VIDEO_IS_LIVE_BOOL AS is_live_video,
  VIDEO_VIEWS_NUM AS num_views_video,
  VIDEO_LIKES_NUM AS num_likes_video,
  VIDEO_DURATION_SECONDS_FLOAT AS duration_seconds_video,
  VIDEO_DURATION_SECONDS_FLOAT / 60 AS duration_minutes_video,
  VIDEO_DURATION_SECONDS_FLOAT / 3600 AS duration_hours_video,
  MOD(VIDEO_DURATION_SECONDS_FLOAT, 60) AS duration_seconds_component_video,
  CAST(MOD(VIDEO_DURATION_SECONDS_FLOAT, 3600) / 60 AS NUMBER) AS duration_minutes_component_video,
  VIDEO_SCRAPE_TIMESTAMP AS time_of_scraping_unix_video

FROM VIDEOS