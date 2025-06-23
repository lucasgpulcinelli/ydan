{{ config(materialized='table') }}

WITH

CHANNELS AS (
  SELECT *
  FROM {{ ref('channels_integrated') }}
)

SELECT
  ROW_NUMBER() OVER(ORDER BY 1) AS ID_channel,
  CHANNEL_ID AS youtube_id_channel,
  CHANNEL_VIDEO_COUNT_NUM AS num_videos_total_channel,
  CHANNEL_SUBSCRIBERS_NUM AS num_subscribers_channel,
  CHANNEL_SCRAPE_TIMESTAMP AS time_of_scraping_unix_channel,
  CHANNEL_WAS_SCRAPED_BOOL AS was_scraped_channel

FROM CHANNELS