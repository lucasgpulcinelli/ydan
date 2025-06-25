{{ config(materialized='table') }}

WITH

KEYWORDS AS (
    SELECT *
    FROM {{ ref('dim_keywords') }}
),

DATE_TABLE AS (
    SELECT *
    FROM {{ ref('dim_date') }}
),

VIDEOS AS (
    SELECT *
    FROM {{ ref('dim_video') }}
),

CHANNELS AS (
    SELECT *
    FROM {{ ref('dim_channel') }}
),

VIDEO_KEYWORDS AS (
    SELECT *
    FROM {{ ref('video_keywords') }}
)


SELECT 
    KEYWORDS.id_keyword,
    VIDEOS.id_video,
    CHANNELS.id_channel AS id_channel,
    DATE_TABLE.id_date AS id_upload_date,
    VIDEO_KEYWORDS.KEYWORD_VIDEO_VIEWS_NUM AS num_views_subject, 
    VIDEO_KEYWORDS.KEYWORD_VIDEO_LIKES_NUM AS num_likes_subject,  
    1 AS count

FROM VIDEO_KEYWORDS

JOIN KEYWORDS
    ON VIDEO_KEYWORDS.KEYWORD_TEXT = KEYWORDS.text_keyword

JOIN VIDEOS
    ON VIDEO_KEYWORDS.KEYWORD_VIDEO_ID = VIDEOS.youtube_id_video

JOIN CHANNELS
    ON VIDEO_KEYWORDS.KEYWORD_CHANNEL_ID = CHANNELS.youtube_id_channel

JOIN DATE_TABLE
    ON EXTRACT(DAY FROM VIDEO_KEYWORDS.KEYWORD_VIDEO_UPLOAD_DATE) = DATE_TABLE.day_component_date AND
        EXTRACT(YEAR FROM VIDEO_KEYWORDS.KEYWORD_VIDEO_UPLOAD_DATE) = DATE_TABLE.year_component_date AND
        EXTRACT(MONTH FROM VIDEO_KEYWORDS.KEYWORD_VIDEO_UPLOAD_DATE) = DATE_TABLE.month_component_date