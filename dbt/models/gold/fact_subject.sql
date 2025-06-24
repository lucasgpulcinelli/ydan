{{ config(materialized='table') }}

-- Keyowords table from silver layer 
WITH silver_keywords AS (
    SELECT
        KEYWORD_VIDEO_ID,
        KEYWORD_TEXT
    FROM {{ ref('keywords') }}
    WHERE KEYWORD_TEXT IS NOT NULL
),

-- Video table from silver layer
silver_videos AS (
    SELECT
        VIDEO_ID,
        VIDEO_CHANNEL_ID,      
        VIDEO_UPLOAD_DATE,
        VIDEO_VIEWS_NUM,
        VIDEO_LIKES_NUM
    FROM {{ ref('videos') }}
    WHERE VIDEO_ID IS NOT NULL AND VIDEO_CHANNEL_ID IS NOT NULL
),

-- keyword dimension from gold layer
gold_dim_keywords AS (
    SELECT
        id_keyword,
        keyword_text
    FROM {{ ref('dim_keywords') }}
),

-- date dimension (I didn't create it yet)
gold_dim_date AS (
    SELECT
        date_id,
        date_date
    FROM {{ ref('dim_date') }}
),

joined_data AS (
    SELECT
        sk.KEYWORD_VIDEO_ID AS video_id, 
        sk.KEYWORD_TEXT AS keyword_text,
        sv.VIDEO_CHANNEL_ID AS channel_id,
        CAST(sv.VIDEO_UPLOAD_DATE AS DATE) AS upload_date, 
        sv.VIDEO_VIEWS_NUM,
        sv.VIDEO_LIKES_NUM
    FROM silver_keywords sk
    INNER JOIN silver_videos sv
        ON sk.KEYWORD_VIDEO_ID = sv.VIDEO_ID 
    WHERE sv.VIDEO_VIEWS_NUM IS NOT NULL AND 
          sv.VIDEO_LIKES_NUM IS NOT NULL AND
          sk.KEYWORD_TEXT IS NOT NULL 
),

fact_staging AS (
    SELECT
        jdk.id_keyword,
        jd.date_id AS id_upload_date,
        jd.upload_date, 
        jd.video_id AS id_video,
        jd.channel_id AS id_channel, 
        jd.VIDEO_VIEWS_NUM,
        jd.VIDEO_LIKES_NUM
    FROM joined_data jd
    INNER JOIN gold_dim_keywords jdk
        ON jd.keyword_text = jdk.keyword_text 
    INNER JOIN gold_dim_date gdd
        ON jd.upload_date = gdd.date_date 
),

final_fact_subject AS (
    SELECT
        id_keyword,
        id_video,
        id_channel,
        id_upload_date,
        SUM(VIDEO_VIEWS_NUM) AS num_views_subject, 
        SUM(VIDEO_LIKES_NUM) AS num_likes_subject,  
        COUNT(*) AS row_count                     
    FROM fact_staging
    GROUP BY
        id_keyword,
        id_video,
        id_channel,
        id_upload_date
    ORDER BY
        id_upload_date, id_video, id_keyword, id_channel 
)

SELECT *
FROM final_fact_subject

