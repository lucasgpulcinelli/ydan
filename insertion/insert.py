#!/usr/bin/env python3

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from transform_filename import decode_base64


spark = SparkSession.builder.appName("Ydan data movement").getOrCreate()

decode_base64_udf = udf(decode_base64, StringType())
spark.udf.register("decode_base64_udf", decode_base64_udf)

df_channel = spark.read.json("s3a://youtube_data/channel_data/**/*.json")

df_videos = spark.read.json("s3a://youtube_data/video_data/**/*.json")

df_captions = (
    spark.read.format("xml")
    .option("rowTag", "transcript")
    .load("s3a://youtube_data/captions/**/*.xml")
)

df_channel.createOrReplaceTempView("channel")
df_videos.createOrReplaceTempView("video")
df_captions.createOrReplaceTempView("caption")

result_channels = spark.sql("""
    SELECT
      id AS channel_id,
      name AS channel_name,
      description AS channel_description,
      CAST(subscribers AS BIGINT) AS channel_subscribers_num,
      CAST(video_count AS BIGINT) AS chanel_video_count_num,
      to_timestamp(from_unixtime(time_of_scraping_unix)) AS channel_scrape_timestamp
    FROM channel
""")


result_channelvideos = spark.sql("""
    SELECT
      channel.id AS channelvideos_channel_id,
      video.channel_id AS channelvideos_real_channel_id,
      video.id AS channelvideos_video_id,
      pos AS channelvideo_position_serial_num,
      CAST(video.views AS BIGINT) AS channelvideos_video_views_num,
      CAST(video.duration AS REAL) AS channelvideos_video_duration_seconds_float
    FROM channel
    LATERAL VIEW posexplode(channel.videos) AS pos, video
""")

result_videos = spark.sql("""
    SELECT
        id AS video_id,
        title AS video_title,
        description AS video_description,
        CAST(views AS BIGINT) AS video_views_num,
        CAST(likes AS BIGINT) AS video_likes_num,
        CAST(is_live AS BOOLEAN) AS video_is_live_bool,
        CAST(family_friendly AS BOOLEAN) AS video_is_family_friendly_bool,
        to_timestamp(from_unixtime(time_of_scraping_unix)) AS video_scrape_timestamp,
        CAST(duration_s AS REAL) AS video_duration_seconds_float,
        caption_language AS video_caption_language,
        to_date(uploaded_date, 'yyyy/MM/dd') AS video_upload_date,
        channel_subscribers AS video_channel_subscribers,
        channel_id AS video_channel_id
    FROM video
""")


result_recommendations = spark.sql("""
    SELECT
      video.id AS recommendation_origin_video_id,
      rec.id AS recommendation_destination_video_id,
      rec.channel_id AS recommendation_destination_channel_id,
      CAST(rec.views AS BIGINT) AS recommendation_destination_views_num,
      CAST(rec.duration AS REAL) AS recommendation_destination_duration_seconds_float,
      pos AS recommendation_position_serial_num
    FROM video
    LATERAL VIEW posexplode(video.recommendations) AS pos, rec
""")


result_keywords = spark.sql("""
    SELECT
      video.id AS keyword_video_id,
      pos AS keyword_position_serial_num,
      keyword AS keyword_text
    FROM video
    LATERAL VIEW posexplode(video.keywords) AS pos, keyword
""")

result_heatmap = spark.sql("""
    SELECT
      video.id AS heatmap_video_id,
      pos AS heatmap_position_serial_num,
      CAST(heatmapv AS REAL) AS heatmap_value_float
    FROM video
    LATERAL VIEW posexplode(video.heatmap) AS pos, heatmapv
""")


result_captions = spark.sql("""
    SELECT
        decode_base64_udf(input_file_name()) AS caption_video_id,
        line._value AS caption_line_text,
        pos AS caption_line_position_serial_num,
        CAST(line._dur AS REAL) AS caption_line_duration_seconds_float,
        CAST(line._start AS REAL) AS caption_line_start_seconds_float
    FROM caption
    LATERAL VIEW posexplode(caption.text) AS pos, line
""")


queries = {
    "channels": result_channels,
    "channelvideos": result_channelvideos,
    "videos": result_videos,
    "recommendations": result_recommendations,
    "keywords": result_keywords,
    "heatmap": result_heatmap,
    "captions": result_captions,
}


sfOptions = {
    "sfURL": f"{os.environ['SNOWFLAKE_ACCOUNT_IDENTIFIER']}.snowflakecomputing.com",
    "sfUser": os.environ["SNOWFLAKE_USERNAME"],
    "sfPassword": os.environ["SNOWFLAKE_PASSWORD"],
    "sfDatabase": os.environ["SNOWFLAKE_DATABASE"],
    "sfRole": os.environ["SNOWFLAKE_ROLE"],
    "sfSchema": os.environ["SNOWFLAKE_SCHEMA"],
    "sfWarehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
}

for table_name in queries:
    (
        queries[table_name].write.format("net.snowflake.spark.snowflake")
        .options(**sfOptions)
        .option("dbtable", table_name)
        .option("mode", "ignore")
        .save()
    )
