-- quais são os assuntos de maior interesse para o público do canal X?
-- são usadas as recomendações do youtube para isso

SELECT
  FACT_SUBJECT.keyword_text AS keyword_text,
  SUM(FACT_RECOMMENDATION.relevance) AS relevance,
  SUM(FACT_RECOMMENDATION.count) AS count

FROM FACT_RECOMMENDATION

JOIN DIM_CHANNEL
  ON FACT_RECOMMENDATION.id_channel_origin = DIM_CHANNEL.id_channel  

JOIN FACT_SUBJECT
  ON FACT_RECOMMENDATION.id_video_destination = FACT_SUBJECT.id_video

JOIN DIM_KEYWORDS
  ON FACT_SUBJECT.id_keyword = DIM_KEYWORDS.id_keyword

WHERE 
  DIM_CHANNEL.youtube_id_channel = 'channel_id'

GROUP BY 
  FACT_SUBJECT.keyword_text

ORDER BY 
  SUM(FACT_RECOMMENDATION.relevance) DESC,
  SUM(FACT_RECOMMENDATION.count) DESC

LIMIT 10