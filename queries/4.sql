-- quais são os assuntos de maior interesse para o público do canal X?
-- são usadas as recomendações do youtube para isso

SELECT
  KEYWORD_DEST.text_keyword AS text_keyword,
  SUM(FACT_RECOMMENDATION.relevance) AS relevance,
  SUM(FACT_RECOMMENDATION.count) AS recom_count

FROM FACT_RECOMMENDATION

JOIN FACT_SUBJECT AS SUBJECT_ORIGIN
  ON FACT_RECOMMENDATION.id_video_origin = SUBJECT_ORIGIN.id_video

JOIN DIM_KEYWORDS AS KEYWORD_ORIGIN
  ON SUBJECT_ORIGIN.id_keyword = KEYWORD_ORIGIN.id_keyword

JOIN FACT_SUBJECT AS SUBJECT_DEST
  ON FACT_RECOMMENDATION.id_video_destination = SUBJECT_DEST.id_video

JOIN DIM_KEYWORDS AS KEYWORD_DEST
  ON SUBJECT_DEST.id_keyword = KEYWORD_DEST.id_keyword

WHERE 
  KEYWORD_ORIGIN.text_keyword = 'TRUMP'

GROUP BY 
  KEYWORD_DEST.text_keyword

ORDER BY 
  relevance DESC,
  recom_count DESC

LIMIT 10;