-- qual o período do ano que mais tem views para o assunto X?

SELECT
  DIM_DATE.month AS month, 
  AVG(FACT_SUBJECT.num_views_subject) AS avg_views,
  STD(FACT_SUBJECT.num_views_subject) AS std_views

FROM FACT_SUBJECT

JOIN DIM_DATE
  ON FACT_SUBJECT.id_upload_date = DIM_DATE.id_date

JOIN DIM_KEYWORDS
  ON FACT_SUBJECT.id_keyword = dim_keywords.id_keyword

WHERE 
  DIM_KEYWORDS.text_keyword = 'keyword_text'

GROUP BY 
  DIM_DATE.month

ORDER BY 
  DIM_DATE.month ASC