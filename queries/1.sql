-- qual o período do ano que mais tem views para o assunto X?

SELECT
  DIM_DATE.month_component_date AS month, 
  AVG(FACT_SUBJECT.num_views_subject) AS avg_views,
  STDDEV(FACT_SUBJECT.num_views_subject) AS std_views

FROM FACT_SUBJECT

JOIN DIM_DATE
  ON FACT_SUBJECT.id_upload_date = DIM_DATE.id_date

JOIN DIM_KEYWORDS
  ON FACT_SUBJECT.id_keyword = dim_keywords.id_keyword

WHERE 
  DIM_KEYWORDS.text_keyword = 'NFL'

GROUP BY 
  DIM_DATE.month_component_date

ORDER BY 
  DIM_DATE.month_component_date ASC
