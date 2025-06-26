-- qual a relevância de um termo ao longo do tempo?

SELECT
  DIM_DATE.year_component_date,
  DIM_DATE.month_component_date,
  AVG(FACT_TERM.relevance) AS avg_relevance,
  STD(FACT_TERM.relevance) AS std_relevance

FROM FACT_TERM

JOIN DIM_DATE
  ON FACT_TERM.id_date = DIM_DATE.id_date

JOIN DIM_DICTIONARY
  ON FACT_TERM.id_dictionary = DIM_DICTIONARY.id_dictionary

WHERE
  DIM_DICTIONARY.text_dictionary = 'trump'

GROUP BY
  DIM_DATE.year_component_date,
  DIM_DATE.month_component_date

ORDER BY
  DIM_DATE.year_component_date ASC,
  DIM_DATE.month_component_date ASC