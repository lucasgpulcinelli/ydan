-- qual a relevância de um termo ao longo do tempo?

SELECT
  DIM_DATE.year_component_date,
  DIM_DATE.month_component_date,
  COUNT(FACT_TERM.count) AS count

FROM FACT_TERM

JOIN DIM_DATE
  ON FACT_TERM.id_date = DIM_DATE.id_date

JOIN DIM_DICTIONARY
  ON FACT_TERM.id_dictionary = DIM_DICTIONARY.id_dictionary

WHERE
  DIM_DICTIONARY.word_dictionary = 'PYTHON'

GROUP BY
  DIM_DATE.year_component_date,
  DIM_DATE.month_component_date

ORDER BY
  DIM_DATE.year_component_date ASC,
  DIM_DATE.month_component_date ASC