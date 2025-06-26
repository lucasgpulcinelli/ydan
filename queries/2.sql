-- qual minuto do vídeo que é ideal você falar para a pessoa se inscrever? 
-- é considerado ideal o momento que tem relevância média alta

SELECT
  DIM_TIMESTAMP.minutes_total_timestamp AS minute,
  AVG(FACT_TERM.relevance) AS avg_relevance,
  STD(FACT_TERM.relevance) AS std_relevance

FROM FACT_TERM

JOIN DIM_TIMESTAMP
  ON FACT_TERM.id_timestamp = DIM_TIMESTAMP.id_timestamp

JOIN DIM_CHANNEL
  ON FACT_TERM.id_channel = DIM_CHANNEL.id_channel

GROUP BY
  DIM_TIMESTAMP.minutes_total_timestamp

ORDER BY
  DIM_TIMESTAMP.minutes_total_timestamp ASC