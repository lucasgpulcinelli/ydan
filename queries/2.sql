-- qual minuto do vídeo que é ideal você falar para a pessoa se inscrever? 
-- é considerado ideal o momento que tem relevância média alta

SELECT
  DIM_TIMESTAMP.minutes_total_timestamp AS minute,
  SUM(FACT_TERM.relevance) AS sum_relevance

FROM FACT_TERM

JOIN DIM_TIMESTAMP
  ON FACT_TERM.id_timestamp = DIM_TIMESTAMP.id_timestamp

JOIN DIM_DICTIONARY
  ON FACT_TERM.id_dictionary = DIM_DICTIONARY.id_dictionary

JOIN DIM_CHANNEL
  ON FACT_TERM.id_channel = DIM_CHANNEL.id_channel
  
WHERE DIM_DICTIONARY.WORD_DICTIONARY = 'SUBSCRIBE'
  AND minute < 30 AND DIM_CHANNEL.channel_subscribers_num > 1000000

GROUP BY
  DIM_TIMESTAMP.minutes_total_timestamp

ORDER BY
  DIM_TIMESTAMP.minutes_total_timestamp ASC