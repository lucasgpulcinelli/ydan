-- qual a competitividade (número de vídeos) de canais sobre diferentes assuntos? 
-- quantos vídeos um canal fala sobre um assunto, e quantos vídeos um assunto aparece em um canal?

SELECT 
  DIM_CHANNEL.youtube_id_channel AS channel_id,
  "keyword1" AS keyword1,
  "keyword2" AS keyword2,
  "keyword3" AS keyword3

FROM FACT_SUBJECT

JOIN DIM_CHANNEL
  ON FACT_SUBJECT.id_channel = DIM_CHANNEL.id_channel

JOIN DIM_KEYWORDS
  ON FACT_SUBJECT.id_keyword = DIM_KEYWORDS.id_keyword

PIVOT(
  SUM(FACT_SUBJECT.count) 
  FOR DIM_KEYWORDS.text_keyword IN ('keyword1', 'keyword2', 'keyword3')
)

ORDER BY 
  "keyword1" DESC,
  "keyword2" DESC,
  "keyword3" DESC