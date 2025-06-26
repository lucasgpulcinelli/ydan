-- qual a competitividade (número de vídeos) de canais sobre diferentes assuntos? 
-- quantos vídeos um canal fala sobre um assunto, e quantos vídeos um assunto aparece em um canal?

SELECT *
FROM (
  SELECT 
    DIM_CHANNEL.youtube_id_channel AS channel_id,
    DIM_KEYWORDS.text_keyword,
    FACT_SUBJECT.count
  FROM FACT_SUBJECT
  JOIN DIM_CHANNEL
    ON FACT_SUBJECT.id_channel = DIM_CHANNEL.id_channel
  JOIN DIM_KEYWORDS
    ON FACT_SUBJECT.id_keyword = DIM_KEYWORDS.id_keyword
) AS base_data
PIVOT (
  SUM(count) 
  FOR text_keyword IN ('FOR KIDS', 'MINECRAFT', 'GAMES')
) AS pivoted_data
WHERE 
  pivoted_data."'FOR KIDS'" > 0 AND pivoted_data."'MINECRAFT'" > 0 AND pivoted_data."'GAMES'" > 0;