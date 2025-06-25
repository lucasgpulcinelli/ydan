WITH 
    gen_timestamps AS (
        SELECT 
            TO_TIMESTAMP_NTZ('2021-01-01 00:00:00') AS "CUR_TIMESTAMP",
        UNION ALL
        SELECT "CUR_TIMESTAMP" + INTERVAL '1 second' 
        FROM gen_timestamps 
        WHERE "CUR_TIMESTAMP" + INTERVAL '1 second' <= TO_TIMESTAMP_NTZ('2021-01-01 23:59:59') 
    )
SELECT 
    EXTRACT (SECOND FROM "CUR_TIMESTAMP") AS "Segundo",
    EXTRACT (MINUTE FROM "CUR_TIMESTAMP") AS "Minuto",
    EXTRACT (SECOND FROM "CUR_TIMESTAMP") AS "timestamp_s",
    EXTRACT (MINUTE FROM "CUR_TIMESTAMP") AS "timestamp_m",
    EXTRACT (HOUR FROM "CUR_TIMESTAMP") AS "timestamp_h"
FROM gen_timestamps;