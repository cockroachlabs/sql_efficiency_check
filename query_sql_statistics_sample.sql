WITH stmt_hr_calc AS (
    SELECT
        aggregated_ts,
        app_name,
        fingerprint_id,
        metadata->>'query' as queryTxt,
        sampled_plan,
        IF (metadata->'implicitTxn' = 'false', 1, 0) as explicitTxn,
        IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
        CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as numRows,
        CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT as rowsRead,
        CASE
            WHEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT > CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
                THEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT
            ELSE CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
            END as rowsMean,
        CAST(statistics->'statistics'->'cnt' as INT) as execCnt,
        CASE
            WHEN (sampled_plan @> '{"Name": "index join"}') THEN 1
            WHEN (sampled_plan->'Children'->0->>'Name' = 'index join') THEN 1
            WHEN (sampled_plan->'Children'->1->>'Name' = 'index join') THEN 1
            WHEN (sampled_plan->'Children'->2->>'Name' = 'index join') THEN 1
            WHEN (sampled_plan->'Children'->3->>'Name' = 'index join') THEN 1
            WHEN (sampled_plan->'Children'->4->>'Name' = 'index join') THEN 1
            ELSE 0
            END as iJoinStmt
    FROM crdb_internal.statement_statistics
    WHERE 1=1
--             aggregated_ts = '2022-02-15 18:00:00+00'
      AND aggregated_ts > now() - INTERVAL '1hr'
      AND app_name not like '$ internal-%'
), sql_distinct_cnt as (
    SELECT DISTINCT aggregated_ts,
                    -- app_name,
                    -- fingerprint_id,
                    substring(queryTxt for 30)                                                as queryTxt,
                    -- sampled_plan,
                    sum(fullScan) OVER (PARTITION BY aggregated_ts, fingerprint_id)           as fullCnt,
                    sum(iJoinStmt) OVER (PARTITION BY aggregated_ts, fingerprint_id)          as iJoinCnt,
                    sum(explicitTxn) OVER (PARTITION BY aggregated_ts, fingerprint_id)        as explicitCnt,
                    sum(IF((fullScan = 0) and (iJoinStmt = 0) and (explicitTxn = 0), 1, 0))
                        OVER (PARTITION BY  aggregated_ts, fingerprint_id) as healthyCnt,
                    sum(execCnt) OVER (PARTITION BY aggregated_ts)                            as execTotal,
                    sum(rowsMean * execCnt) OVER (PARTITION BY aggregated_ts)                 as lioTotal,
                    sum(rowsMean * execCnt) OVER (PARTITION BY aggregated_ts, fingerprint_id) as lioPerStmt
    FROM stmt_hr_calc
    ORDER BY lioPerStmt
), lio_normalization as (
    SELECT aggregated_ts,
           lioTotal,
           sum(lioPerStmt * (IF(fullCnt > 0, 1, 0)))     as fullLio,
           sum(lioPerStmt * (IF(iJoinCnt > 0, 1, 0)))    as iJoinLio,
           sum(lioPerStmt * (IF(explicitCnt > 0, 1, 0))) as explicitLio,
           sum(lioPerStmt * (IF(healthyCnt > 0, 1, 0)))  as healtyLio
    FROM sql_distinct_cnt
    GROUP BY 1, 2
)
SELECT
--     experimental_strftime(aggregated_ts,'%Y-%m-%d %H:%M:%S%z') as aggregated_ts,
    extract(epoch from aggregated_ts) as aggEpochSecs,
    LioTotal,
    fullLio,
    iJoinLio,
    explicitLio,
    healtyLio
FROM lio_normalization
;