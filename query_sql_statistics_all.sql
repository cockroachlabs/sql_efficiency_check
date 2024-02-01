WITH stmt_hr_calc AS (
    SELECT
        aggregated_ts,
        app_name,
        fingerprint_id,
        metadata->>'query' as queryTxt,
        (crdb_internal.decode_plan_gist(statistics->'statistics'->'planGists'->>0)) as sampled_plan,
        IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
        IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
        IF (crdb_internal.decode_plan_gist(statistics->'statistics'->'planGists'->>0) like '%index join%', 1, 0) as ijoinStmt,
        CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as numRows,
        CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT as rowsRead,
        CAST(statistics->'execution_statistics'->'contentionTime'->>'mean' as FLOAT) as contentionTime,
        CASE
            WHEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT > CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
                THEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT
            ELSE CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
            END as rowsMean,
        CAST(statistics->'statistics'->'cnt' as INT) as execCnt
    FROM crdb_internal.statement_statistics
    WHERE 1=1
        AND metadata->>'query' not like '%stmt_hr_calc%'
      --AND app_name not like '$ internal-%'
), stmt_hr_stats AS (
    SELECT
        aggregated_ts,
        app_name,
        fingerprint_id,
        queryTxt,
        sampled_plan,
        fullScan,
        iJoinStmt,
        implicitTxn,
        execCnt,
        sum(rowsMean*execCnt) OVER (PARTITION BY aggregated_ts) as lioAggTotal,
        sum(rowsMean*execCnt) OVER (PARTITION BY aggregated_ts, fingerprint_id) as lioPerStmt
    FROM stmt_hr_calc
    ORDER BY lioPerStmt DESC
), stmt_hr_pct AS (
    SELECT
        aggregated_ts,
        app_name,
        queryTxt,
        sampled_plan,
        fullScan,
        iJoinStmt,
        implicitTxn,
        lioPerStmt,
        lioAggTotal,
        execCnt,
        lioPerStmt/lioAggTotal as lioPct
    FROM stmt_hr_stats
)
SELECT
    experimental_strftime(aggregated_ts,'%Y-%m-%d %H:%M:%S%z') as aggregated_ts,
    app_name,
    queryTxt,
    sampled_plan,
    fullScan,
    iJoinStmt,
    implicitTxn,
    (lioPerStmt/execCnt)::int as readsPerExec,
    lioAggTotal,
    lioPct
FROM stmt_hr_pct
WHERE 1=1 and
        app_name not like '%internal-%'
ORDER BY lioPct DESC;