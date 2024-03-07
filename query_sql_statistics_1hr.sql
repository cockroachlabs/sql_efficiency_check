WITH stmt_hr_calc AS (
    SELECT
        aggregated_ts,
        app_name,
        fingerprint_id,
        metadata->>'query' as queryTxt,
       -- (select string_agg(z,E'\n') from crdb_internal.decode_plan_gist(statistics->'statistics'->'planGists'->>0) as z) as sampled_plan,
        'SELECT ZZZ' as sampled_plan,
        IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
        IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
        IF ((select string_agg(z,E'\n') from crdb_internal.decode_plan_gist(statistics->'statistics'->'planGists'->>0) as z) like '%index join%', 1, 0) as ijoinStmt,
        CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as numRows,
        CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT as rowsRead,
        CASE
            WHEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT > CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
                THEN CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT
            ELSE CAST(statistics->'statistics'->'rowsRead'->>'mean' as FLOAT)::INT
            END as rowsMean,
        CAST(statistics->'statistics'->'cnt' as INT) as execCnt
    FROM crdb_internal.statement_statistics
    WHERE 1=1
      AND aggregated_ts > now() - INTERVAL '1hr'
      AND metadata->>'query' not like '%stmt_hr_calc%'
      AND metadata->>'query' not like '%internal-%'
      AND metadata->>'query' not like '%FROM system.%'
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
WHERE 1=1
ORDER BY lioPct DESC;