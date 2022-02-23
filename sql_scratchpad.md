# Scratchpad for SQL Statements

## Format per-statement statistics  :: index Joins

This query will put in a 0/1 for **iJoin** or **fullScan** as well as 
calculate the "LIO" logical IO (rows) retrieved per statement.  Using 
the total for the entire AGG interval, the lioPCT is displayed for a 
given statement. This version shows the TOP 10 statements where 
an `index join` is run.  The timeframe, statment, LIO, and Percentage of iJoin
is displayed.

```sql
WITH stmt_hr_calc AS (
    SELECT 
        aggregated_ts,
        metadata->>'query' as queryTxt,
        IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
        IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
        CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as rowsMean, CAST(statistics->'statistics'->'cnt' as INT) as sumcnt,
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
    WHERE metadata @> '{"distsql": false}' 
    -- AND 
    --       aggregated_ts > now() - INTERVAL '2hr'
), stmt_hr_stats AS (
    SELECT 
        aggregated_ts,
        substring(queryTxt for 30) as queryTxt,
        fullScan,
        iJoinStmt,
        sum(rowsMean*sumcnt) OVER (PARTITION BY aggregated_ts, queryTxt) as lioPerStmt
    FROM stmt_hr_calc
    ORDER BY lioPerStmt DESC
), stmt_hr_pct AS (
    SELECT 
        aggregated_ts,
        queryTxt,
        fullScan,
        iJoinStmt,
        lioPerStmt/(sum(lioPerStmt) OVER (PARTITION BY aggregated_ts)) as lioPct
    FROM stmt_hr_stats
)
SELECT aggregated_ts, queryTxt, iJoinStmt, fullScan, lioPct
FROM stmt_hr_pct
WHERE iJoinStmt = 1
ORDER BY lioPct DESC
LIMIT 10;
    
aggregated_ts      |            querytxt            | ijoinstmt | fullscan |         liopct
-------------------------+--------------------------------+-----------+----------+-------------------------
  2022-02-07 19:00:00+00 | SELECT c1, c2, c3 FROM scandir |         1 |        0 | 0.94224811180627227600
  2022-02-08 18:00:00+00 | SELECT c1, c2, c3 FROM scandir |         1 |        0 | 0.85878848935714620633
  2022-02-08 17:00:00+00 | SELECT c1, c2, c3 FROM scandir |         1 |        0 | 0.50375937286121324252
  2022-02-01 21:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49955901362542753413
  2022-02-01 21:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49955491203221899122
  2022-02-01 01:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49377680659160290481
  2022-02-01 01:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49375504225612358826
  2022-01-31 23:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49361297910555658495
  2022-01-31 23:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49360118333996962169
  2022-02-01 02:00:00+00 | SELECT big_id, c1 FROM scandir |         1 |        0 | 0.49358896959319343615
(10 rows)


Time: 2.101s total (execution 2.100s / network 0.000s)
```


## SQL for SAMPLE Percentages

```sql
    WITH stmt_hr_calc AS (
        SELECT
            aggregated_ts,
            app_name,
            fingerprint_id,
            metadata->>'query' as queryTxt,
            sampled_plan,
            IF (metadata->'implicitTxn' = 'false', 1, 0) as explicitTxn,
            IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
            CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as rowsMean,
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
    )
        SELECT 
               aggregated_ts,
               lioTotal,
               sum(lioPerStmt * (IF(fullCnt > 0, 1, 0)))     as fullLio,
               sum(lioPerStmt * (IF(iJoinCnt > 0, 1, 0)))    as iJoinLio,
               sum(lioPerStmt * (IF(explicitCnt > 0, 1, 0))) as explicitLio,
               sum(lioPerStmt * (IF(healthyCnt > 0, 1, 0))) as healtyLio
        FROM sql_distinct_cnt
        GROUP BY 1,2;
```

## System statement table without agg

```sql
    WITH stmt_hr_calc AS (
        SELECT
            aggregated_ts,
            app_name,
            fingerprint_id,
            metadata->>'query' as queryTxt,
            plan,
            IF (metadata->'implicitTxn' = 'false', 1, 0) as explicitTxn,
            IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
            CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as rowsMean,
            CAST(statistics->'statistics'->'cnt' as INT) as execCnt,
            CASE
                WHEN (plan @> '{"Name": "index join"}') THEN 1
                WHEN (plan->'Children'->0->>'Name' = 'index join') THEN 1
                WHEN (plan->'Children'->1->>'Name' = 'index join') THEN 1
                WHEN (plan->'Children'->2->>'Name' = 'index join') THEN 1
                WHEN (plan->'Children'->3->>'Name' = 'index join') THEN 1
                WHEN (plan->'Children'->4->>'Name' = 'index join') THEN 1
                ELSE 0
                END as iJoinStmt
        FROM system.statement_statistics
        WHERE 1=1 
--             aggregated_ts = '2022-02-15 18:00:00+00' 
--             AND aggregated_ts > now() - INTERVAL '4hr'
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
           aggregated_ts, 
           fullLio/lioTotal as fullPct,
           iJoinLio/lioTotal as iJoinPCT,
           explicitLio/lioTotal as explicitPCT,
           healtyLio/lioTotal as healtyPCT
    FROM lio_normalization
;
```
