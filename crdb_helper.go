package main

import (
	"context"
	"fmt"
	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

func getDbVersion(ctx context.Context, pool *pgxpool.Pool) error {
	var dbversion string

	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&dbversion); err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("\n%s\n", color.HiWhiteString(dbversion))

	majorVersion, err := strconv.Atoi(regexp.MustCompile(`\.`).Split(strings.Fields(dbversion)[2], -1)[0][1:3])
	minorVersion, err := strconv.Atoi(regexp.MustCompile(`\.`).Split(strings.Fields(dbversion)[2], -1)[1])

	//minorVersion = 1

	if (majorVersion < 21) || (majorVersion == 21 && minorVersion < 2) {
		fmt.Println("Must be CRDB version V21.2.x or greater")
		os.Exit(0)
	}

	return err
}

func showClusterId(ctx context.Context, pool *pgxpool.Pool) error {
	var clusterId string
	clusterIdSQL := `
	SELECT value FROM crdb_internal.node_build_info WHERE field = 'ClusterID';`

	if err := pool.QueryRow(ctx, clusterIdSQL).Scan(&clusterId); err != nil {
		fmt.Println(err)
		return err
	}

	clustStr := fmt.Sprintf("ClusterID: %s \n\n", clusterId)
	fmt.Printf("%s", color.HiWhiteString(clustStr))

	return nil
}

func getStmtLio(ctx context.Context, pool *pgxpool.Pool) []Row {

	// SQL statement to retrieve rows by statement and compute the number of rows
	// referenced "Logical IO" Lio.  There are also flags to show if they are
	// indexJoins, fullScans, or implicit transactions.  This is returned
	// as an array of Rows to main.
	//
	// This data is then used to identify the efficiency of the various statements
	// calling out issues.
	//

	stmtSqlLio := `
WITH stmt_hr_calc AS (
    SELECT 
        aggregated_ts,
		app_name,
		fingerprint_id,
        metadata->>'query' as queryTxt,
		sampled_plan,
        IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
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
    -- WHERE 1=1 AND 
    --  aggregated_ts > now() - INTERVAL '2hr'
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
ORDER BY lioPct DESC`

	stmtSqlLioHr := `
WITH stmt_hr_calc AS (
    SELECT 
        aggregated_ts,
		app_name,
		fingerprint_id,
        metadata->>'query' as queryTxt,
		sampled_plan,
        IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
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
    WHERE 1=1 AND 
    aggregated_ts > now() - INTERVAL '1hr'
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
ORDER BY lioPct DESC`

	//var rowArray Row
	rowArray := Row{}
	var resultSet []Row
	var rows pgx.Rows
	var err error

	if *LastHour {
		rows, err = pool.Query(ctx, stmtSqlLioHr)
	} else {
		rows, err = pool.Query(ctx, stmtSqlLio)
	}
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&rowArray.aggregatedTs, &rowArray.appName, &rowArray.queryTxt, &rowArray.prettyPlan, &rowArray.fullScan, &rowArray.iJoinStmt, &rowArray.implicitTxn, &rowArray.readsPerExec, &rowArray.lioAggTotal, &rowArray.lioPct)
		if err != nil {
			log.Fatal(err)
		}
		resultSet = append(resultSet, rowArray)
	}

	return resultSet
}
