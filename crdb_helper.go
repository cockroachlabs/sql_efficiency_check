package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v4/pgxpool"
)

func getDbVersion(ctx context.Context, pool *pgxpool.Pool) error {
	var dbversion string
	//var err error

	if err := pool.QueryRow(ctx, "SELECT version()").Scan(&dbversion); err != nil {
		fmt.Println(err)
	}
	fmt.Printf("%s \n\n", dbversion)

	//dot := regexp.MustCompile(`\.`)
	majorVersion, err := strconv.Atoi(regexp.MustCompile(`\.`).Split(strings.Fields(dbversion)[2], -1)[0][1:3])

	if err != nil || (majorVersion < 21) {
		fmt.Println("Must be CRDB version V21.x or greater")
		return err
	}
	return err
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
			metadata->>'query' as queryTxt,
			sampled_plan,
			IF (metadata->'implicitTxn' = 'true', 1, 0) as implicitTxn,
			IF (metadata->'fullScan' = 'true', 1, 0) as fullScan,
			CAST(statistics->'statistics'->'numRows'->>'mean' as FLOAT)::INT as rowsMean, 
			CAST(statistics->'statistics'->'cnt' as INT) as sumcnt,
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
		-- AND 
		--       aggregated_ts > now() - INTERVAL '2hr'
	), stmt_hr_stats AS (
		SELECT 
			aggregated_ts,
			-- substring(queryTxt for 30) as queryTxt,
			queryTxt,
			sampled_plan,
			fullScan,
			iJoinStmt,
			sum(rowsMean*sumcnt) OVER (PARTITION BY aggregated_ts, queryTxt) as lioPerStmt
		FROM stmt_hr_calc
		ORDER BY lioPerStmt DESC
	), stmt_hr_pct AS (
		SELECT 
			aggregated_ts,
			queryTxt,
			sampled_plan,
			fullScan,
			iJoinStmt,
			lioPerStmt/(sum(lioPerStmt) OVER (PARTITION BY aggregated_ts)) as lioPct
		FROM stmt_hr_stats
	)
	SELECT 
		experimental_strftime(aggregated_ts,'%Y-%m-%d %H:%M:%S%z') as aggregated_ts,
		queryTxt,
		sampled_plan,
		fullScan, 
		iJoinStmt, 
		lioPct
	FROM stmt_hr_pct
	WHERE iJoinStmt = 1
	ORDER BY lioPct DESC;`

	//var rowArray Row
	rowArray := Row{}
	var resultSet []Row
	rows, err := pool.Query(ctx, stmtSqlLio)
	if err != nil {
		log.Fatal(err)
	}

	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&rowArray.aggregatedTs, &rowArray.queryTxt, &rowArray.prettyPlan, &rowArray.fullScan, &rowArray.iJoinStmt, &rowArray.lioPct)
		if err != nil {
			log.Fatal(err)
		}
		resultSet = append(resultSet, rowArray)
	}

	//for i := 0; i < len(resultSet); i++ {
	//	fmt.Println(resultSet[i])
	//}

	return resultSet
}
