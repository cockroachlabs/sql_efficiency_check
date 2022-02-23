package main

import (
	"context"
	_ "embed"
	"fmt"
	"github.com/fatih/color"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//
// SQL statement to retrieve rows by statement and compute the number of rows
// referenced "Logical IO" Lio.  There are also flags to show if they are
// indexJoins, fullScans, or implicit transactions.  This is returned
// as an array of Rows to main.
//
// This data is then used to identify the efficiency of the various statements
// calling out issues.
//
//go:embed query_sql_statistics_1hr.sql
var stmtSqlLioHr string

//go:embed query_sql_statistics_all.sql
var stmtSqlLio string

//go:embed query_sql_statistics_sample.sql
var stmtSample string

func noNegVals(a int, b int) float64 {
	if a > b {
		return float64(a - b)
	} else {
		return float64(0)
	}
}

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
	// Run SQL to extract statement statistics and normalize to LIO
	// These values are returned as a data structure of Rows which
	// is then operated on by various statements to show potential inefficiencies

	//var rowArray Row
	rowArray := Row{}
	var resultSet []Row
	var rows pgx.Rows
	var err error

	// Sample Last Hour or all History
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

func lioSampler(ctx context.Context, pool *pgxpool.Pool) error {

	// Run SQL to extract statement statistics and normalize to LIO
	// These values are returned as a data structure of Rows which
	// is then operated on by various statements to show potential inefficiencies

	//var rowArray Row
	rowArray := RowLioSample{}
	rowArrayLast := RowLioSample{
		aggEpochSecs: 0,
		lioTotal:     0,
		fullLio:      0,
		iJoinLio:     0,
		explicitLio:  0,
		healthyLio:   0,
	}

	//var resultSet []RowLioSample
	var rows pgx.Rows
	var err error

	for {
		// Sample Last Hour or all History
		rows, err = pool.Query(ctx, stmtSample)

		if err != nil {
			log.Fatal(err)
		}

		//defer rows.Close()

		for rows.Next() {
			err := rows.Scan(&rowArray.aggEpochSecs, &rowArray.lioTotal, &rowArray.fullLio, &rowArray.iJoinLio, &rowArray.explicitLio, &rowArray.healthyLio)
			if err != nil {
				log.Fatal(err)
			}

			if rowArrayLast.aggEpochSecs != rowArray.aggEpochSecs {
				log.Printf("RESET COUNTERs due to AggInterval change")
				stmtStats.Reset()
			} else {
				aggEpochTs.Set(float64(rowArray.aggEpochSecs))
				//lioTotal.Set(float64(rowArray.lioTotal))
				//fullLio.Set(float64(rowArray.fullLio))
				//iJoinLio.Set(float64(rowArray.iJoinLio))
				//explicitLio.Set(float64(rowArray.explicitLio))
				//healthyLio.Set(float64(rowArray.healthyLio))

				//stmtStats.WithLabelValues("Total").Add(noNegVals(rowArray.lioTotal, rowArrayLast.lioTotal))
				stmtStats.WithLabelValues("full").Add(noNegVals(rowArray.fullLio, rowArrayLast.fullLio))
				stmtStats.WithLabelValues("ijoin").Add(noNegVals(rowArray.iJoinLio, rowArrayLast.iJoinLio))
				stmtStats.WithLabelValues("explicit").Add(noNegVals(rowArray.explicitLio, rowArrayLast.explicitLio))
				stmtStats.WithLabelValues("Optimized").Add(noNegVals(rowArray.healthyLio, rowArrayLast.healthyLio))
			}

			rowArrayLast.aggEpochSecs = rowArray.aggEpochSecs
			rowArrayLast.lioTotal = rowArray.lioTotal
			rowArrayLast.fullLio = rowArray.fullLio
			rowArrayLast.iJoinLio = rowArray.iJoinLio
			rowArrayLast.explicitLio = rowArray.explicitLio
			rowArrayLast.healthyLio = rowArray.healthyLio
		}
		rows.Close()

		//Sample every 10 seconds
		time.Sleep(10 * time.Second)
	}

	return err
}
