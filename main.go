package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/pkg/errors"
	"log"
	"os"
	"os/signal"
)

//go:generate go run github.com/cockroachdb/crlfmt -w .

var (
	ConnString = flag.String(
		"conn",
		"postgresql://root@localhost:26257/defaultdb?sslmode=disable",
		"database connect string")
	LastHour = flag.Bool(
		"lastHr",
		false,
		"Sample \"now() - INTERVAL '1hr'\"")
	MaxStmt = flag.Int(
		"maxStmt",
		5,
		"the maximum number of SQL Statements to display for each issue")
	ShowFull = flag.Bool(
		"showFull",
		false,
		"Print the FULL statement")
	ShowPlans = flag.Bool(
		"showPlans",
		false,
		"Print the FULL Query Plan")
	//MetricsServer = flag.String(
	//	"http",
	//	":8181",
	//	"a bind string for the metrics server")
)

func main() {
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := run(ctx); err != nil {
		log.Printf("command failed: %v", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func run(ctx context.Context) error {

	// Create and Connect to Database pools
	poolCfg, err := pgxpool.ParseConfig(*ConnString)
	poolCfg.MaxConns = 2

	pool, err := pgxpool.ConnectConfig(ctx, poolCfg)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	// Must be DB version v21+ to continue
	err = getDbVersion(ctx, pool)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	// Show cluster id
	err = showClusterId(ctx, pool)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	//go metricsServer(ctx, pool)

	// Get statements from crdb_internal.statement_statistics
	var res []Row
	res = getStmtLio(ctx, pool)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	if len(res) < 2 {
		fmt.Printf("Not enough statements... mostly idle cluster")
		os.Exit(0)
	}

	// Top Overall Statements
	topLioHr := topStatements(ctx, res, *MaxStmt)
	// Exit if mostly idle system
	if topLioHr < 3600*10 {
		fmt.Println("Mostly Idle system...Less than 10 LIO/sec in top Hour")
		return err
	}

	// indexJoin
	filterByiJoin(ctx, res, *MaxStmt)

	// Full Scan
	filterByFull(ctx, res, *MaxStmt)

	// Implicit Txn
	filterByImplicit(ctx, res, *MaxStmt)

	// Big SQL Statements
	filterByFatTxn(ctx, res, *MaxStmt)

	return err
}
