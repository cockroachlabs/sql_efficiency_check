package main

import (
	"context"
	"flag"
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
		"database connection string")
	MaxConns = flag.Int(
		"maxConns",
		2,
		"the maximum number of open database connections")
	MaxStmt = flag.Int(
		"maxStmt",
		5,
		"the maximum number of SQL Statements to display for each issue")
	ShowPlans = flag.Bool(
		"showPlans",
		false,
		"Print the Sampled Query Plan")
	MetricsServer = flag.String(
		"http",
		":8181",
		"a bind string for the metrics server")
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
	poolCfg.MaxConns = int32(*MaxConns)

	pool, err := pgxpool.ConnectConfig(ctx, poolCfg)
	if err != nil {
		return errors.Wrap(err, "could not connect")
	}

	// Must be DB version v21+ to continue
	err = getDbVersion(ctx, pool)
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
