# SQL efficiency checker 

This tool uses the `crdb_internal.statement_statistics` to analyze the various sql statements. Efficiency is analyzed to determine if the statements could be better optimized.  This tool has several options expand control the verbosity of the report.


## Options

```bash
$ sql_efficiency_check -help

Usage of ./sql_efficiency_check:
  -conn string
    	database connect string (default "postgresql://root@localhost:26257/defaultdb?sslmode=disable")
  -http string
    	a bind string for the metrics server (default ":8181")
  -lastHr
    	Sample "now() - INTERVAL '1hr'"
  -maxStmt int
    	the maximum number of SQL Statements to display for each issue (default 5)
  -metricServer
    	Run Metric Server instead of Report
  -showFull
    	Print the FULL statement
  -showPlans
    	Print the FULL Query Plan
```

If you have a fully secure cluster embed the username, password, and certificate paths in the `-conn` string like so:

```bash
sql_efficiench_check -conn 'postgresql://my_name:my_password@my_ipaddress:26257/defaultdb?sslmode=verify-full&sslrootcert=$HOME/Library/CockroachCloud/certs/my_ca.crt'
```

## Default Output "Report Mode"

With default options the data is output as shown below with colors to indicate the severity of various efficiency issues:

![](img/default_report_output_1.png)
...
![](img/default_report_output_2.png)

This highlights the top SQL statements that contribute to distributed SQL fan-out, explicit transactions, and FULL scans.  These can be adjusted by better indexes, DDL, and application restructuring.  To provide more details regarding problem areas you can use the options to expose the FULL SQL text and explain plans for the problem statements.

The following statement uses:
- `-maxStmt` option to limit the number of statements in each category 
- `-showFull` option to display the sql statement
- `-showPlans` to display the `EXPLAIN` plan

```bash
$ sql_efficiency_check -maxStmt 1 -showFull -showPlans
```

**Example output of a few sections are displayed below.....**

![](img/report_output_plans.png)

## Metric Server

To start it in Prometheus metric server mode do the following...

```bash
$ sql_efficiency_check -metricServer

CockroachDB CCL v21.2.2 (x86_64-apple-darwin19, built 2021/12/01 14:38:36, go1.16.6)
ClusterID: e3642879-49d2-481d-a71c-1619597a4f72

Running Prometheus Metric Server
2022/02/23 17:28:58 listening on [::]:8181
2022/02/23 17:29:02 RESET COUNTERs due to AggInterval change
2022/02/23 18:00:17 RESET COUNTERs due to AggInterval change
2022/02/23 19:00:13 RESET COUNTERs due to AggInterval change
```

### Dashboard output

The SQL efficiency Dashboard [json](SQLEfficiencyDashboard-1645677578406.json) can be used to ingest the metrics displayed when running ing `-metricServer` mode.  Below is an example output:

![](img/sql_efficiency_dashboard_prometheus.png)
