package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/fatih/color"
)

func PrettyString(str string) (string, error) {
	var prettyJSON bytes.Buffer
	if err := json.Indent(&prettyJSON, []byte(str), "", "    "); err != nil {
		return "", err
	}
	return prettyJSON.String(), nil
}

func topStatements(ctx context.Context, res []Row, desiredLimit int) int {

	// Configure Sort or Row Data Structure
	//
	lioTotalDesc := func(c1, c2 *Row) bool {
		return c1.lioAggTotal > c2.lioAggTotal
	}
	readsPerExecDesc := func(c1, c2 *Row) bool {
		return c1.readsPerExec > c2.readsPerExec
	}
	//readsPerExecDesc := func(c1, c2 *Row) bool {
	//	return c1.readsPerExec > c2.readsPerExec
	//}
	//fullScanDesc := func(c1, c2 *Row) bool {
	//	return c1.fullScan > c2.fullScan
	//}

	var resTopStmt []Row
	for i := 0; i < len(res); i++ {
		resTopStmt = append(resTopStmt, res[i])
	}

	var actualLimit int
	if len(resTopStmt) > desiredLimit {
		actualLimit = desiredLimit
	} else {
		actualLimit = len(resTopStmt)
	}

	fmt.Println(color.HiBlueString("======================================================"))
	fmt.Println(color.HiBlueString("=== Top Statements by Reads per Aggregate Interval ==="))
	fmt.Println(color.HiBlueString("======================================================"))

	OrderedBy(lioTotalDesc, readsPerExecDesc).Sort(resTopStmt)

	var ldsp string
	var lpct string
	var pq bool
	var maxlen int
	var scnt float64
	var lioAggTotalHr int

	//lioAggTotalHr := res[0].lioAggTotal
	//lioAggTotalHrDisp := fmt.Sprintf("%d LIO in top Hour", lioAggTotalHr)
	//fmt.Printf("%s\n\n", color.HiWhiteString(lioAggTotalHrDisp))

	for i := 0; i < actualLimit; i++ {

		if i == 0 {
			lioAggTotalHr = resTopStmt[i].lioAggTotal
			lioAggTotalHrDisp := fmt.Sprintf("%d LIO in top Hour %s", lioAggTotalHr, resTopStmt[i].aggregatedTs)
			fmt.Printf("%s\n\n", color.HiWhiteString(lioAggTotalHrDisp))
		}

		fmt.Printf("%s\n", color.HiWhiteString(resTopStmt[i].aggregatedTs))

		lpct = fmt.Sprintf("%d Rows", resTopStmt[i].lioAggTotal)
		scnt = float64(resTopStmt[i].lioAggTotal) * (resTopStmt[i].lioPct)
		dexec := fmt.Sprintf("%s :: %8.0f ExecPerAggInterval :: %d RowsPerExec", lpct, scnt, resTopStmt[i].readsPerExec)

		ldsp, pq = color.HiRedString(dexec), true

		fmt.Printf("\t%s\n", ldsp)

		if pq {
			maxlen = len(resTopStmt[i].queryTxt)
			if maxlen > 70 && !(*ShowFull) {
				maxlen = 70
			}

			fmt.Println("\t", color.HiWhiteString(resTopStmt[i].queryTxt[:maxlen]))
			if *ShowPlans {
				fmt.Println("", color.WhiteString(PrettyString(resTopStmt[i].prettyPlan)))
			}
		}
	}

	fmt.Printf("\n\n")

	//return resTopStmt[0].lioAggTotal
	return lioAggTotalHr
}

func filterByiJoin(ctx context.Context, res []Row, desiredLimit int) {

	// Configure Sort or Row Data Structure
	//
	lioPctDesc := func(c1, c2 *Row) bool {
		return c1.lioPct > c2.lioPct
	}
	iJoinStmtDesc := func(c1, c2 *Row) bool {
		return c1.iJoinStmt > c2.iJoinStmt
	}
	//fullScanDesc := func(c1, c2 *Row) bool {
	//	return c1.fullScan > c2.fullScan
	//}

	var resIjoin []Row
	for i := 0; i < len(res); i++ {
		if res[i].iJoinStmt == 1 {
			resIjoin = append(resIjoin, res[i])
		}
	}

	var actualLimit int
	if len(resIjoin) > desiredLimit {
		actualLimit = desiredLimit
	} else {
		actualLimit = len(resIjoin)
	}

	fmt.Println(color.HiBlueString("================================================="))
	fmt.Println(color.HiBlueString("=== Top Index Join Times by PCT% of Rows Read ==="))
	fmt.Println(color.HiBlueString("================================================="))
	OrderedBy(lioPctDesc, iJoinStmtDesc).Sort(resIjoin)

	var ldsp string
	var pq bool
	var maxlen int
	for i := 0; i < actualLimit; i++ {
		lpct := fmt.Sprintf("%6.2f%s", resIjoin[i].lioPct*100, "% Rows")

		if resIjoin[i].lioPct >= 0 {
			ldsp, pq = color.HiRedString(lpct), true
		}
		if resIjoin[i].lioPct <= 0.20 {
			ldsp, pq = color.YellowString(lpct), false
		}
		if resIjoin[i].lioPct <= 0.10 {
			ldsp, pq = color.GreenString(lpct), false
		}

		fmt.Printf("%s %s :: %d RowsPerExec\n", color.HiWhiteString(resIjoin[i].aggregatedTs), ldsp, resIjoin[i].readsPerExec)
		if pq {
			maxlen = len(resIjoin[i].queryTxt)
			if maxlen > 70 && !(*ShowFull) {
				maxlen = 70
			}

			fmt.Println("\t", color.HiWhiteString(resIjoin[i].queryTxt[:maxlen]))
			if *ShowPlans {
				fmt.Println("", color.WhiteString(PrettyString(resIjoin[i].prettyPlan)))
			}
		}
	}

	fmt.Printf("\n\n")
	return
}

func filterByFull(ctx context.Context, res []Row, desiredLimit int) {

	// Configure Sort or Row Data Structure
	//
	lioPctDesc := func(c1, c2 *Row) bool {
		return c1.lioPct > c2.lioPct
	}
	fullScanDesc := func(c1, c2 *Row) bool {
		return c1.fullScan > c2.fullScan
	}

	var resFull []Row
	for i := 0; i < len(res); i++ {
		if res[i].fullScan == 1 {
			resFull = append(resFull, res[i])
		}
	}

	var actualLimit int
	var maxlen int

	if len(resFull) > desiredLimit {
		actualLimit = desiredLimit
	} else {
		actualLimit = len(resFull)
	}

	fmt.Println(color.HiBlueString("==========================================="))
	fmt.Println(color.HiBlueString("=== Top FULL SCANs by PCT of Logical IO ==="))
	fmt.Println(color.HiBlueString("==========================================="))

	OrderedBy(lioPctDesc, fullScanDesc).Sort(resFull)

	for i := 0; i < actualLimit; i++ {
		maxlen = len(resFull[i].queryTxt)
		if maxlen > 70 && !(*ShowFull) {
			maxlen = 70
		}
		lpct := fmt.Sprintf("%6.2f%s", resFull[i].lioPct*100, "% Rows")
		fmt.Printf("%s %s :: %d RowsPerExec\n", color.HiWhiteString(resFull[i].aggregatedTs), color.HiRedString(lpct), resFull[i].readsPerExec)
		fmt.Println("\t", color.HiWhiteString(resFull[i].queryTxt)[:maxlen])
		if *ShowPlans {
			fmt.Println("", color.WhiteString(PrettyString(resFull[i].prettyPlan)))
		}
	}

	fmt.Printf("\n\n")
	return
}

func filterByImplicit(ctx context.Context, res []Row, desiredLimit int) {

	// Configure Sort or Row Data Structure
	//
	lioPctDesc := func(c1, c2 *Row) bool {
		return c1.lioPct > c2.lioPct
	}
	implicitDesc := func(c1, c2 *Row) bool {
		return c1.implicitTxn > c2.implicitTxn
	}

	var resImplicit []Row
	for i := 0; i < len(res); i++ {
		if res[i].implicitTxn == 0 {
			resImplicit = append(resImplicit, res[i])
		}
	}

	var actualLimit int
	var maxlen int

	if len(resImplicit) > desiredLimit {
		actualLimit = desiredLimit
	} else {
		actualLimit = len(resImplicit)
	}

	fmt.Println(color.HiBlueString("======================================================"))
	fmt.Println(color.HiBlueString("=== Top EXPLICIT Transactions by PCT of Logical IO ==="))
	fmt.Println(color.HiBlueString("======================================================"))

	OrderedBy(lioPctDesc, implicitDesc).Sort(resImplicit)

	for i := 0; i < actualLimit; i++ {
		maxlen = len(resImplicit[i].queryTxt)
		if maxlen > 70 && !(*ShowFull) {
			maxlen = 70
		}
		lpct := fmt.Sprintf("%6.2f%s", resImplicit[i].lioPct*100, "% Rows")
		fmt.Printf("%s %s :: %d RowsPerExec\n", color.HiWhiteString(resImplicit[i].aggregatedTs), color.HiRedString(lpct), resImplicit[i].readsPerExec)
		fmt.Println("\t", color.HiWhiteString(resImplicit[i].queryTxt)[:maxlen])
		if *ShowPlans {
			fmt.Println("", color.WhiteString(PrettyString(resImplicit[i].prettyPlan)))
		}
	}

	fmt.Printf("\n\n")
	return
}

func filterByFatTxn(ctx context.Context, res []Row, desiredLimit int) {

	// Configure Sort or Row Data Structure
	//
	readsPerExecDesc := func(c1, c2 *Row) bool {
		return c1.readsPerExec > c2.readsPerExec
	}
	//implicitDesc := func(c1, c2 *Row) bool {
	//	return c1.implicitTxn > c2.implicitTxn
	//}

	var resFatTxn []Row
	for i := 0; i < len(res); i++ {
		if res[i].readsPerExec > 1000 {
			resFatTxn = append(resFatTxn, res[i])
		}
	}

	var actualLimit int
	var maxlen int

	if len(resFatTxn) > desiredLimit {
		actualLimit = desiredLimit
	} else {
		actualLimit = len(resFatTxn)
	}

	fmt.Println(color.HiBlueString("======================================================"))
	fmt.Println(color.HiBlueString("=== Top Big SQL statements ==========================="))
	fmt.Println(color.HiBlueString("======================================================"))

	OrderedBy(readsPerExecDesc).Sort(resFatTxn)

	for i := 0; i < actualLimit; i++ {
		maxlen = len(resFatTxn[i].queryTxt)
		if maxlen > 70 && !(*ShowFull) {
			maxlen = 70
		}
		lpct := fmt.Sprintf("%6.2f%s", resFatTxn[i].lioPct*100, "% Rows")
		fmt.Printf("%s %s :: %d RowsPerExec\n", color.HiWhiteString(resFatTxn[i].aggregatedTs), color.HiRedString(lpct), resFatTxn[i].readsPerExec)
		fmt.Println("\t", color.HiWhiteString(resFatTxn[i].queryTxt)[:maxlen])
		if *ShowPlans {
			fmt.Println("", color.WhiteString(PrettyString(resFatTxn[i].prettyPlan)))
		}
	}

	fmt.Printf("\n\n")
	return
}
