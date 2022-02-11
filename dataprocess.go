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
