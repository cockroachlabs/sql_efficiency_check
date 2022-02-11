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

		fmt.Printf("%s %s\n", color.HiWhiteString(resIjoin[i].aggregatedTs), ldsp)
		if pq {
			fmt.Println("\t", color.HiWhiteString(resIjoin[i].queryTxt))
			if *ShowPlans {
				fmt.Println("", color.WhiteString(PrettyString(resIjoin[i].prettyPlan)))
			}
		}
	}

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
		lpct := fmt.Sprintf("%6.2f%s", resFull[i].lioPct*100, "% Rows")
		fmt.Printf("%s %s\n", color.HiWhiteString(resFull[i].aggregatedTs), color.HiRedString(lpct))
		fmt.Println("\t", color.HiWhiteString(resFull[i].queryTxt))
		if *ShowPlans {
			fmt.Println("", color.WhiteString(PrettyString(resFull[i].prettyPlan)))
		}
	}

	return
}
