package core

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"math/rand"
	"os"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
	"github.com/you06/doppelganger/pkg/types"
	"github.com/you06/doppelganger/executor"
)

func (e *Executor) reloadSchema() error {
	schema, err := e.coreConn.FetchSchema(e.dbname)
	if err != nil {
		return errors.Trace(err)
	}
	e.ss = smith.New()
	e.ss.LoadSchema(schema)
	e.ss.SetDB(e.dbname)
	e.ss.SetStable(e.coreOpt.Stable)
	return nil
}

func (e *Executor) startDataCompare() {
	switch e.mode {
	case "abtest":
		c := time.Tick(time.Minute)
		for range c {
			go func() {
				result, err := e.abTestCompareData()
				log.Info("test compare data result", result)
				if err != nil {
					log.Fatalf("compare data error %+v", errors.ErrorStack(err))
				}
			}()
		}
	}
}

func (e *Executor) abTestCompareData() (bool, error) {
	// only for abtest
	if e.mode != "abtest" {
		return true, nil
	}

	// start a temp session for keep the snapshot of state
	compareExecutor, err := executor.NewABTest(e.dsn1, e.dsn2, e.execOpt.Clone())
	if err != nil {
		return false, errors.Trace(err)
	}
	defer func(compareExecutor *executor.Executor) {
		if err := compareExecutor.Close(); err != nil {
			log.Fatal("close compare executor error %+v\n", errors.ErrorStack(err))
		}
	}(compareExecutor)

	// commit or rollback all transactions
	e.Lock()
	// no async here to ensure all transactions are committed or rollbacked in order
	// use resolveDeadLock func to avoid deadlock
	e.resolveDeadLock(e.lastExecID)
	// for _, executor := range e.executors {
	// 	if err := executor.ABTestTxnCommit(); err != nil {
	// 		return false, errors.Trace(err)
	// 	}
	// 	<- executor.TxnReadyCh
	// }
	if err := compareExecutor.ABTestTxnBegin(); err != nil {
		e.Unlock()
		return false, errors.Trace(err)
	}
	<- compareExecutor.TxnReadyCh
	// free the lock since the compare has already got the same snapshot in both side
	// go on other transactions
	// defer here for protect environmentz
	defer e.Unlock()

	time.Sleep(time.Duration(rand.Intn(5))*time.Second)
	schema, err := compareExecutor.GetConn().FetchSchema(e.dbname)
	if err != nil {
		return false, errors.Trace(err)
	}
	sqls := makeCompareSQLs(schema)
	for _, sql := range sqls {
		if err := compareExecutor.ABTestSelect(sql); err != nil {
			log.Fatalf("inconsistency when exec %s compare data %+v\n", sql, err)
		}
	}
	log.Info("consistency check pass")
	os.Exit(0)
	return true, nil
}

func makeCompareSQLs (schema [][5]string) []string {
	rowCountSQLs := []string{}
	columnDataSQLs := []string{}
	tables := make(map[string][]string)

	for _, record := range schema {
		if _, ok := tables[record[1]]; !ok {
			tables[record[1]] = []string{}
		}
		if record[3] != "id" {
			tables[record[1]] = append(tables[record[1]], record[3])
		}
	}

	for name, table := range tables {
		rowCountSQLs = append(rowCountSQLs, fmt.Sprintf("SELECT COUNT(1) FROM %s", name))
		columnDataSQL := fmt.Sprintf("SELECT %s FROM %s ORDER BY %s", strings.Join(table, ", "), name, strings.Join(table, ", "))
		columnDataSQLs = append(columnDataSQLs, columnDataSQL)
	}

	sort.Sort(types.BySQL(rowCountSQLs))
	sort.Sort(types.BySQL(columnDataSQLs))
	return append(rowCountSQLs, columnDataSQLs...)
}
