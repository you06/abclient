package core

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"
	"os"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
	"github.com/you06/doppelganger/pkg/types"
	"github.com/you06/doppelganger/executor"
	"github.com/you06/doppelganger/util"
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
		time := 0
		for range c {
			time++
			go func(time int) {
				log.Info("ready to compare data")
				result, err := e.abTestCompareData(true)
				log.Info("test compare data result", result)
				if err != nil {
					log.Fatalf("compare data error %+v", errors.ErrorStack(err))
				}
				if time == 10 {
					os.Exit(0)
				}
			}(time)
		}
	}
}

// abTestCompareDataWithoutCommit take snapshot without other transactions all committed
// this function can run async and channel is for waiting taking snapshot complete
func (e *Executor) abTestCompareDataWithoutCommit(ch chan struct{}) {
	// start a temp session for keep the snapshot of state
	opt := e.execOpt.Clone()
	opt.Mute = true
	compareExecutor, err := executor.NewABTest(e.dsn1, e.dsn2, opt)
	if err != nil {
		log.Fatal(err)
	}
	// schema should be fetch first
	schema, err := compareExecutor.GetConn().FetchSchema(e.dbname)
	if err != nil {
		log.Fatal(err)
	}
	if err := compareExecutor.ABTestTxnBegin(); err != nil {
		log.Fatal(err)
	}
	sqls := makeCompareSQLs(schema)
	if err := compareExecutor.ABTestSelect(sqls[0]); err != nil {
		log.Fatal(err)
	}
	begin := util.CurrentTimeStrAsLog()
	ch <- struct{}{}
	
	time.Sleep(time.Duration(rand.Intn(1000))*time.Millisecond)
	if err != nil {
		log.Fatal("get schema err %+v", errors.ErrorStack(err))
	}
	for _, sql := range sqls {
		if err := compareExecutor.ABTestSelect(sql); err != nil {
			log.Fatalf("inconsistency when exec %s compare data %+v, begin: %s\n", sql, err, begin)
		}
	}
	log.Info("consistency check pass")
}

func (e *Executor) abTestCompareData(delay bool) (bool, error) {
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
	e.resolveDeadLock()
	// for _, executor := range e.executors {
	// 	if err := executor.ABTestTxnCommit(); err != nil {
	// 		return false, errors.Trace(err)
	// 	}
	// 	<- executor.TxnReadyCh
	// }
	// schema should be fetch first
	schema, err := compareExecutor.GetConn().FetchSchema(e.dbname)
	if err != nil {
		e.Unlock()
		return false, errors.Trace(err)
	}
	if err := compareExecutor.ABTestTxnBegin(); err != nil {
		e.Unlock()
		return false, errors.Trace(err)
	}
	sqls := makeCompareSQLs(schema)
	if err := compareExecutor.ABTestSelect(sqls[0]); err != nil {
		log.Fatal(err)
	}
	<- compareExecutor.TxnReadyCh
	begin := util.CurrentTimeStrAsLog()
	// free the lock since the compare has already got the same snapshot in both side
	// go on other transactions
	// defer here for protect environment
	defer e.Unlock()

	// delay will hold on this snapshot and check it later
	if delay {
		time.Sleep(time.Duration(rand.Intn(5))*time.Second)
	}
	for _, sql := range sqls {
		if err := compareExecutor.ABTestSelect(sql); err != nil {
			log.Fatalf("inconsistency when exec %s compare data %+v, begin: %s\n", sql, err, begin)
		}
	}
	log.Info("consistency check pass")
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
