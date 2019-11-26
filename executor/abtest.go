package executor

import (
	"fmt"
	"sync"
	"github.com/you06/doppelganger/connection"
	"github.com/you06/doppelganger/util"
	"github.com/you06/doppelganger/pkg/types"
	"github.com/juju/errors"
	// "github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
)

func (e *Executor) abTest() {
	for {
		var (
			err error
			sql = <- e.ch
		)

		switch sql.SQLType {
		case types.SQLTypeReloadSchema:
			err = e.abTestReloadSchema()
		case types.SQLTypeDMLSelect:
			err = e.abTestSelect(sql.SQLStmt)
		case types.SQLTypeDMLUpdate:
			err = e.abTestUpdate(sql.SQLStmt)
		case types.SQLTypeDMLInsert:
			err = e.abTestInsert(sql.SQLStmt)
		case types.SQLTypeDMLDelete:
			err = e.abTestDelete(sql.SQLStmt)
		case types.SQLTypeDDLCreate:
			err = e.abTestCreateTable(sql.SQLStmt)
		case types.SQLTypeTxnBegin:
			err = e.abTestTxnBegin()
		case types.SQLTypeTxnCommit:
			err = e.abTestTxnCommit()
		case types.SQLTypeTxnRollback:
			err = e.abTestTxnRollback()
		case types.SQLTypeExec:
			e.abTestExec(sql.SQLStmt)
		case types.SQLTypeExit:
			e.Stop("receive exit SQL signal")
		default:
			panic(fmt.Sprintf("unhandled case %+v", sql))
		}

		e.abTestLog(sql.SQLStmt, err)
	}
}

func (e *Executor) abTestLog(stmt string, err error) {
	if err != nil {
		e.logger.Infof("[FAIL] Exec SQL %s error %v", stmt, err)
	} else {
		e.logger.Infof("[SUCCESS] Exec SQL %s success", stmt)
	}
}

func (e *Executor) abTestReloadSchema() error {
	schema, err := e.conn1.FetchSchema(e.dbname)
	if err != nil {
		return errors.Trace(err)
	}
	e.ss1 = smith.New()
	e.ss1.LoadSchema(schema)
	e.ss1.SetDB(e.dbname)
	e.ss1.SetStable(e.opt.Stable)
	return nil
}

// ABTestSelect expose abTestSelect
func (e *Executor) ABTestSelect(sql string) error {
	err := e.abTestSelect(sql)
	e.abTestLog(sql, err)
	return err
}

// ABTestTxnBegin export abTestTxnBegin
func (e *Executor) ABTestTxnBegin() error {
	err := e.abTestTxnBegin()
	e.abTestLog("BEGIN", err)
	return err
}

// ABTestTxnCommit export abTestTxnCommit
func (e *Executor) ABTestTxnCommit() error {
	err := e.abTestTxnCommit()
	e.abTestLog("COMMIT", err)
	return err
}

// ABTestTxnRollback export abTestTxnRollback
func (e *Executor) ABTestTxnRollback() error {
	err := e.abTestTxnRollback()
	e.abTestLog("ROLLBACK", err)
	return err
}

// ABTestIfTxn expose abTestIfTxn
func (e *Executor) ABTestIfTxn() bool {
	return e.abTestIfTxn()
}

// DML
func (e *Executor) abTestSelect(sql string) error {
	var (
		wg sync.WaitGroup
		res1 [][]*connection.QueryItem
		res2 [][]*connection.QueryItem
		err1 error
		err2 error
	)
	wg.Add(2)
	go func() {
		res1, err1 = e.conn1.Select(sql)
		wg.Done()
	}()
	go func() {
		res2, err2 = e.conn2.Select(sql)
		wg.Done()
	}()
	wg.Wait()

	// log.Info("select abtest err", err1, err2)
	if err := util.ErrorMustSame(err1, err2); err != nil {
		return err
	}

	if len(res1) != len(res2) {
		return errors.Errorf("row number not match res1: %d, res2: %d", len(res1), len(res2))
	}
	for index := range res1 {
		var (
			row1 = res1[index]
			row2 = res2[index]
		)

		if len(row1) != len(row1) {
			return errors.Errorf("column number not match res1: %d, res2: %d", len(res1), len(res2))	
		}

		for rIndex := range row1 {
			var (
				item1 = row1[rIndex]
				item2 = row2[rIndex]
			)
			if err := item1.MustSame(item2); err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Executor) abTestUpdate(sql string) error {
	var (
		wg sync.WaitGroup
		err1 error
		err2 error
	)
	wg.Add(2)
	go func() {
		err1 = e.conn1.Update(sql)
		wg.Done()
	}()
	go func() {
		err2 = e.conn2.Update(sql)
		wg.Done()
	}()
	wg.Wait()

	if err := util.ErrorMustSame(err1, err2); err != nil {
		return err
	}
	return nil
}

func (e *Executor) abTestInsert(sql string) error {
	var (
		wg sync.WaitGroup
		err1 error
		err2 error
	)
	wg.Add(2)
	go func() {
		err1 = e.conn1.Update(sql)
		wg.Done()
	}()
	go func() {
		err2 = e.conn2.Update(sql)
		wg.Done()
	}()
	wg.Wait()

	if err := util.ErrorMustSame(err1, err2); err != nil {
		return err
	}
	return nil
}

func (e *Executor) abTestDelete(sql string) error {
	var (
		wg sync.WaitGroup
		err1 error
		err2 error
	)
	wg.Add(2)
	go func() {
		err1 = e.conn1.Update(sql)
		wg.Done()
	}()
	go func() {
		err2 = e.conn2.Update(sql)
		wg.Done()
	}()
	wg.Wait()

	if err := util.ErrorMustSame(err1, err2); err != nil {
		return err
	}
	return nil
}

// DDL
func (e *Executor) abTestCreateTable(sql string) error {
	var (
		wg sync.WaitGroup
		err1 error
		err2 error
	)
	wg.Add(2)
	go func() {
		err1 = e.conn1.ExecDDL(sql)
		_ = e.conn1.Commit()
		wg.Done()
	}()
	go func() {
		err2 = e.conn2.ExecDDL(sql)
		_ = e.conn2.Commit()
		wg.Done()
	}()
	wg.Wait()
	e.TxnReadyCh <- struct{}{}
	return util.ErrorMustSame(err1, err2)
}

// just execute
func (e *Executor) abTestExec(sql string) {
	var (
		wg sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		_ = e.conn1.Exec(sql)
		wg.Done()
	}()
	go func() {
		_ = e.conn2.Exec(sql)
		wg.Done()
	}()
	wg.Wait()
}

func (e *Executor) abTestTxnBegin() error {
	var (
		err1 error
		err2 error
	)
	err1 = e.conn1.Begin()
	err2 = e.conn2.Begin()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return util.ErrorMustSame(err1, err2)
}

func (e *Executor) abTestTxnCommit() error {
	var (
		err1 error
		err2 error
	)
	err1 = e.conn1.Commit()
	err2 = e.conn2.Commit()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return util.ErrorMustSame(err1, err2)
}

func (e *Executor) abTestTxnRollback() error {
	var (
		err1 error
		err2 error
	)
	err1 = e.conn1.Rollback()
	err2 = e.conn2.Rollback()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return util.ErrorMustSame(err1, err2)
}

func (e *Executor) abTestIfTxn() bool {
	return e.conn1.IfTxn() || e.conn2.IfTxn()
}
