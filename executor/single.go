package executor

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
	"github.com/you06/doppelganger/pkg/types"
)

func (e *Executor) singleTest() {
	for {
		var (
			err error
			sql = <- e.ch
		)

		switch sql.SQLType {
		case types.SQLTypeReloadSchema:
			err = e.singleTestReloadSchema()
		case types.SQLTypeDMLSelect:
			err = e.singleTestSelect(sql.SQLStmt)
		case types.SQLTypeDMLUpdate:
			err = e.singleTestUpdate(sql.SQLStmt)
		case types.SQLTypeDMLInsert:
			err = e.singleTestInsert(sql.SQLStmt)
		case types.SQLTypeDMLDelete:
			err = e.singleTestDelete(sql.SQLStmt)
		case types.SQLTypeDDLCreate:
			err = e.singleTestCreateTable(sql.SQLStmt)
		case types.SQLTypeTxnBegin:
			err = e.singleTestTxnBegin()
		case types.SQLTypeTxnCommit:
			err = e.singleTestTxnCommit()
		case types.SQLTypeTxnRollback:
			err = e.singleTestTxnRollback()
		case types.SQLTypeExec:
			e.singleTestExec(sql.SQLStmt)
		case types.SQLTypeExit:
			e.Stop("receive exit SQL signal")
		default:
			panic(fmt.Sprintf("unhandled case %+v", sql))
		}

		if err != nil {
			log.Errorf("Exec SQL %s error %v\n", sql.SQLStmt, err)
		} else {
			log.Infof("Exec SQL %s successful", sql.SQLStmt)
		}
	}
}

func (e *Executor) singleTestReloadSchema() error {
	schema, err := e.conn1.FetchSchema(e.dbname)
	if err != nil {
		return errors.Trace(err)
	}
	e.ss1 = smith.New()
	e.ss1.LoadSchema(schema)
	e.ss1.SetDB(e.dbname)
	// e.ss1.Debug()
	e.ss1.SetStable(e.opt.Stable)
	return nil
}

// SingleTestSelect expose singleTestSelect
func (e *Executor) SingleTestSelect(sql string) error {
	return e.singleTestSelect(sql)
}

// SingleTestTxnBegin export singleTestTxnBegin
func (e *Executor) SingleTestTxnBegin() error {
	return e.singleTestTxnBegin()
}

// SingleTestTxnCommit export singleTestTxnCommit
func (e *Executor) SingleTestTxnCommit() error {
	return e.singleTestTxnCommit()
}

// SingleTestTxnRollback export singleTestTxnRollback
func (e *Executor) SingleTestTxnRollback() error {
	return e.singleTestTxnRollback()
}

// SingleTestIfTxn expose singleTestIfTxn
func (e *Executor) SingleTestIfTxn() bool {
	return e.singleTestIfTxn()
}

// DML
func (e *Executor) singleTestSelect(sql string) error {
	_, err := e.conn1.Select(sql)
	return errors.Trace(err)
}

func (e *Executor) singleTestUpdate(sql string) error {
	return errors.Trace(e.conn1.Update(sql))
}

func (e *Executor) singleTestInsert(sql string) error {
	return errors.Trace(e.conn1.Insert(sql))
}

func (e *Executor) singleTestDelete(sql string) error {
	return errors.Trace(e.conn1.Delete(sql))
}

// DDL
func (e *Executor) singleTestCreateTable(sql string) error {
	return errors.Trace(e.conn1.ExecDDL(sql))
}


// just execute
func (e *Executor) singleTestExec(sql string) {
	_ = e.conn1.Exec(sql)
}

func (e *Executor) singleTestTxnBegin() error {
	err := e.conn1.Begin()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return errors.Trace(err)
}

func (e *Executor) singleTestTxnCommit() error {
	err := e.conn1.Commit()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return errors.Trace(err)
}

func (e *Executor) singleTestTxnRollback() error {
	err := e.conn1.Rollback()
	// continue generate
	e.TxnReadyCh <- struct{}{}
	return errors.Trace(err)
}

func (e *Executor) singleTestIfTxn() bool {
	return e.conn1.IfTxn()
}
