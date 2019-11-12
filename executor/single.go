package executor

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
)

func (e *Executor) singleTest() {
	for {
		var (
			err error
			sql = <- e.ch
		)

		switch sql.SQLType {
		case SQLTypeReloadSchema:
			err = e.singleTestReloadSchema()
		case SQLTypeDMLSelect:
			err = e.singleTestSelect(sql.SQLStmt)
		case SQLTypeDMLUpdate:
			err = e.singleTestUpdate(sql.SQLStmt)
		case SQLTypeDMLInsert:
			err = e.singleTestInsert(sql.SQLStmt)
		case SQLTypeDMLDelete:
			err = e.singleTestDelete(sql.SQLStmt)
		case SQLTypeDDLCreate:
			err = e.singleTestCreateTable(sql.SQLStmt)
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
	return nil
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
