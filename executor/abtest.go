package executor

import (
	"sync"
	"github.com/you06/sqlsmith-client/connection"
	"github.com/you06/sqlsmith-client/util"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
)

func (e *Executor) abTest() {
	for {
		var (
			err error
			sql = <- e.ch
		)

		switch sql.SQLType {
		case SQLTypeReloadSchema:
			err = e.abTestReloadSchema()
		case SQLTypeDMLSelect:
			err = e.abTestSelect(sql.SQLStmt)
		case SQLTypeDMLUpdate:
			err = e.abTestUpdate(sql.SQLStmt)
		case SQLTypeDMLInsert:
			err = e.abTestInsert(sql.SQLStmt)
		case SQLTypeDMLDelete:
			err = e.abTestDelete(sql.SQLStmt)
		case SQLTypeDDLCreate:
			err = e.abTestCreateTable(sql.SQLStmt)
		}

		if err != nil {
			log.Errorf("Exec SQL %s error %v\n", sql.SQLStmt, err)
		}
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
	return nil
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
		wg.Done()
	}()
	go func() {
		err2 = e.conn2.ExecDDL(sql)
		wg.Done()
	}()
	wg.Wait()

	if err := util.ErrorMustSame(err1, err2); err != nil {
		return err
	}
	return nil
}
