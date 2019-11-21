package core

import (
	"math/rand"
	"github.com/you06/doppelganger/executor"
	"github.com/you06/doppelganger/pkg/types"
)

func (e *Executor) startHandler() {
	switch e.mode {
	case "single":
		e.singleTest()
	case "abtest":
		e.abTest()
	default:
		panic("unhandled test mode")
	}
}

func (e *Executor) singleTest() {}

func (e *Executor) abTest() {
	for {
		var (
			err error
			sql = <- e.ch
		)

		switch sql.SQLType {
		case types.SQLTypeReloadSchema:
			err = e.reloadSchema()
		case types.SQLTypeExit:
			e.Stop("receive exit SQL signal")
		default:
			e.randExecutor().ExecSQL(sql)	
		}

		if err != nil {
			e.logger.Infof("[FAIL] Exec SQL %s error %v", sql.SQLStmt, err)
		} else {
			e.logger.Infof("[SUCCESS] Exec SQL %s success", sql.SQLStmt)
		}
	}
}

func (e *Executor) randExecutor() *executor.Executor {
	return e.executors[rand.Intn(len(e.executors))]
}
