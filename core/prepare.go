package core

import (
	"github.com/juju/errors"
)

var (
	mustExecSQLs = []string{
		`SET @@GLOBAL.SQL_MODE="NO_ENGINE_SUBSTITUTION"`,
		`SET @@GLOBAL.TIME_ZONE = "+8:00"`,
	}
	mustExecSQLsIgnoreErr = []string{
		`SET @@GLOBAL.TIDB_TXN_MODE="pessimistic"`,
	}
)

func (e *Executor) mustExec() error {
	for _, sql := range mustExecSQLs {
		if err := e.coreExec.Exec(sql); err != nil {
			return errors.Trace(err)
		}
	}
	for _, sql := range mustExecSQLsIgnoreErr {
		e.coreExec.ExecIgnoreErr(sql)
	}
	return e.reConnect()
}
