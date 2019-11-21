package executor

import (
	"github.com/you06/doppelganger/pkg/types"
)

// ExecSQL add sql into exec queue
func (e *Executor) ExecSQL(sql *types.SQL) {
	e.ch <- sql
}
