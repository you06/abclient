package executor

import (
	"github.com/you06/sqlsmith-client/connection"
	"github.com/juju/errors"
)

// Executor define test executor
type Executor struct {
	dsn1  string
	dsn2  string
	conn1 *connection.Connection
	conn2 *connection.Connection
	mode  string
}

// New create Executor
func New(dsn string) (*Executor, error) {
	conn, err := connection.New(dsn)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Executor{
		dsn1:  dsn,
		conn1: conn,
		mode:  "single",
	}, nil
}

// NewABTest create abtest Executor
func NewABTest(dsn1, dsn2 string) (*Executor, error) {
	conn1, err := connection.New(dsn1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn2, err := connection.New(dsn2)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return &Executor{
		dsn1:  dsn1,
		dsn2:  dsn2,
		conn1: conn1,
		conn2: conn2,
		mode:  "abtest",
	}, nil
}

// Start start test
func (e *Executor) Start() {

}
