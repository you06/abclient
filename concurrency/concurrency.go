package concurrency

import (
	"github.com/you06/doppelganger/executor"
)

// Executor struct
type Executor struct {
	opt         *executor.Option
	concurrency int
	executors   []*executor.Executor
}

// New create Executor
func New(dsn string, opt *executor.Option, concurrency int) (*Executor, error) {
	e := Executor{
		opt: opt,
		concurrency: concurrency,
	}

	return &e, nil
}

// NewABTest create abtest Executor
func NewABTest(dsn1, dsn2 string, opt *executor.Option, concurrency int) (*Executor, error) {
	e := Executor{
		opt: opt,
		concurrency: concurrency,
	}

	return &e, nil
}
