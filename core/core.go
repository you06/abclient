package core

import (
	"fmt"
	"os"
	"regexp"
	"github.com/ngaut/log"
	"github.com/you06/doppelganger/executor"
	"github.com/you06/doppelganger/pkg/logger"
	"github.com/you06/doppelganger/pkg/types"
	"github.com/juju/errors"
	smith "github.com/you06/sqlsmith-go"
	"github.com/you06/doppelganger/connection"
)

var (
	dbnameRegex = regexp.MustCompile(`([a-z0-9A-Z_]+)$`)
	schemaConnOption = connection.Option{
		Mute: true,
	}
)

// Executor struct
type Executor struct {
	coreOpt     *Option
	execOpt     *executor.Option
	executors   []*executor.Executor
	ss          *smith.SQLSmith
	ch          chan *types.SQL
	logger      *logger.Logger
	dbname      string
	mode        string
	// DSN here is for fetch schema only
	schemaDSN   string
	schemaConn  *connection.Connection
}

// New create Executor
func New(dsn string, coreOpt *Option, execOpt *executor.Option) (*Executor, error) {
	e := Executor{
		coreOpt: coreOpt,
		execOpt: execOpt,
		ch: make(chan *types.SQL, 1),
		schemaDSN: dsn,
		mode: "single",
	}

	log.Info("coreOpt.Concurrency is", coreOpt.Concurrency)
	for i := 0; i < coreOpt.Concurrency; i++ {
		opt := execOpt.Clone()
		opt.LogSuffix = fmt.Sprintf("-%d", i + 1)
		exec, err := executor.New(dsn, opt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.executors = append(e.executors, exec)
	}
	return e.init()
}

// NewABTest create abtest Executor
func NewABTest(dsn1, dsn2 string, coreOpt *Option, execOpt *executor.Option) (*Executor, error) {
	e := Executor{
		coreOpt: coreOpt,
		execOpt: execOpt,
		ch: make(chan *types.SQL, 1),
		schemaDSN: dsn1,
		mode: "abtest",
	}

	log.Info("coreOpt.Concurrency is", coreOpt.Concurrency)
	for i := 0; i < coreOpt.Concurrency; i++ {
		opt := execOpt.Clone()
		opt.LogSuffix = fmt.Sprintf("-%d", i + 1)
		exec, err := executor.NewABTest(dsn1, dsn2, opt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.executors = append(e.executors, exec)
	}
	return e.init()
}

func (e *Executor) init() (*Executor, error) {
	// parse dbname
	dbname := dbnameRegex.FindString(e.schemaDSN)
	if dbname == "" {
		return nil, errors.NotFoundf("empty dbname in dsn")
	}
	e.dbname = dbname
	// init logger
	l, err := logger.New("", false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.logger = l
	// init schema conn
	schemaConn, err := connection.New(e.schemaDSN, &schemaConnOption)
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.schemaConn = schemaConn
	return e, nil
}

// Start test
func (e *Executor) Start() error {
	if err := e.reloadSchema(); err != nil {
		return errors.Trace(err)
	}
	for _, executor := range e.executors {
		go executor.Start()
	}
	go e.smithGenerate()
	go e.startHandler()
	return nil
}

// Stop test
func (e *Executor) Stop(msg string) {
	log.Infof("[STOP] message: %s\n", msg)
	os.Exit(0)
}
