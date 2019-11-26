package core

import (
	"fmt"
	"os"
	"regexp"
	"sync"
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
	sync.Mutex
	dsn1        string
	dsn2        string
	coreOpt     *Option
	execOpt     *executor.Option
	executors   []*executor.Executor
	lastExecID  int
	ss          *smith.SQLSmith
	ch          chan *types.SQL
	logger      *logger.Logger
	dbname      string
	mode        string
	deadlockCh  chan int
	// DSN here is for fetch schema only
	coreConn  *connection.Connection
	coreExec  *executor.Executor
}

// New create Executor
func New(dsn string, coreOpt *Option, execOpt *executor.Option) (*Executor, error) {
	e := Executor{
		dsn1: dsn,
		coreOpt: coreOpt,
		execOpt: execOpt,
		ch: make(chan *types.SQL, 1),
		deadlockCh: make(chan int, 1),
		mode: "single",
	}

	log.Info("coreOpt.Concurrency is", coreOpt.Concurrency)
	for i := 0; i < coreOpt.Concurrency; i++ {
		opt := execOpt.Clone()
		opt.ID = i + 1
		opt.LogSuffix = fmt.Sprintf("-%d", i + 1)
		exec, err := executor.New(e.dsn1, opt)
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
		dsn1: dsn1,
		dsn2: dsn2,
		coreOpt: coreOpt,
		execOpt: execOpt,
		ch: make(chan *types.SQL, 1),
		deadlockCh: make(chan int, 1),
		mode: "abtest",
	}

	log.Info("coreOpt.Concurrency is", coreOpt.Concurrency)
	for i := 0; i < coreOpt.Concurrency; i++ {
		opt := execOpt.Clone()
		opt.ID = i + 1
		opt.LogSuffix = fmt.Sprintf("-%d", i + 1)
		exec, err := executor.NewABTest(e.dsn1, e.dsn2, opt)
		if err != nil {
			return nil, errors.Trace(err)
		}
		e.executors = append(e.executors, exec)
	}
	return e.init()
}

func (e *Executor) init() (*Executor, error) {
	// parse dbname
	dbname := dbnameRegex.FindString(e.dsn1)
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
	// init schema exec
	var exec *executor.Executor
	switch e.mode {
	case "single":
		exec, err = executor.New(e.dsn1, e.execOpt.Clone())
	case "abtest":
		exec, err = executor.NewABTest(e.dsn1, e.dsn2, e.execOpt.Clone())
	default:
		panic("unhandled mode")
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	e.coreExec = exec
	e.coreConn = exec.GetConn()
	return e, nil
}

// Start test
func (e *Executor) Start() error {
	if err := e.mustExec(); err != nil {
		return errors.Trace(err)
	}
	if err := e.reloadSchema(); err != nil {
		return errors.Trace(err)
	}
	for _, executor := range e.executors {
		go executor.Start()
	}
	go e.watchDeadLock()
	go e.smithGenerate()
	go e.startHandler()
	go e.startDataCompare()
	return nil
}

// Stop test
func (e *Executor) Stop(msg string) {
	log.Infof("[STOP] message: %s\n", msg)
	os.Exit(0)
}
