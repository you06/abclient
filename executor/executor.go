package executor

import (
	"fmt"
	"path"
	"os"
	"regexp"
	"github.com/you06/doppelganger/connection"
	"github.com/you06/doppelganger/pkg/logger"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
)

var (
	dbnameRegex = regexp.MustCompile(`([a-z0-9A-Z_]+)$`)
)

// SQLType enums for SQL types
type SQLType int

// SQLTypeDMLSelect
const (
	SQLTypeReloadSchema SQLType = iota
	SQLTypeDMLSelect
	SQLTypeDMLUpdate
	SQLTypeDMLInsert
	SQLTypeDMLDelete
	SQLTypeDDLCreate
	SQLTypeExec
	SQLTypeExit
)

// SQL struct
type SQL struct {
	SQLType SQLType
	SQLStmt string
}

// Option struct
type Option struct {
	Clear bool
	Log string
	Reproduce string
	Stable bool
}

// Executor define test executor
type Executor struct {
	dsn1   string
	dsn2   string
	conn1  *connection.Connection
	conn2  *connection.Connection
	ss1    *smith.SQLSmith
	ss2    *smith.SQLSmith
	dbname string
	mode   string
	ch     chan *SQL
	opt    *Option
	logger *logger.Logger
}

// New create Executor
func New(dsn string, opt *Option) (*Executor, error) {
	var connLogPath, executorLogPath string
	if opt.Log != "" {
		connLogPath = path.Join(opt.Log, "single-conn.log")
		executorLogPath = path.Join(opt.Log, "single-test.log")
	}

	conn, err := connection.New(dsn, &connection.Option{
		Log: connLogPath,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	l, err := logger.New(executorLogPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Executor{
		dsn1:  dsn,
		conn1: conn,
		mode:  "single",
		ch: make(chan *SQL, 1),
		dbname: dbnameRegex.FindString(dsn),
		opt: opt,
		logger: l,
	}, nil
}

// NewABTest create abtest Executor
func NewABTest(dsn1, dsn2 string, opt *Option) (*Executor, error) {
	var conn1LogPath, conn2LogPath, executorLogPath string
	if opt.Log != "" {
		conn1LogPath = path.Join(opt.Log, "ab-conn1.log")
		conn2LogPath = path.Join(opt.Log, "ab-conn2.log")
		executorLogPath = path.Join(opt.Log, "ab-test.log")
	}

	conn1, err := connection.New(dsn1, &connection.Option{
		Log: conn1LogPath,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	conn2, err := connection.New(dsn2, &connection.Option{
		Log: conn2LogPath,
	})
	if err != nil {
		return nil, errors.Trace(err)
	}
	l, err := logger.New(executorLogPath)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Executor{
		dsn1:  dsn1,
		dsn2:  dsn2,
		conn1: conn1,
		conn2: conn2,
		mode:  "abtest",
		ch: make(chan *SQL, 1),
		dbname: dbnameRegex.FindString(dsn1),
		opt: opt,
		logger: l,
	}, nil
}

func (e *Executor) init() error {
	switch e.mode {
	case "single":
		return errors.Trace(e.singleTestReloadSchema())
	case "abtest":
		return errors.Trace(e.abTestReloadSchema())
	}
	return errors.New("not support mode")
}

// Start start test
func (e *Executor) Start() {
	if err := e.init(); err != nil {
		log.Fatalf("init failed %v\n", errors.ErrorStack(err))
	}
	if e.opt.Reproduce != "" {
		go e.reproduce()
	} else {
		go e.smithGenerate()
	}
	switch e.mode {
	case "single":
		e.singleTest()
	case "abtest":
		e.abTest()
	}
}

// PrintSchema print schema information and return
func (e *Executor) PrintSchema() error {
	schema, err := e.conn1.FetchSchema(e.dbname)
	if err != nil {
		return errors.Trace(err)
	}
	for _, item := range schema {
		fmt.Printf("{\"%s\", \"%s\", \"%s\", \"%s\", \"%s\"},\n", item[0], item[1], item[2], item[3], item[4])
	}
	return nil
}

// Stop exit process
func (e *Executor) Stop(msg string) {
	log.Infof("[STOP] message: %s\n", msg)
	os.Exit(0)
}
