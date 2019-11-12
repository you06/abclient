package executor

import (
	"fmt"
	"regexp"
	"github.com/you06/sqlsmith-client/connection"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	smith "github.com/you06/sqlsmith-go"
)

var (
	dbnameRegex = regexp.MustCompile(`([a-z0-9A-Z]+)$`)
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
)

// SQL struct
type SQL struct {
	SQLType SQLType
	SQLStmt string
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
		ch: make(chan *SQL, 1),
		dbname: dbnameRegex.FindString(dsn),
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
		ch: make(chan *SQL, 1),
		dbname: dbnameRegex.FindString(dsn1),
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
	go e.smithGenerate()
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
