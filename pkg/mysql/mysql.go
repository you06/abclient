package mysql

import (
	"sync"
	"context"
	"database/sql"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// DBConnect wraps db
type DBConnect struct {
	sync.Mutex
	dsn string
	db  *sql.DB
	txn *sql.Tx
}

// DBAccessor can be txn snapshot or db it self
type DBAccessor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

// MustExec must execute sql or fatal
func (conn *DBConnect) MustExec(query string, args ...interface{}) sql.Result {
	r, err := conn.db.Exec(query, args...)
	if err != nil {
		log.Errorf("exec %s err %v", query, err)
	}
	return r
}

// Exec execute sql
func (conn *DBConnect) Exec(query string, args ...interface{}) (sql.Result, error) {
	conn.Lock()
	defer conn.Unlock()
	return conn.GetDBAccessor().Exec(query, args...)
}

// Query execute select statement
func (conn *DBConnect) Query(query string, args ...interface{}) (*sql.Rows, error) {
	conn.Lock()
	defer conn.Unlock()
	return conn.GetDBAccessor().Query(query, args...)
}

// GetDB get real db object
func (conn *DBConnect) GetDB() *sql.DB {
	return conn.db
}

// GetDBAccessor get DBAccessor interface
func (conn *DBConnect) GetDBAccessor() DBAccessor {
	if conn.txn != nil {
		return conn.txn
	}
	return conn.db
}

// IfTxn show if in a transaction
func (conn *DBConnect) IfTxn() bool {
	return conn.txn != nil
}

// Begin a transaction
func (conn *DBConnect) Begin() error {
	conn.Lock()
	defer conn.Unlock()
	if conn.txn != nil {
		return nil
	}
	txn, err := conn.db.Begin()
	if err != nil {
		return err
	}
	conn.txn = txn
	return nil
}

// Commit a transaction
func (conn *DBConnect) Commit() error {
	conn.Lock()
	defer conn.Unlock()
	if conn.txn == nil {
		return nil
	}
	txn := conn.txn
	conn.txn = nil
	return txn.Commit()
}

// Rollback a transaction
func (conn *DBConnect) Rollback() error {
	conn.Lock()
	defer conn.Unlock()
	if conn.txn == nil {
		return nil
	}
	txn := conn.txn
	conn.txn = nil
	return txn.Rollback()
}

// CloseDB turn off db connection
func (conn *DBConnect) CloseDB() error {
	return conn.db.Close()
}

// ReConnect rebuild connection
func (conn *DBConnect) ReConnect() error {
	if err := conn.CloseDB(); err != nil {
		return err
	}
	db, err := sql.Open("mysql", conn.dsn)
	if err != nil {
		return err
	}
	conn.db = db
	return nil
}

// RunWithRetry tries to run func in specified count
func RunWithRetry(ctx context.Context, retryCnt int, interval time.Duration, f func() error) error {
	var (
		err error
	)
	for i := 0; retryCnt < 0 || i < retryCnt; i++ {
		err = f()
		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(interval):
		}
	}
	return err
}

// OpenDB opens db
func OpenDB(dsn string, maxIdleConns int) (*DBConnect, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	// log.Info("DB opens successfully")
	return &DBConnect{
		db: db,
		dsn: dsn,
	}, nil
}

// IsErrDupEntry returns true if error code = 1062
func IsErrDupEntry(err error) bool {
	return isMySQLError(err, 1062)
}

func isMySQLError(err error, code uint16) bool {
	err = originError(err)
	e, ok := err.(*mysql.MySQLError)
	return ok && e.Number == code
}

// originError return original error
func originError(err error) error {
	for {
		e := errors.Cause(err)
		if e == err {
			break
		}
		err = e
	}
	return err
}
