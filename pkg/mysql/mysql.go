package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// DBConnect wraps db
type DBConnect struct {
	db *sql.DB
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
	return conn.db.Exec(query, args...)
}

// Query execute select statement
func (conn *DBConnect) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return conn.db.Query(query, args...)
}

// GetDB get real db object
func (conn *DBConnect) GetDB() *sql.DB {
	return conn.db
}

// CloseDB turn off db connection
func (conn *DBConnect) CloseDB() error {
	return conn.db.Close()
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
	return errors.Trace(err)
}

// OpenDB opens db
func OpenDB(dsn string, maxIdleConns int) (*DBConnect, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(maxIdleConns)
	log.Info("DB opens successfully")
	return &DBConnect{db}, nil
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
