package connection

import (
	"fmt"
	"github.com/you06/sqlsmith-client/pkg/mysql"
	"github.com/juju/errors"
)

// Connection define connection struct
type Connection struct {
	db *mysql.DBConnect
}

// New create Connection instance from dsn
func New(dsn string) (*Connection, error) {
	db, err := mysql.OpenDB(dsn, 1)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &Connection{
		db,
	}, nil
}

// Prepare create test database
func (c *Connection) Prepare(db string) {
	c.db.MustExec(fmt.Sprintf(dropDatabaseSQL, db))
	c.db.MustExec(fmt.Sprintf(createDatabaseSQL, db))
}

// ExecDDL do DDL actions
func (c *Connection) ExecDDL(query string, args ...interface{}) error {
	_, err := c.db.Exec(query, args...)
	return errors.Trace(err)
}
