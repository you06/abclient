package connection

import (
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
