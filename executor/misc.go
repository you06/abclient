package executor

import (
	"fmt"
	"github.com/juju/errors"
)

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

