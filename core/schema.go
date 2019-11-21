package core

import (
	"log"
	"github.com/juju/errors"
	smith "github.com/you06/sqlsmith-go"
)

func (e *Executor) reloadSchema() error {
	schema, err := e.schemaConn.FetchSchema(e.dbname)
	log.Println(e.dbname, "reload schema", schema, err)
	if err != nil {
		return errors.Trace(err)
	}
	e.ss = smith.New()
	e.ss.LoadSchema(schema)
	e.ss.SetDB(e.dbname)
	e.ss.SetStable(e.coreOpt.Stable)
	return nil
}
