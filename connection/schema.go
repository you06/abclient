package connection

import (
	"fmt"
	"github.com/juju/errors"
	// "github.com/ngaut/log"
)

// FetchSchema get schema of given database from database
func (c *Connection) FetchSchema(db string) ([][5]string, error) {
	var schema [][5]string
	tables, err := c.db.Query(schemaSQL)
	if err != nil {
		return schema, errors.Trace(err)
	}

	for tables.Next() {
		var schemaName, tableName, tableType string
		if err = tables.Scan(&schemaName, &tableName, &tableType); err != nil {
			return [][5]string{}, errors.Trace(err)
		}
		if schemaName == db {
			columns, err := c.db.Query(fmt.Sprintf(tableSQL, schemaName, tableName))
			for columns.Next() {
				var columnName, columnType string
				var col1, col2, col3, col4 interface{}
				if err = columns.Scan(&columnName, &columnType, &col1, &col2, &col3, &col4); err != nil {
					return [][5]string{}, errors.Trace(err)
				}
				schema = append(schema, [5]string{schemaName, tableName, tableType, columnName, columnType})
			}
		}
	}
	return schema, nil
}
