package connection

import (
	"fmt"
	"github.com/juju/errors"
	// "github.com/ngaut/log"
)

// FetchSchema get schema of given database from database
func (c *Connection) FetchSchema(db string) ([][5]string, error) {
	var (
		schema [][5]string
		tablesInDB [][3]string
	)
	tables, err := c.db.Query(schemaSQL)
	if err != nil {
		return schema, errors.Trace(err)
	}

	// fetch tables need to be described
	for tables.Next() {
		var schemaName, tableName, tableType string
		if err = tables.Scan(&schemaName, &tableName, &tableType); err != nil {
			return [][5]string{}, errors.Trace(err)
		}
		if schemaName == db {
			tablesInDB = append(tablesInDB, [3]string{schemaName, tableName, tableType})
		}
	}

	// desc tables
	for _, table := range tablesInDB {
		var (
			schemaName = table[0]
			tableName = table[1]
			tableType = table[2]
		)
		columns, err := c.db.Query(fmt.Sprintf(tableSQL, schemaName, tableName))
		if err != nil {
			return [][5]string{}, errors.Trace(err)
		}
		for columns.Next() {
			var columnName, columnType string
			var col1, col2, col3, col4 interface{}
			if err = columns.Scan(&columnName, &columnType, &col1, &col2, &col3, &col4); err != nil {
				return [][5]string{}, errors.Trace(err)
			}
			schema = append(schema, [5]string{schemaName, tableName, tableType, columnName, columnType})
		}
	}
	return schema, nil
}
