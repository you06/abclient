package connection

import (
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

// Select run select statement and return query result
func (c *Connection) Select(stmt string, args ...interface{}) ([][]*QueryItem, error) {
	rows, err := c.db.Query(stmt, args...)
	if err != nil {
		return [][]*QueryItem{}, errors.Trace(err)
	}

	columnTypes, _ := rows.ColumnTypes()
	var result [][]*QueryItem

	for rows.Next() {
		var (
			rowResultSets []interface{}
			resultRow     []*QueryItem
		)
		for range columnTypes {
			var accepter interface{}
			rowResultSets = append(rowResultSets, &accepter)
		}
		if err := rows.Scan(rowResultSets...); err != nil {
			log.Info(err)
		}
		for index, resultItem := range(rowResultSets) {
			r := *resultItem.(*interface{})
			item := QueryItem{
				ValType: columnTypes[index],
			}
			if r != nil {
				bytes := r.([]byte)
				item.ValString = string(bytes)
			} else {
				item.Null = true
			}
			resultRow = append(resultRow, &item)
		}
		result = append(result, resultRow)
	}

	return result, nil
}

// Update run update statement and return error
func (c *Connection) Update(stmt string) error {
	_, err := c.db.Exec(stmt)
	return errors.Trace(err)
}

// Insert run insert statement and return error
func (c *Connection) Insert(stmt string) error {
	_, err := c.db.Exec(stmt)
	return errors.Trace(err)
}

// Delete run delete statement and return error
func (c *Connection) Delete(stmt string) error {
	_, err := c.db.Exec(stmt)
	return errors.Trace(err)
}
