package types


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
	SQLTypeTxnBegin
	SQLTypeTxnCommit
	SQLTypeTxnRollback
	SQLTypeExec
	SQLTypeExit
)

// SQL struct
type SQL struct {
	SQLType SQLType
	SQLStmt string
}
