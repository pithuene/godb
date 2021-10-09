package table

import "errors"

type ColumnDef struct {
	Name string
	Type *DataType
}

type TableSchema struct {
	Columns []ColumnDef
}

func (ts *TableSchema) FindColumnByName(name string) (*ColumnDef, int, error) {
	for idx, col := range ts.Columns {
		if col.Name == name {
			return &col, idx, nil
		}
	}
	return nil, -1, errors.New("Column '" + name + "' not found")
}

type DataType struct {
	// The byte length of the type
	// TODO: What about variable length types
	Length int64
	Decode func([]byte) (ColumnValue, error)
}

type Row = []ColumnValue

type ColumnValue interface {
	Type() *DataType
	Encode() []byte
	Compare(other ColumnValue) (int, error)
	String() string
}

// Check whether the row conforms to the schema
func (ts *TableSchema) CheckSchema(row Row) bool {
	for idx, val := range row {
		if val.Type() != ts.Columns[idx].Type {
			return false
		}
	}
	return true
}
