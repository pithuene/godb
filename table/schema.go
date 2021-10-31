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
	// Decodes the value from a byte slice that starts with the value (can be longer)
	// For variable length values the length can be encoded in the first bytes.
	Decode func([]byte) (ColumnValue, error)
	Id     uint16
}

type Row []ColumnValue

// The byte length required to save this
func (row *Row) Length() int {
	length := 0
	for _, col := range *row {
		length += col.Length()
	}
	return length
}

type ColumnValue interface {
	Type() *DataType
	Length() int
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
