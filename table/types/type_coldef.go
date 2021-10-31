package types

import (
	"encoding/binary"
	"errors"
	"godb/table"
)

// The byte length of the length stored in front of a string value
const COLDEF_LEN_LEN = 8

var TypeColDefs = &table.DataType{
	Decode: func(encoded []byte) (table.ColumnValue, error) {
		offset := 0
		columnCount := binary.BigEndian.Uint64(encoded)
		offset += COLDEF_LEN_LEN
		colDefs := make([]table.ColumnDef, columnCount)
		for idx := 0; idx < int(columnCount); idx++ {
			dataTypeId := binary.BigEndian.Uint16(encoded[offset:])
			offset += 2
			colDefs[idx].Type = TypeIds[dataTypeId]
			nameLength := binary.BigEndian.Uint16(encoded[offset:])
			offset += 2
			name := string(encoded[offset : offset+int(nameLength)])
			colDefs[idx].Name = name
			offset += int(nameLength)
		}

		return ColDefs(colDefs), nil
	},
	Id: 2,
}

type ColDefs []table.ColumnDef

func (val ColDefs) String() string {
	// TODO
	return "ColDef String not implemented"
}

func (val ColDefs) Type() *table.DataType {
	return TypeColDefs
}

func (val ColDefs) Length() int {
	length := COLDEF_LEN_LEN
	for _, col := range val {
		length += 2 // DataTypeId
		length += 2 // Length of the name
		length += len(col.Name)
	}
	return length
}

// Serialize the column defs
//
// The encoding is as follows:
//   8 bytes uint64 Number of column defs that will follow
// For each column def
//   2 bytes uint16 DataTypeId
//   2 bytes uint16 Length of the name
//   n bytes        The actual name
func (val ColDefs) Encode() []byte {
	res := make([]byte, val.Length())
	binary.BigEndian.PutUint64(res, uint64(len(val)))

	offset := COLDEF_LEN_LEN
	for _, col := range val {
		binary.BigEndian.PutUint16(res[offset:], col.Type.Id)
		offset += 2
		binary.BigEndian.PutUint16(res[offset:], uint16(len(col.Name)))
		offset += 2
		copy(res[offset:], col.Name)
		offset += len(col.Name)
	}

	return res
}

func (this ColDefs) Compare(other table.ColumnValue) (int, error) {
	switch other := other.(type) {
	case ColDefs:
		if len(this) != len(other) {
			return -1, nil
		} else {
			for i, col := range this {
				if col != other[i] {
					return -1, nil
				}
			}
			return 0, nil
		}
	default:
		return 0, errors.New("ColumnValues incomparable, different types")
	}
}
