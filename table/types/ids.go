package types

import "godb/table"

var TypeIds = map[uint16]*table.DataType{}

func InitializeTypeIds() {
	TypeIds[0] = TypeLong
	TypeIds[1] = TypeString
	TypeIds[2] = TypeColDefs
}
