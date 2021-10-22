package types

import (
	"encoding/binary"
	"errors"
	"godb/table"
	"strconv"
)

var TypeLong = &table.DataType{
	Decode: func(encoded []byte) (table.ColumnValue, error) {
		result, n := binary.Varint(encoded[0:8])
		if n <= 0 {
			return nil, errors.New("Failed to decode value")
		}
		return Long(result), nil
	},
}

type Long int64

func (val Long) String() string {
	return strconv.FormatInt(int64(val), 10)
}

func (val Long) Type() *table.DataType {
	return TypeLong
}

func (val Long) Length() int {
	return 8
}

func (val Long) Encode() []byte {
	res := make([]byte, val.Length())
	binary.PutVarint(res, int64(val))
	return res
}

func (this Long) Compare(other table.ColumnValue) (int, error) {
	switch other := other.(type) {
	case Long:
		if this == other {
			return 0, nil
		} else if other > this {
			return 1, nil
		} else {
			return -1, nil
		}
	default:
		return 0, errors.New("ColumnValues incomparable, different types")
	}
}
