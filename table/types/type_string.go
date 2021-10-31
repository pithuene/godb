package types

import (
	"encoding/binary"
	"errors"
	"godb/table"
	"strings"
)

// The byte length of the length stored in front of a string value
const STRING_LEN_LEN = 8

var TypeString = &table.DataType{
	Decode: func(encoded []byte) (table.ColumnValue, error) {
		// The byte length of the value field
		valueLength := binary.BigEndian.Uint64(encoded)
		value := string(encoded[STRING_LEN_LEN : STRING_LEN_LEN+valueLength])
		return String(value), nil
	},
	Id: 1,
}

type String string

func (val String) String() string {
	return string(val)
}

func (val String) Type() *table.DataType {
	return TypeString
}

func (val String) Length() int {
	return STRING_LEN_LEN + len(val)
}

func (val String) Encode() []byte {
	res := make([]byte, STRING_LEN_LEN+len(val))
	binary.BigEndian.PutUint64(res, uint64(len(val)))
	copy(res[STRING_LEN_LEN:STRING_LEN_LEN+len(val)], []byte(val))
	return res
}

func (this String) Compare(other table.ColumnValue) (int, error) {
	switch other := other.(type) {
	case String:
		return strings.Compare(string(other), string(this)), nil
	default:
		return 0, errors.New("ColumnValues incomparable, different types")
	}
}
