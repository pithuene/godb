package table

import (
	"encoding/binary"
	"errors"
	"strings"
)

func StringFrom(val string) *ValString {
	return &ValString{Value: val}
}

// The byte length of the length stored in front of a string value
const STRING_LEN_LEN = 8

var TypeString = &DataType{
	Decode: func(encoded []byte) (ColumnValue, error) {
		// The byte length of the value field
		valueLength := binary.BigEndian.Uint64(encoded)
		value := string(encoded[STRING_LEN_LEN : STRING_LEN_LEN+valueLength])
		return StringFrom(value), nil
	},
}

type ValString struct {
	Value string
}

func (l *ValString) String() string {
	return l.Value
}

func (l *ValString) Type() *DataType {
	return TypeString
}

func (l *ValString) Length() int {
	return STRING_LEN_LEN + len(l.Value)
}

func (l *ValString) Encode() []byte {
	res := make([]byte, STRING_LEN_LEN+len(l.Value))
	binary.BigEndian.PutUint64(res, uint64(len(l.Value)))
	copy(res[STRING_LEN_LEN:STRING_LEN_LEN+len(l.Value)], []byte(l.Value))
	return res
}

func (this *ValString) Compare(other ColumnValue) (int, error) {
	switch other := other.(type) {
	case *ValString:
		return strings.Compare(other.Value, this.Value), nil
	default:
		return 0, errors.New("ColumnValues incomparable, different types")
	}
}