package table

import (
	"encoding/binary"
	"errors"
	"strconv"
)

func LongFrom(val int64) *ValLong {
	return &ValLong{Value: val}
}

var TypeLong = &DataType{
	Decode: func(encoded []byte) (ColumnValue, error) {
		result, n := binary.Varint(encoded[0:8])
		if n <= 0 {
			return nil, errors.New("Failed to decode value")
		}
		return LongFrom(result), nil
	},
}

// TODO: Replace these with just int64 by creating a typedef
// TODO: type ValLong int64

type ValLong struct {
	Value int64
}

func (l *ValLong) String() string {
	return strconv.FormatInt(l.Value, 10)
}

func (l *ValLong) Type() *DataType {
	return TypeLong
}

func (l *ValLong) Length() int {
	return 8
}

func (l *ValLong) Encode() []byte {
	res := make([]byte, l.Length())
	binary.PutVarint(res, l.Value)
	return res
}

func (this *ValLong) Compare(other ColumnValue) (int, error) {
	switch other := other.(type) {
	case *ValLong:
		if this.Value == other.Value {
			return 0, nil
		} else if other.Value > this.Value {
			return 1, nil
		} else {
			return -1, nil
		}
	default:
		return 0, errors.New("ColumnValues incomparable, different types")
	}
}
