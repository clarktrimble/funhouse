// Package funlite provides helpers for use with lite clickhouse.
package funlite

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"
)

// Input packs Columns in the form of an Input.
func Input(names []string, byName map[string]proto.Column) (input proto.Input) {

	input = proto.Input{}

	for _, name := range names {
		input = append(input, proto.InputColumn{
			Name: name,
			Data: byName[name],
		})
	}

	return
}

// Results packs Columns in the form of a Results.
func Results(names []string, byName map[string]proto.Column) (results proto.Results) {

	results = proto.Results{}

	for _, name := range names {

		results = append(results, proto.ResultColumn{
			Name: name,
			Data: byName[name],
		})
	}

	return
}

// StrArrayValues unpacks slices of strings from a result.
func StrArrayValues(cr proto.ColResult) (vals [][]string) {

	ca, ok := cr.(*proto.ColArr[string])
	if !ok {
		return
	}

	vals = make([][]string, cr.Rows())
	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}

	return
}

// UInt8Values unpacks uint8's from a result.
func UInt8Values(cr proto.ColResult) (vals []uint8) {

	ca, ok := cr.(*proto.ColUInt8)
	if !ok {
		return
	}

	vals = make([]uint8, ca.Rows())
	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}

	return
}

// Dt64Values unpacks Time values from a result.
func Dt64Values(cr proto.ColResult) (vals []time.Time) {

	vals = make([]time.Time, cr.Rows())

	ca, ok := cr.(*proto.ColDateTime64)
	if !ok {
		return
	}

	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}

	return
}

// StrValues unpacks strings from a result.
func StrValues(cr proto.ColResult) (vals []string) {

	ca, ok := cr.(*proto.ColStr)
	if !ok {
		return
	}

	vals = make([]string, ca.Rows())

	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}

	return
}

// EnumValues unpacks enumerated string values from a result.
func EnumValues(cr proto.ColResult) (vals []string) {

	ca, ok := cr.(*proto.ColEnum)
	if !ok {
		return
	}

	return ca.Values

	return
	// Todo: look at ca.Row(i) -> Enum8
}
