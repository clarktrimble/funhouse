// Package funlite provides helpers for use with lightweight interactions with clickhouse.
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

	ci, ok := cr.(*proto.ColUInt8)
	if !ok {
		return
	}

	return *ci
}

// Dt64Values unpacks Time values from a result.
func Dt64Values(cr proto.ColResult) (vals []time.Time) {

	vals = make([]time.Time, cr.Rows())

	cd, ok := cr.(*proto.ColDateTime64)
	if !ok {
		return
	}

	for i := 0; i < cd.Rows(); i++ {
		vals[i] = cd.Row(i)
	}

	return
}

// StrValues unpacks strings from a result.
func StrValues(cr proto.ColResult) (vals []string) {

	vals = []string{}

	cs, ok := cr.(*proto.ColStr)
	if !ok {
		return
	}

	// Todo:fix!!! and  look at all row all the time??
	err := cs.ForEach(func(i int, str string) error {
		vals = append(vals, str)
		return nil
	})
	if err != nil {
		panic(err)
		// Todo: handle
	}

	return
}

// EnumValues unpacks enumerated string values from a result.
func EnumValues(cr proto.ColResult) []string {

	ce, ok := cr.(*proto.ColEnum)
	if !ok {
		return []string{}
	}

	return ce.Values
}
