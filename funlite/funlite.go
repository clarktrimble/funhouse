// Package funlite provides helpers for use with lite clickhouse.
// Note!!: only a few column types are implemented, see "...Values" methods below.
package funlite

import (
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

// type Rower[T any] interface {
// Rows() int
// Row(i int) T
// }

// Append appends to a generic slice.
func Append[T any](slice *[]T, rr proto.ColumnOf[T]) {

	for i := 0; i < rr.Rows(); i++ {
		*slice = append(*slice, rr.Row(i))
	}
}
