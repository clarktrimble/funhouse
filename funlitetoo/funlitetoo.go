// Package funlitetoo provides ch-go helpers reusable across types.
package funlitetoo

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// ColNamer specifies an interface for getting columns and their names.
//
// Columns and names must be of the same length and in the same order.
type ColNamer interface {
	ColNames() (cols []proto.Column, names []string)
}

// Input creates an Input suitable for putting data via ch-go.
func Input(cnr ColNamer) (input proto.Input, err error) {

	cols, names := cnr.ColNames()
	if len(cols) != len(names) {
		err = fmt.Errorf("unequal number of columns and names")
		return
	}

	input = proto.Input{}
	for i, name := range names {
		input = append(input, proto.InputColumn{
			Name: name,
			Data: cols[i],
		})
	}

	return
}

// Results creates a Results suidable for getting data via ch-go.
func Results(cnr ColNamer) (results proto.Results, err error) {

	cols, names := cnr.ColNames()
	if len(cols) != len(names) {
		err = fmt.Errorf("unequal number of columns and names")
		return
	}

	results = proto.Results{}
	for i, name := range names {

		results = append(results, proto.ResultColumn{
			Name: name,
			Data: cols[i],
		})
	}

	return
}

// DropTable drops the table with a given name.
func DropTable(ctx context.Context, client *ch.Client, name string) (err error) {

	err = client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", name),
	})
	return
}

// UpsertTable creates the table with a given name if it does not exist.
func UpsertTable(ctx context.Context, client *ch.Client, name, ddl, engine string) (err error) {

	err = client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s\n%s ENGINE = %s", name, ddl, engine),
	})
	return
}

// Append appends to a slice from a correspondingly typed column.
func Append[T any](slice *[]T, col proto.ColumnOf[T]) {

	for i := 0; i < col.Rows(); i++ {
		*slice = append(*slice, col.Row(i))
	}
}
