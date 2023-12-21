// Package funlitetoo provides a ClickHouse client wrapper for working with tables.
package funlitetoo

import (
	"context"
	"fmt"
	"io"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// Tabler specifies an interface for getting columns and their names.
//
// Columns and names must be of the same length and in the same order.
type Tabler interface {
	TableName() (name string)
	Ddl() (name string)
	Total() (count int)
	CheckLen() (err error)
	ColNames() (cols []proto.Column, names []string)
	AppendFrom(count int, results proto.Results) (err error)
	AppendTo(bgn, end int)
}

// Fh is a funhouse client.
type Fh struct {
	Client *ch.Client
}

// DropTable drops the table with a given name.
func (fh *Fh) DropTable(ctx context.Context, tbr Tabler) (err error) {

	err = fh.Client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", tbr.TableName()),
	})
	return
}

// UpsertTable creates the table with a given name if it does not exist.
func (fh *Fh) UpsertTable(ctx context.Context, engine string, tbr Tabler) (err error) {

	err = fh.Client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s\n%s ENGINE = %s", tbr.TableName(), tbr.Ddl(), engine),
	})
	return
}

// GetResults gets all records from a table.
func (fh *Fh) GetResults(ctx context.Context, tbr Tabler) (err error) {

	results, err := results(tbr)
	if err != nil {
		return
	}

	// query hardcoded here as ch-go will error with "raw block: target: 6 (columns) != 2 (target)"
	// when selecting only two cols in the query and yet try to use "results" for all 6
	//
	// might be nice, certainly more col-oriented,to get one column at a time and merge later?
	// another approach would be to make "results" helper aware of select fields
	//
	// and of course would be straight-forward to pass in a "where" clause if needed

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", tbr.TableName()),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			return tbr.AppendFrom(block.Rows, results)
		},
	})
	return
}

// PutInput puts records from Tablers' data into its table.
func (fh *Fh) PutInput(ctx context.Context, chunkSize int, tbr Tabler) (err error) {

	// Todo: check that all's well when chunk is bigger than total

	err = tbr.CheckLen()
	if err != nil {
		return
	}

	var idx int
	total := tbr.Total()

	input, err := input(tbr)
	if err != nil {
		return
	}

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(tbr.TableName()),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			if idx > total {
				return io.EOF
			}
			end := min(idx+chunkSize, total)

			tbr.AppendTo(idx, end)

			idx += chunkSize
			return nil
		},
	})
	return
}

// Append is a helper that appends to a slice from a correspondingly typed column.
func Append[T any](slice *[]T, col proto.ColumnOf[T]) {

	for i := 0; i < col.Rows(); i++ {
		*slice = append(*slice, col.Row(i))
	}
}

// unexported

func input(tbr Tabler) (input proto.Input, err error) {

	cols, names := tbr.ColNames()
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

func results(tbr Tabler) (results proto.Results, err error) {

	cols, names := tbr.ColNames()
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
