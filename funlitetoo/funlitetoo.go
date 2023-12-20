// Package funlitetoo provides ch-go helpers reusable across types.
package funlitetoo

import (
	"context"
	"fmt"
	"io"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

// ColNamer specifies an interface for getting columns and their names.
//
// Columns and names must be of the same length and in the same order.
type ColNamer interface {
	ColNames() (cols []proto.Column, names []string)
	Stash(count int, results proto.Results) (err error)
	Destash(bgn, end int)
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

type Fh struct {
	Client *ch.Client
}

func (fh *Fh) GetResults(ctx context.Context, query string, cnr ColNamer) (err error) {

	results, err := Results(cnr)
	if err != nil {
		return
	}

	err = fh.Client.Do(ctx, ch.Query{
		Body:   query,
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			return cnr.Stash(block.Rows, results)
		},
	})
	return
}

func (fh *Fh) PutInput(ctx context.Context, chunkSize, total int, table string, cnr ColNamer) (err error) {

	// Todo: get table name, total from cnr?

	var idx int

	//err = mcs.CheckLen()
	//if err != nil {
	//return
	//}

	input, err := Input(cnr)
	if err != nil {
		return
	}

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(table),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			//if idx > mcs.Length {
			if idx > total {
				return io.EOF
			}

			//end := min(idx+chunkSize, mcs.Length)
			end := min(idx+chunkSize, total)

			// MsgTable fields (i.e.: mt.Ts) are the same as provided with "Input: input"

			cnr.Destash(idx, end)
			/*
				mt.Ts.AppendArr(mcs.Timestamps[idx:end])
				mt.SeverityTxt.AppendArr(mcs.SeverityTxts[idx:end])
				mt.SeverityNum.AppendArr(mcs.SeverityNums[idx:end])
				mt.Body.AppendArr(mcs.Bodies[idx:end])
				mt.Name.AppendArr(mcs.Names[idx:end])
				mt.Arr.AppendArr(mcs.Tagses[idx:end])
			*/

			idx += chunkSize
			// Todo: check that all's well when chunk is bigger than total
			return nil
		},
	})
	return
}
