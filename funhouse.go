package funhouse

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
	"funhouse/table"
)

// FunHouse is a column-oriented low-ish level clickhouse client.
type FunHouse struct {
	Client    *ch.Client
	ChunkSize int
}

// New creates a Funhouse and connects to clickhouse.
func New(ctx context.Context, url string, chunkSize int) (fh *FunHouse, err error) {

	client, err := ch.Dial(ctx, ch.Options{Address: url})
	if err != nil {
		return
	}

	fh = &FunHouse{
		Client:    client,
		ChunkSize: chunkSize,
	}

	return
}

// UpsertTables creates a table if it does not exist.
func (fh *FunHouse) UpsertTable(ctx context.Context, tbl table.Table) (err error) {

	err = fh.Client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s\n%s", tbl.Name, tbl.Ddl),
	})
	return
}

// Lengther specifies getting and adding a length attribute for validation of block operations.
// In practice, it will be the object we're appending or chunking to/from as well.
type Lengther interface {
	AddLen(size int)
	Len() int
}

// GetColumns reads blocks from a table.
func (fh *FunHouse) GetColumns(ctx context.Context, qs string, tbl table.Table, lngr Lengther) (err error) {

	results := tbl.Results()

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf(qs, tbl.Name),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			lngr.AddLen(block.Rows)
			err := appendResults(results, tbl.Specs, lngr)
			return err
		},
	})
	if err != nil {
		return
	}

	err = tbl.Specs.ValLens(lngr.Len(), lngr)
	return
}

// PutColumns inserts chunks into a table.
func (fh *FunHouse) PutColumns(ctx context.Context, tbl table.Table, lngr Lengther) (err error) {

	err = tbl.Specs.ValLens(lngr.Len(), lngr)
	if err != nil {
		return
	}

	idx := 0
	input := tbl.Input()

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(tbl.Name),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			end := min(idx+fh.ChunkSize, lngr.Len())

			err := chunkInput(input, tbl.Specs, lngr, idx, end)
			if err != nil {
				return err
			}
			//return io.EOF

			idx += fh.ChunkSize
			if idx > lngr.Len() {
				return io.EOF
			}
			// Todo: maybe this fails if chunk size is larger than mcs len on first go?
			//       per eof return just after chunkInput
			//       if so put back per ch example (return nil at least once)

			return nil
		},
	})
	return
}

// unexported

func appendResults(results proto.Results, specs colspec.ColSpec, lngr Lengther) (err error) {

	for _, col := range results {

		switch tc := col.Data.(type) {
		case *proto.ColDateTime64:
			err = specs.Append(col.Name, dt64Values(tc), lngr)
		case *proto.ColEnum:
			err = specs.Append(col.Name, enumValues(tc), lngr)
		case *proto.ColUInt8:
			err = specs.Append(col.Name, uint8Values(tc), lngr)
		case *proto.ColStr:
			err = specs.Append(col.Name, strValues(tc), lngr)
		case *proto.ColArr[string]:
			err = specs.Append(col.Name, strArrayValues(tc), lngr)
		default:
			err = fmt.Errorf("append type switch does not support: %#v\n", col)
		}
		if err != nil {
			return
		}

		col.Data.Reset()
	}

	return
}

func chunkInput(cols proto.Input, specs colspec.ColSpec, lngr Lengther, bgn, end int) (err error) {

	ok := true
	var tt []time.Time
	var ts []string
	var tu []uint8
	var tz [][]string

	for i := range cols {

		switch tc := cols[i].Data.(type) {
		case *proto.ColDateTime64:

			tt, ok = specs.Chunk(cols[i].Name, lngr, bgn, end).([]time.Time)
			tc.AppendArr(tt)
		case *proto.ColEnum:
			ts, ok = specs.Chunk(cols[i].Name, lngr, bgn, end).([]string)
			tc.AppendArr(ts)
		case *proto.ColUInt8:
			tu, ok = specs.Chunk(cols[i].Name, lngr, bgn, end).([]uint8)
			tc.AppendArr(tu)
		case *proto.ColStr:
			ts, ok = specs.Chunk(cols[i].Name, lngr, bgn, end).([]string)
			tc.AppendArr(ts)
		case *proto.ColArr[string]:
			tz, ok = specs.Chunk(cols[i].Name, lngr, bgn, end).([][]string)
			tc.AppendArr(tz)
		default:
			err = fmt.Errorf("chunk type switch does not support: %#v\n", cols[i])
		}
		if !ok {
			err = fmt.Errorf("chunk type assertion failed for: %s %#v\n", cols[i].Name, cols[i])
		}
		if err != nil {
			return
		}
	}

	return
}

// get values from different col types

func strArrayValues(ca *proto.ColArr[string]) (vals [][]string) {

	vals = make([][]string, ca.Rows())

	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}

	return
}

func uint8Values(ci *proto.ColUInt8) (vals []uint8) {

	return *ci
}

func dt64Values(cd *proto.ColDateTime64) (vals []time.Time) {

	vals = make([]time.Time, cd.Rows())

	for i := 0; i < cd.Rows(); i++ {
		vals[i] = cd.Row(i)
	}

	return
}

func strValues(cs *proto.ColStr) (vals []string) {

	vals = make([]string, cs.Rows())

	for i := 0; i < cs.Rows(); i++ {
		vals[i] = cs.Row(i)
	}

	return
}

func enumValues(ce *proto.ColEnum) []string {

	return ce.Values
}
