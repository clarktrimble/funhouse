package funhouse

import (
	"context"
	"fmt"
	"funhouse/table"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type FunHouse struct {
	Client    *ch.Client
	ChunkSize int
}

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

func (fh *FunHouse) UpsertTable(ctx context.Context, tbl table.Table) (err error) {

	err = fh.Client.Do(ctx, ch.Query{
		Body: fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s\n%s", tbl.Name, tbl.Ddl),
	})
	return
}

type Appender interface {
	AddLen(size int)
	Append(name string, vals any)
}

func (fh *FunHouse) GetColumns(ctx context.Context, tbl table.Table, appr Appender) (err error) {

	//results := results()
	results := tbl.Cols.Results()

	err = fh.Client.Do(ctx, ch.Query{
		//Body:   fmt.Sprintf("select * from %s limit 5", MsgTable),
		Body:   fmt.Sprintf("select * from %s", tbl.Name),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			appr.AddLen(block.Rows)
			appendResults(results, appr)
			return nil
		},
	})
	return
}

type Chunker interface {
	Chunk(name string, bgn, end int) (vals any)
	Len() int
}

func (fh *FunHouse) PutColumns(ctx context.Context, tbl table.Table, chkr Chunker) (err error) {

	idx := 0
	cols := tbl.Cols.ByName
	input := tbl.Cols.Input()

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(tbl.Name),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			end := min(idx+fh.ChunkSize, chkr.Len())

			chunkInput(cols, chkr, idx, end)
			//return io.EOF

			idx += fh.ChunkSize
			if idx > chkr.Len() {
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

// chaffer/dechaffer

func appendResults(results proto.Results, appr Appender) {

	for _, col := range results {

		switch col.Data.(type) {
		case *proto.ColDateTime64:
			appr.Append(col.Name, dt64Values(col.Data))
		case *proto.ColEnum:
			appr.Append(col.Name, enumValues(col.Data))
		case *proto.ColUInt8:
			appr.Append(col.Name, uint8Values(col.Data))
		case *proto.ColStr:
			appr.Append(col.Name, strValues(col.Data))
		case *proto.ColArr[string]:
			appr.Append(col.Name, strArrayValues(col.Data))
		default:
			continue // Todo: wot?
		}
		col.Data.Reset()
	}
}

func chunkInput(cols map[string]proto.Column, chkr Chunker, bgn, end int) {

	for name, col := range cols {

		// Todo: check for vals assertion fail
		switch col.(type) {
		case *proto.ColDateTime64:
			vals := chkr.Chunk(name, bgn, end).([]time.Time)
			col.(*proto.ColDateTime64).AppendArr(vals)
		case *proto.ColEnum:
			vals := chkr.Chunk(name, bgn, end).([]string)
			col.(*proto.ColEnum).AppendArr(vals)
		case *proto.ColUInt8:
			vals := chkr.Chunk(name, bgn, end).([]uint8)
			col.(*proto.ColUInt8).AppendArr(vals)
		case *proto.ColStr:
			vals := chkr.Chunk(name, bgn, end).([]string)
			col.(*proto.ColStr).AppendArr(vals)
		case *proto.ColArr[string]:
			vals := chkr.Chunk(name, bgn, end).([][]string)
			col.(*proto.ColArr[string]).AppendArr(vals)
		default:
			continue // Todo: wot?
		}
	}
}

// get values from different col types

func strArrayValues(cr proto.ColResult) (vals [][]string) {

	vals = make([][]string, cr.Rows())

	ca, ok := cr.(*proto.ColArr[string])
	if !ok {
		return
		// Todo: handle maybe prescan?
	}

	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}
	return
}

func uint8Values(cr proto.ColResult) (vals []uint8) {

	ci, ok := cr.(*proto.ColUInt8)
	if !ok {
		return
	}

	return *ci
}

func dt64Values(cr proto.ColResult) (vals []time.Time) {

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

func strValues(cr proto.ColResult) (vals []string) {

	vals = []string{}

	cs, ok := cr.(*proto.ColStr)
	if !ok {
		return
	}

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

func enumValues(cr proto.ColResult) []string {

	ce, ok := cr.(*proto.ColEnum)
	if !ok {
		return []string{}
	}

	return ce.Values
}
