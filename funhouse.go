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
	Len() int
	//Append(name string, vals any) (err error)
	//Validate() (err error)
}

func (fh *FunHouse) GetColumns(ctx context.Context, tbl table.Table, specs colspec.ColSpecs, appr Appender) (err error) {

	results := tbl.Cols.Results()

	err = fh.Client.Do(ctx, ch.Query{
		//Body:   fmt.Sprintf("select * from %s limit 5", MsgTable),
		Body:   fmt.Sprintf("select * from %s", tbl.Name),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			appr.AddLen(block.Rows)
			err := appendResults(results, specs, appr)
			return err
		},
	})

	//err = appr.Validate()
	//func (specs ColSpecs) ValidateCols(ln int, obj any) (err error) {

	err = specs.ValidateCols(appr.Len(), appr)
	return
}

type Chunker interface {
	//Chunk(name string, bgn, end int) (vals any)
	Len() int
	//Validate() (err error)
}

func (fh *FunHouse) PutColumns(ctx context.Context, tbl table.Table, specs colspec.ColSpecs, chkr Chunker) (err error) {

	//err = chkr.Validate()
	err = specs.ValidateCols(chkr.Len(), chkr)
	if err != nil {
		return
	}

	idx := 0
	cols := tbl.Cols.ByName
	input := tbl.Cols.Input()

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(tbl.Name),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			end := min(idx+fh.ChunkSize, chkr.Len())

			err := chunkInput(cols, specs, chkr, idx, end)
			if err != nil {
				return err
			}
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

func appendResults(results proto.Results, specs colspec.ColSpecs, appr Appender) (err error) {

	for _, col := range results {

		//func (specs ColSpecs) Append(colName string, vals any, obj any) (err error) {

		switch tc := col.Data.(type) {
		case *proto.ColDateTime64:
			specs.Append(col.Name, dt64Values(tc), appr)
			//err = appr.Append(col.Name, dt64Values(tc))
		case *proto.ColEnum:
			specs.Append(col.Name, enumValues(tc), appr)
			//err = appr.Append(col.Name, enumValues(tc))
		case *proto.ColUInt8:
			specs.Append(col.Name, uint8Values(tc), appr)
			//err = appr.Append(col.Name, uint8Values(tc))
		case *proto.ColStr:
			specs.Append(col.Name, strValues(tc), appr)
			//err = appr.Append(col.Name, strValues(tc))
		case *proto.ColArr[string]:
			specs.Append(col.Name, strArrayValues(tc), appr)
			//err = appr.Append(col.Name, strArrayValues(tc))
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

func chunkInput(cols map[string]proto.Column, specs colspec.ColSpecs, chkr Chunker, bgn, end int) (err error) {

	ok := true
	var tt []time.Time
	var ts []string
	var tu []uint8
	var tz [][]string

	for name, col := range cols {

		switch tc := col.(type) {
		case *proto.ColDateTime64:
			//func (specs ColSpecs) Chunk(fieldName string, obj any, bgn, end int) (vals any) {
			//tt, ok = chkr.Chunk(name, bgn, end).([]time.Time)
			tt, ok = specs.Chunk(name, chkr, bgn, end).([]time.Time)
			tc.AppendArr(tt)
		case *proto.ColEnum:
			//ts, ok = chkr.Chunk(name, bgn, end).([]string)
			ts, ok = specs.Chunk(name, chkr, bgn, end).([]string)
			tc.AppendArr(ts)
		case *proto.ColUInt8:
			//tu, ok = chkr.Chunk(name, bgn, end).([]uint8)
			tu, ok = specs.Chunk(name, chkr, bgn, end).([]uint8)
			tc.AppendArr(tu)
		case *proto.ColStr:
			//ts, ok = chkr.Chunk(name, bgn, end).([]string)
			ts, ok = specs.Chunk(name, chkr, bgn, end).([]string)
			tc.AppendArr(ts)
		case *proto.ColArr[string]:
			//tz, ok = chkr.Chunk(name, bgn, end).([][]string)
			tz, ok = specs.Chunk(name, chkr, bgn, end).([][]string)
			tc.AppendArr(tz)
		default:
			err = fmt.Errorf("chunk type switch does not support: %#v\n", col)
		}
		if !ok {
			err = fmt.Errorf("chunk type switch failed for: %s %#v\n", name, col)
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
