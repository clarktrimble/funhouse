package funhouse

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/table"
)

type Appender interface {
	AddLen(size int)
	Append(name string, vals any)
}

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

func (fh *FunHouse) PutColumns(ctx context.Context, tbl table.Table, chkr Chunker) (err error) {

	idx := 0
	//cols := columns()
	//input := inputs(cols)
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

// This is the old col'ish msg'ish get
/*
func (fh *FunHouse) GetMsgColumns(ctx context.Context) (mcs *entity.MsgCols, err error) {

	mcs = &entity.MsgCols{}
	results := results()

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", MsgTable),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			mcs.Len += block.Rows
			//blah(results)
			for _, col := range results {

				fmt.Printf(">>> col type: %T\n", col.Data)

				switch col.Name {
				case "ts":
					mcs.Timestamps = append(mcs.Timestamps, dt64Values(col.Data)...)
				case "severity_text":
					mcs.SeverityTxts = append(mcs.SeverityTxts, enumValues(col.Data)...)
				case "severity_number":
					mcs.SeverityNums = append(mcs.SeverityNums, uint8Values(col.Data)...)
				case "name":
					mcs.Names = append(mcs.Names, strValues(col.Data)...)
				case "body":
					mcs.Bodies = append(mcs.Bodies, strValues(col.Data)...)
				case "arr":
					mcs.Tagses = append(mcs.Tagses, strArrayValues(col.Data)...)
				}
				col.Data.Reset()
			}

			return nil
		},
	})
	return
}
*/
// This is the old col'ish msg'ish put
/*
func (fh *FunHouse) PutMsgColumns(ctx context.Context, mcs *entity.MsgCols) (err error) {

	idx := 0
	cols := columns()
	input := inputs(cols)

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(MsgTable),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			end := min(idx+fh.ChunkSize, mcs.Len)

			cols["ts"].(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
			cols["severity_text"].(*proto.ColEnum).AppendArr(mcs.SeverityTxts[idx:end])
			cols["severity_number"].(*proto.ColUInt8).AppendArr(mcs.SeverityNums[idx:end])
			cols["name"].(*proto.ColStr).AppendArr(mcs.Names[idx:end])
			cols["body"].(*proto.ColStr).AppendArr(mcs.Bodies[idx:end])
			cols["arr"].(*proto.ColArr[string]).AppendArr(mcs.Tagses[idx:end])

			idx += fh.ChunkSize
			if idx > mcs.Len {
				return io.EOF
			}

			return nil
		},
	})
	return
}
*/
