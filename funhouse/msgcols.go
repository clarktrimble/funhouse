package funhouse

import (
	"context"
	"fmt"
	"io"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"chtest/entity"
)

func (fh *FunHouse) GetMsgColumns(ctx context.Context) (mcs *entity.MsgCols, err error) {

	mcs = &entity.MsgCols{}
	result := results()

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", MsgTable),
		Result: result,
		OnResult: func(ctx context.Context, block proto.Block) error {

			mcs.Len += block.Rows
			for _, col := range result {
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
