package funhouse

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"chtest/entity"
)

var (
	MsgTable = "test_table_insert"
)

func (fh *FunHouse) CreateMsgTable(ctx context.Context) (err error) {

	err = fh.Client.Do(ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS test_table_insert
(
	ts                DateTime64(9),
	severity_text     Enum8('INFO'=1, 'DEBUG'=2),
	severity_number   UInt8,
	body              String,
	name              String,
	arr               Array(String)
) ENGINE = Memory`,
	})
	return
}

func (fh *FunHouse) GetMsgs(ctx context.Context) (msgs entity.Msgs, err error) {

	msgs = entity.Msgs{}
	results := results()

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", MsgTable),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			blockMsgs := make(entity.Msgs, block.Rows)
			for _, col := range results {
				switch col.Name {
				case "ts":
					msgs.SetTimestamps(dt64Values(col.Data))
				case "severity_text":
					msgs.SetSeverityTxts(enumValues(col.Data))
				case "severity_number":
					msgs.SetSeverityNums(uint8Values(col.Data))
				case "name":
					msgs.SetNames(strValues(col.Data))
				case "body":
					msgs.SetBodies(strValues(col.Data))
				case "arr":
					msgs.SetTags(strArrayValues(col.Data))
				}
				col.Data.Reset()
			}

			msgs = append(msgs, blockMsgs...)
			return nil
		},
	})
	return
}

func (fh *FunHouse) PutMsgs(ctx context.Context, msgs entity.Msgs) (err error) {

	idx := 0
	cols := columns()
	input := inputs(cols)

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(MsgTable),
		Input: input,
		OnInput: func(ctx context.Context) error {
			input.Reset()

			chunk := msgs[idx:min(idx+fh.ChunkSize, len(msgs))]

			cols["ts"].(*proto.ColDateTime64).AppendArr(chunk.Timestamps())
			cols["severity_text"].(*proto.ColEnum).AppendArr(chunk.SeverityTxts())
			cols["severity_number"].(*proto.ColUInt8).AppendArr(chunk.SeverityNums())
			cols["name"].(*proto.ColStr).AppendArr(chunk.Names())
			cols["body"].(*proto.ColStr).AppendArr(chunk.Bodies())
			cols["arr"].(*proto.ColArr[string]).AppendArr(chunk.Tagses())

			idx += fh.ChunkSize
			if idx > len(msgs) {
				return io.EOF
			}

			return nil
		},
	})
	return
}

func names() []string {

	// order, counter-order, disorder!

	return []string{
		"ts",
		"severity_text",
		"severity_number",
		"body",
		"name",
		"arr",
	}
}

// Todo: think about a col type
//   - input() and result()
//   - per proto coltype wax on/off ??
func columns() map[string]proto.Column {

	return map[string]proto.Column{
		"ts":              (&proto.ColDateTime64{}).WithLocation(time.UTC).WithPrecision(proto.PrecisionNano),
		"severity_text":   &proto.ColEnum{},
		"severity_number": &proto.ColUInt8{},
		"body":            &proto.ColStr{},
		"name":            &proto.ColStr{},
		"arr":             (&proto.ColStr{}).Array(),
	}

}

//┌────────────────────────────ts─┬─severity_text─┬─severity_number─┬─body──┬─name─┬─arr─────────────────┐
//│ 2010-01-01 10:22:33.000345678 │ INFO					│							 10 │ Hello │ name │ ['foo','bar','baz'] │
//│ 2010-01-01 10:22:33.000345678 │ INFO					│							 10 │ Hello │ name │ ['foo','bar','baz'] │

//>>> out: proto.Results{proto.ResultColumn{Name:"ts", Data:(*proto.ColDateTime64)(0xc0000a8720)}, proto.ResultColumn{Name:"severity_text", Data:(*proto.ColEnum)(0xc0000e8200)}, proto.ResultColumn{Name:"severity_number", Data:(*proto.ColUInt8)(0xc0000ea090)}, proto.ResultColumn{Name:"body", Data:(*proto.ColStr)(0xc0000a86c0)}, proto.ResultColumn{Name:"name", Data:(*proto.ColStr)(0xc0000a86f0)}, proto.ResultColumn{Name:"arr", Data:(*proto.ColArr[string])(0xc0000a8780)}}
