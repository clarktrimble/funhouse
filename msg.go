package funhouse

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/entity"
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

type Cols struct {
	ByName map[string]proto.Column
	Names  []string
	// func (Input) Columns  -> returns "(foo, bar, baz)" formatted list of Input column names
}

//fmt.Printf(">>> col type: %T\n", col.Data)

/*
			cols["severity_text"].(*proto.ColEnum).AppendArr(mcs.SeverityTxts[idx:end])
			cols["severity_number"].(*proto.ColUInt8).AppendArr(mcs.SeverityNums[idx:end])
			cols["name"].(*proto.ColStr).AppendArr(mcs.Names[idx:end])
			cols["body"].(*proto.ColStr).AppendArr(mcs.Bodies[idx:end])
			cols["arr"].(*proto.ColArr[string]).AppendArr(mcs.Tagses[idx:end])
	for _, col := range results {
		found := ""
		var valr Valuer

		switch col.Data.(type) {
		case *proto.ColDateTime64:
			found = "*proto.ColDateTime64"
			valr = dt64Values
		default:
			continue
		}

		fmt.Printf(">>> %s %s %s\n", col.Name, found, valr)
	}
*/

/*
	func dt64Values(cr proto.ColResult) (vals []time.Time) {
			mcs.Len += block.Rows
			for _, col := range result {
				fmt.Printf(">>> col type: %T\n", col)
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
*/
//}

func (cols Cols) Input() (input proto.Input) {

	input = proto.Input{}

	for _, name := range cols.Names {
		input = append(input, proto.InputColumn{
			Name: name,
			Data: cols.ByName[name],
		})
	}

	return
}

func (cols Cols) Results() (results proto.Results) {

	results = proto.Results{}

	for _, name := range cols.Names {
		results = append(results, proto.ResultColumn{
			Name: name,
			Data: cols.ByName[name],
		})
	}

	return
}

// mcs.Timestamps = append(mcs.Timestamps, dt64Values(col.Data)...)
// case "ts":
// msgs.SetTimestamps(dt64Values(col.Data))
// cols["ts"].(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
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
