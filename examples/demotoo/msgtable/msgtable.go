package msgtable

import (
	"context"
	"fmt"
	"funhouse"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/entity"
	"funhouse/table"
)

func MsgTable() (tbl table.Table, err error) {

	tbl = table.Table{
		Name: "test_table_insert",
		Cols: []table.Col{
			{Name: "ts", Data: (&proto.ColDateTime64{}).WithLocation(time.UTC).WithPrecision(proto.PrecisionNano)},
			{Name: "severity_text", Data: &proto.ColEnum{}},
			{Name: "severity_number", Data: &proto.ColUInt8{}},
			{Name: "body", Data: &proto.ColStr{}},
			{Name: "name", Data: &proto.ColStr{}},
			{Name: "arr", Data: (&proto.ColStr{}).Array()},
		},
		Ddl: `(
			ts                DateTime64(9),
			severity_text     Enum8('INFO'=1, 'DEBUG'=2),
			severity_number   UInt8,
			body              String,
			name              String,
			arr               Array(String)
		) ENGINE = Memory`,
	}

	return
}

func PutMsgColumns(ctx context.Context, fh *funhouse.FunHouse, tbl table.Table, mcs *entity.MsgCols) (err error) {

	idx := 0
	input := tbl.Input()

	err = fh.Client.Do(ctx, ch.Query{
		Body:  input.Into(tbl.Name),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			end := min(idx+fh.ChunkSize, mcs.Length)

			tbl.GetData("ts").(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
			tbl.GetData("severity_text").(*proto.ColEnum).AppendArr(mcs.SeverityTxts[idx:end])
			tbl.GetData("severity_number").(*proto.ColUInt8).AppendArr(mcs.SeverityNums[idx:end])
			tbl.GetData("name").(*proto.ColStr).AppendArr(mcs.Names[idx:end])
			tbl.GetData("body").(*proto.ColStr).AppendArr(mcs.Bodies[idx:end])
			tbl.GetData("arr").(*proto.ColArr[string]).AppendArr(mcs.Tagses[idx:end])

			// Todo: fix
			idx += fh.ChunkSize
			if idx > mcs.Length {
				return io.EOF
			}

			return nil
		},
	})
	return
}

// func (fh *FunHouse) GetMsgColumns(ctx context.Context) (mcs *entity.MsgCols, err error) {
func GetMsgColumns(ctx context.Context, fh *funhouse.FunHouse, tbl table.Table) (mcs *entity.MsgCols, err error) {

	mcs = &entity.MsgCols{}
	//result := results()
	results := tbl.Results()

	err = fh.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", tbl.Name),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			mcs.Length += block.Rows
			for _, col := range results {
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
