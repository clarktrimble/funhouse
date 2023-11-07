// Package msg provides relatively simple implementations for getting and putting MsgCols.
package msg

// Todo: spell check comments w lint??

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/entity"
	fl "funhouse/funlite"
)

var (
	tableName = "test_table_insert"
	dataCols  = map[string]proto.Column{
		"ts":              (&proto.ColDateTime64{}).WithLocation(time.UTC).WithPrecision(proto.PrecisionNano),
		"severity_text":   &proto.ColEnum{},
		"severity_number": &proto.ColUInt8{},
		"body":            &proto.ColStr{},
		"name":            &proto.ColStr{},
		"arr":             (&proto.ColStr{}).Array(),
	}
	colNames = []string{
		"ts",
		"severity_text",
		"severity_number",
		"body",
		"name",
		"arr",
	}
)

// PutColumns inserts the given messages.
func PutColumns(ctx context.Context, client *ch.Client, chunkSize int, mcs *entity.MsgCols) (err error) {

	err = mcs.CheckLen()
	if err != nil {
		return
	}

	idx := 0
	input := fl.Input(colNames, dataCols)

	err = client.Do(ctx, ch.Query{
		Body:  input.Into(tableName),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			if idx > mcs.Length {
				return io.EOF
			}

			end := min(idx+chunkSize, mcs.Length)
			idx += chunkSize

			dataCols["ts"].(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
			dataCols["severity_text"].(*proto.ColEnum).AppendArr(mcs.SeverityTxts[idx:end])
			dataCols["severity_number"].(*proto.ColUInt8).AppendArr(mcs.SeverityNums[idx:end])
			dataCols["name"].(*proto.ColStr).AppendArr(mcs.Names[idx:end])
			dataCols["body"].(*proto.ColStr).AppendArr(mcs.Bodies[idx:end])
			dataCols["arr"].(*proto.ColArr[string]).AppendArr(mcs.Tagses[idx:end])

			return nil
		},
	})
	return
}

// GetColumns gets messages given a query string.
func GetColumns(ctx context.Context, client *ch.Client, qSpec string) (mcs *entity.MsgCols, err error) {

	mcs = &entity.MsgCols{}
	results := fl.Results(colNames, dataCols)

	err = client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf(qSpec, tableName),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			mcs.Length += block.Rows
			for _, col := range results {
				switch col.Name {
				case "ts":
					mcs.Timestamps = append(mcs.Timestamps, fl.Dt64Values(col.Data)...)
				case "severity_text":
					mcs.SeverityTxts = append(mcs.SeverityTxts, fl.EnumValues(col.Data)...)
				case "severity_number":
					mcs.SeverityNums = append(mcs.SeverityNums, fl.UInt8Values(col.Data)...)
				case "name":
					mcs.Names = append(mcs.Names, fl.StrValues(col.Data)...)
				case "body":
					mcs.Bodies = append(mcs.Bodies, fl.StrValues(col.Data)...)
				case "arr":
					mcs.Tagses = append(mcs.Tagses, fl.StrArrayValues(col.Data)...)
				}
				col.Data.Reset()
			}

			err = mcs.CheckLen()
			if err != nil {
				return err
			}

			return nil
		},
	})
	return
}
