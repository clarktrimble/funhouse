package msgtable

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
	"funhouse/table"
)

func MsgTable() table.Table {

	return table.Table{
		Name: "test_table_insert",
		Specs: colspec.ColSpecs{
			"ts":              "Timestamps",
			"severity_text":   "SeverityTxts",
			"severity_number": "SeverityNums",
			"name":            "Names",
			"body":            "Bodies",
			"arr":             "Tagses",
		},
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
}

//┌────────────────────────────ts─┬─severity_text─┬─severity_number─┬─body──┬─name─┬─arr─────────────────┐
//│ 2010-01-01 10:22:33.000345678 │ INFO          │              10 │ Hello │ name │ ['foo','bar','baz'] │
//│ 2010-01-01 10:22:33.000345678 │ INFO          │              10 │ Hello │ name │ ['foo','bar','baz'] │

//>>> out: proto.Results{proto.ResultColumn{Name:"ts", Data:(*proto.ColDateTime64)(0xc0000a8720)}, proto.ResultColumn{Name:"severity_text", Data:(*proto.ColEnum)(0xc0000e8200)}, proto.ResultColumn{Name:"severity_number", Data:(*proto.ColUInt8)(0xc0000ea090)}, proto.ResultColumn{Name:"body", Data:(*proto.ColStr)(0xc0000a86c0)}, proto.ResultColumn{Name:"name", Data:(*proto.ColStr)(0xc0000a86f0)}, proto.ResultColumn{Name:"arr", Data:(*proto.ColArr[string])(0xc0000a8780)}}
