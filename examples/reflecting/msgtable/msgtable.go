// Package msgtable houses details of a table for use with MsgCols.
package msgtable

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
	"funhouse/entity"
	"funhouse/table"
)

// MsgTable creates a table for use with MsgCols.
func MsgTable() (tbl table.Table, err error) {

	specs, err := colspec.New(&entity.MsgCols{})
	if err != nil {
		return
	}

	tbl = table.Table{
		Name:  "test_table_insert",
		Specs: specs,
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
