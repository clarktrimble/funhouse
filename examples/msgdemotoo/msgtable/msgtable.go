// Package msgtable provides specifics for putting and getting entity.MsgCols
// to and from a ClickHouse table by implementing Tabler from the funlitetoo pkg.
package msgtable

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"funhouse/entity"
	flt "funhouse/funlitetoo"
)

const (
	ddl = `(
		ts                DateTime64(9),
		severity_text     Enum8('INFO'=1, 'DEBUG'=2),
		severity_number   UInt8,
		body              String,
		name              String,
		arr               Array(String)
	)`
)

// Cols are columns typed corresponding to those in the table.
type Cols struct {
	Ts          *proto.ColDateTime64
	SeverityTxt *proto.ColEnum
	SeverityNum *proto.ColUInt8
	Body        *proto.ColStr
	Name        *proto.ColStr
	Arr         *proto.ColArr[string]
}

// MsgTable represents a table for msg's.
//
// Instances are very much not safe for concurrent use.
// Cols and Data are caught up in the details of getting from or putting to a table.
type MsgTable struct {
	Name string
	Cols Cols
	Data *entity.MsgCols
}

// New creates a MsgTable.
func New(name string) *MsgTable {
	return &MsgTable{
		Name: name,
		Cols: Cols{
			Ts:          (&proto.ColDateTime64{}).WithLocation(time.UTC).WithPrecision(proto.PrecisionNano),
			SeverityTxt: &proto.ColEnum{},
			SeverityNum: &proto.ColUInt8{},
			Body:        &proto.ColStr{},
			Name:        &proto.ColStr{},
			Arr:         (&proto.ColStr{}).Array(),
		},
		Data: &entity.MsgCols{},
	}
}

// TableName returns the name of the table.
func (mt *MsgTable) TableName() string {

	return mt.Name
}

// Total returns the total number of msg's in Data.
func (mt *MsgTable) Total() int {

	return mt.Data.Length
}

// CheckLen checks that all column slices in Data are of the same length.
func (mt *MsgTable) CheckLen() error {

	return mt.Data.CheckLen()
}

// Ddl returns ddl to create the table in ClickHouse.
func (mt *MsgTable) Ddl() string {

	return ddl
}

// ColNames returns cols and their names suitiable for constructing Input or Results.
func (mt *MsgTable) ColNames() (cols []proto.Column, names []string) {

	cols = []proto.Column{
		mt.Cols.Ts,
		mt.Cols.SeverityTxt,
		mt.Cols.SeverityNum,
		mt.Cols.Body,
		mt.Cols.Name,
		mt.Cols.Arr,
	}

	names = []string{
		"ts",
		"severity_text",
		"severity_number",
		"body",
		"name",
		"arr",
	}

	return
}

// AppendTo appends from Data to Cols (input to table)
func (mt *MsgTable) AppendTo(idx, end int) {

	mt.Cols.Ts.AppendArr(mt.Data.Timestamps[idx:end])
	mt.Cols.SeverityTxt.AppendArr(mt.Data.SeverityTxts[idx:end])
	mt.Cols.SeverityNum.AppendArr(mt.Data.SeverityNums[idx:end])
	mt.Cols.Body.AppendArr(mt.Data.Bodies[idx:end])
	mt.Cols.Name.AppendArr(mt.Data.Names[idx:end])
	mt.Cols.Arr.AppendArr(mt.Data.Tagses[idx:end])
}

// AppendFrom appends from Cols to Data (results from table)
func (mt *MsgTable) AppendFrom(count int, results proto.Results) (err error) {

	mt.Data.Length += count
	for _, col := range results {
		switch col.Name {
		case "ts":
			flt.Append(&mt.Data.Timestamps, mt.Cols.Ts)
		case "severity_text":
			flt.Append(&mt.Data.SeverityTxts, mt.Cols.SeverityTxt)
		case "severity_number":
			flt.Append(&mt.Data.SeverityNums, mt.Cols.SeverityNum)
		case "body":
			flt.Append(&mt.Data.Bodies, mt.Cols.Body)
		case "name":
			flt.Append(&mt.Data.Names, mt.Cols.Name)
		case "arr":
			flt.Append(&mt.Data.Tagses, mt.Cols.Arr)
		}
		col.Data.Reset()
	}

	return mt.Data.CheckLen()
}
