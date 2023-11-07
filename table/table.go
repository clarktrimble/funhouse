// Package table models a clickhouse table.
package table

import (
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
)

// Col is a name and ch proto Column.
type Col struct {
	Name string
	Data proto.Column
}

// Table has everything we need to use a "col" struct with funhouse.
type Table struct {
	Name  string
	Ddl   string
	Cols  []Col
	Specs colspec.ColSpec // Todo: prolly not here??
}

// Input packs Columns in the form of an Input.
func (tbl Table) Input() (input proto.Input) {

	input = proto.Input{}

	for _, col := range tbl.Cols {
		input = append(input, proto.InputColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}

	return
}

// Results packs Columns in the form of a Results.
func (tbl Table) Results() (results proto.Results) {

	results = proto.Results{}

	for _, col := range tbl.Cols {

		results = append(results, proto.ResultColumn{
			Name: col.Name,
			Data: col.Data,
		})
	}

	return
}
