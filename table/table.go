package table

import (
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
)

type Table struct {
	Name  string
	Ddl   string
	Cols  []Col
	Specs colspec.ColSpec
}

type Col struct {
	Name string
	Data proto.Column
}

func (tbl Table) GetData(name string) proto.Column {

	// Todo: lookup!! via New prolly
	for _, col := range tbl.Cols {

		if name == col.Name {
			return col.Data
		}
	}

	return nil
}

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
