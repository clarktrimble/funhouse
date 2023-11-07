package table

import (
	"fmt"

	"github.com/ClickHouse/ch-go/proto"

	"funhouse/colspec"
)

type Col struct {
	Name string
	Data proto.Column
}

type Table struct {
	Name  string
	Ddl   string
	Cols  []Col
	Specs colspec.ColSpec // Todo: prolly not here??

	//ColumnNames []string
	//Columns     []proto.Column
	//colByName map[string]proto.Column
}

func New(name, ddl string, colNames []string, colDatums []proto.Column) (tbl Table, err error) {

	if len(colNames) != len(colDatums) {
		err = fmt.Errorf("table must have equal length column names and columns")
		return
	}

	colByName := map[string]proto.Column{}
	for i, colName := range colNames {
		colByName[colName] = colDatums[i]
	}

	tbl = Table{
		Name: name,
		Ddl:  ddl,
	}

	return
}

func (tbl Table) GetDataCol(name string) proto.Column {

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
