package table

import "github.com/ClickHouse/ch-go/proto"

type Table struct {
	Name string
	Ddl  string
	Cols Cols
}

type Cols struct {
	ByName map[string]proto.Column
	Names  []string
	// func (Input) Columns  -> returns "(foo, bar, baz)" formatted list of Input column names
}

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
