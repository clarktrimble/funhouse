// Package msgtable provides simple if repetitive code dedicated to
// putting and getting entity.MsgCols to and from a ClickHouse table.
//
// I still yearn to pull "chunking" code into something reusable across types,
// but this one is a solid step forward. :)
package msgtable

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"

	"funhouse/entity"
	flt "funhouse/funlitetoo"
)

const (
	// Ddl can be used to create the table in ClickHouse.
	Ddl = `(
		ts                DateTime64(9),
		severity_text     Enum8('INFO'=1, 'DEBUG'=2),
		severity_number   UInt8,
		body              String,
		name              String,
		arr               Array(String)
	)`
)

// MsgTable represents a table for msg's.
type MsgTable struct {
	Table       string
	Client      *ch.Client
	Ts          *proto.ColDateTime64
	SeverityTxt *proto.ColEnum
	SeverityNum *proto.ColUInt8
	Body        *proto.ColStr
	Name        *proto.ColStr
	Arr         *proto.ColArr[string]
	// Todo: think about putting cols into sub-struct
	Mcs *entity.MsgCols
	// Todo: dedicated mcs is blah? (not safe for concurrent, but neither are colses!)
}

//mcs = &entity.MsgCols{}

// New creates a MsgTable.
func New(name string, client *ch.Client) *MsgTable {
	return &MsgTable{
		Table:       name,
		Client:      client,
		Ts:          (&proto.ColDateTime64{}).WithLocation(time.UTC).WithPrecision(proto.PrecisionNano),
		SeverityTxt: &proto.ColEnum{},
		SeverityNum: &proto.ColUInt8{},
		Body:        &proto.ColStr{},
		Name:        &proto.ColStr{},
		Arr:         (&proto.ColStr{}).Array(),
		Mcs:         &entity.MsgCols{},
	}
}

// ColNames returns cols and their names suitiable for constructing Input or Results.
func (mt *MsgTable) ColNames() (cols []proto.Column, names []string) {

	cols = []proto.Column{
		mt.Ts,
		mt.SeverityTxt,
		mt.SeverityNum,
		mt.Body,
		mt.Name,
		mt.Arr,
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

// PutColumns inserts the given messages.
func (mt *MsgTable) PutColumns(ctx context.Context, chunkSize int, mcs *entity.MsgCols) (err error) {

	var idx int

	err = mcs.CheckLen()
	if err != nil {
		return
	}

	input, err := flt.Input(mt)
	if err != nil {
		return
	}

	err = mt.Client.Do(ctx, ch.Query{
		Body:  input.Into(mt.Table),
		Input: input,
		OnInput: func(ctx context.Context) error {

			input.Reset()
			if idx > mcs.Length {
				return io.EOF
			}

			end := min(idx+chunkSize, mcs.Length)

			// MsgTable fields (i.e.: mt.Ts) are the same as provided with "Input: input"

			mt.Ts.AppendArr(mcs.Timestamps[idx:end])
			mt.SeverityTxt.AppendArr(mcs.SeverityTxts[idx:end])
			mt.SeverityNum.AppendArr(mcs.SeverityNums[idx:end])
			mt.Body.AppendArr(mcs.Bodies[idx:end])
			mt.Name.AppendArr(mcs.Names[idx:end])
			mt.Arr.AppendArr(mcs.Tagses[idx:end])

			idx += chunkSize
			// Todo: check that all's well when chunk is bigger than total
			return nil
		},
	})
	return
}

func (mt *MsgTable) Destash(idx, end int) {

	mt.Ts.AppendArr(mt.Mcs.Timestamps[idx:end])
	mt.SeverityTxt.AppendArr(mt.Mcs.SeverityTxts[idx:end])
	mt.SeverityNum.AppendArr(mt.Mcs.SeverityNums[idx:end])
	mt.Body.AppendArr(mt.Mcs.Bodies[idx:end])
	mt.Name.AppendArr(mt.Mcs.Names[idx:end])
	mt.Arr.AppendArr(mt.Mcs.Tagses[idx:end])
}

func (mt *MsgTable) Stash(count int, results proto.Results) (err error) {

	mt.Mcs.Length += count
	for _, col := range results {
		switch col.Name {
		case "ts":
			flt.Append(&mt.Mcs.Timestamps, mt.Ts)
		case "severity_text":
			flt.Append(&mt.Mcs.SeverityTxts, mt.SeverityTxt)
		case "severity_number":
			flt.Append(&mt.Mcs.SeverityNums, mt.SeverityNum)
		case "body":
			flt.Append(&mt.Mcs.Bodies, mt.Body)
		case "name":
			flt.Append(&mt.Mcs.Names, mt.Name)
		case "arr":
			flt.Append(&mt.Mcs.Tagses, mt.Arr)
		}
		col.Data.Reset()
	}

	return mt.Mcs.CheckLen()
}

// GetColumns gets all messages.
func (mt *MsgTable) GetColumns(ctx context.Context) (mcs *entity.MsgCols, err error) {

	mcs = &entity.MsgCols{}

	results, err := flt.Results(mt)
	if err != nil {
		return
	}

	// query hardcoded here as ch-go will error with "raw block: target: 6 (columns) != 2 (target)"
	// when selecting only two cols in the query and yet try to use "results" for all 6
	//
	// might be nice, certainly more col-oriented,to get one column at a time and merge later?
	// another approach would be to make "Results" method aware of select fields
	//
	// and of course would be straight-forward to pass in a "where" clause if needed

	err = mt.Client.Do(ctx, ch.Query{
		Body:   fmt.Sprintf("select * from %s", mt.Table),
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			// MsgTable fields (i.e.: mt.Ts) are the same as provided with "Result: results"

			mcs.Length += block.Rows
			for _, col := range results {
				switch col.Name {
				case "ts":
					flt.Append(&mcs.Timestamps, mt.Ts)
				case "severity_text":
					flt.Append(&mcs.SeverityTxts, mt.SeverityTxt)
				case "severity_number":
					flt.Append(&mcs.SeverityNums, mt.SeverityNum)
				case "body":
					flt.Append(&mcs.Bodies, mt.Body)
				case "name":
					flt.Append(&mcs.Names, mt.Name)
				case "arr":
					flt.Append(&mcs.Tagses, mt.Arr)
				}
				col.Data.Reset()
			}

			return mcs.CheckLen()
		},
	})
	return
}

/*
type ColNamer interface {
	ColNames() (cols []proto.Column, names []string)
	Stash(count int, results proto.Results) (err error)
}

type Fh struct {
	Client *ch.Client
}

// func (mt *MsgTable) GetColumns(ctx context.Context) (mcs *entity.MsgCols, err error) {
func (fh *Fh) GetResults(ctx context.Context, query string, cnr ColNamer) (err error) {

	//mcs = &entity.MsgCols{}

	results, err := flt.Results(cnr)
	if err != nil {
		return
	}
	// aitäh

	err = fh.Client.Do(ctx, ch.Query{
		//err = mt.Client.Do(ctx, ch.Query{
		//Body:   fmt.Sprintf("select * from %s", mt.Table),
		Body:   query,
		Result: results,
		OnResult: func(ctx context.Context, block proto.Block) error {

			return cnr.Stash(block.Rows, results)
			/*
				mcs.Length += block.Rows
				for _, col := range results {
					switch col.Name {
					case "ts":
						flt.Append(&mcs.Timestamps, mt.Ts)
					case "severity_text":
						flt.Append(&mcs.SeverityTxts, mt.SeverityTxt)
					case "severity_number":
						flt.Append(&mcs.SeverityNums, mt.SeverityNum)
					case "body":
						flt.Append(&mcs.Bodies, mt.Body)
					case "name":
						flt.Append(&mcs.Names, mt.Name)
					case "arr":
						flt.Append(&mcs.Tagses, mt.Arr)
					}
					col.Data.Reset()
				}
*/
/*

			//return mcs.CheckLen()
			//return nil
		},
	})
	return
}
*/
