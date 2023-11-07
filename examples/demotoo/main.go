package main

import (
	"context"
	"fmt"

	"funhouse"
	"funhouse/entity"

	"funhouse/examples/demotoo/msgtable"
)

func main() {

	// create message columns and table objects

	mcs := &entity.MsgCols{}
	msgTable, err := msgtable.MsgTable()
	check(err)

	// connect with db and create table if needed

	ctx := context.Background()
	fh, err := funhouse.New(ctx, "localhost:9000", 9)
	check(err)

	err = fh.UpsertTable(ctx, msgTable)
	check(err)

	// insert some messages and get them back

	//err = fh.PutColumns(ctx, msgTable, entity.SampleMsgCols(30))
	//check(err)
	//func PutMsgColumns(ctx context.Context, fh *funhouse.FunHouse, tbl table.Table, mcs *entity.MsgCols) (err error) {
	err = msgtable.PutMsgColumns(ctx, fh, msgTable, entity.SampleMsgCols(20))
	check(err)

	//err = fh.GetColumns(ctx, "select * from %s", msgTable, mcs)
	//func GetMsgColumns(ctx context.Context, fh *funhouse.FunHouse, tbl table.Table) (mcs *entity.MsgCols, err error) {

	mcs, err = msgtable.GetMsgColumns(ctx, fh, msgTable)
	check(err)

	// convert to non-column messages and print

	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s\n", msgs)
}

// handle top-level errors, nooo!
func check(err error) {
	if err != nil {
		panic(err)
	}
}
