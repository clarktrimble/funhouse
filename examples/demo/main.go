package main

import (
	"context"
	"fmt"

	"funhouse"
	"funhouse/entity"
	"funhouse/msgtable"
)

func main() {

	// create message columns and table objects

	mcs := &entity.MsgCols{}
	msgTable := msgtable.MsgTable()

	// connect with db and create table if needed

	ctx := context.Background()
	fh, err := funhouse.New(ctx, "localhost:9000", 9)
	check(err)

	err = fh.UpsertTable(ctx, msgTable)
	check(err)

	// insert some messages and get them back

	err = fh.PutColumns(ctx, msgTable, entity.SampleMsgCols(30))
	check(err)

	err = fh.GetColumns(ctx, msgTable, mcs)
	check(err)

	//fmt.Printf(">>> mcs: %#v\n", mcs)
	//fmt.Printf(">>> got %d msgs\n", mcs.Len())
	//return

	// convert to non-column messages and print

	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s", msgs)
}

// handle top-level errors
func check(err error) {
	if err != nil {
		panic(err)
	}
}
