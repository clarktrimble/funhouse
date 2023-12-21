package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"

	"funhouse/entity"
	"funhouse/examples/msgdemo/msgtable"
	flt "funhouse/funlite"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {

	// connect with db and re-create table

	ctx := context.Background()
	client, err := ch.Dial(ctx, ch.Options{Address: "localhost:9000"})
	check(err)

	fh := flt.Fh{Client: client}
	tbl := msgtable.New("test_table_too")

	err = fh.DropTable(ctx, tbl)
	check(err)
	err = fh.UpsertTable(ctx, "Memory", tbl)
	check(err)

	// put some messages to the table and get them back

	tbl.Data = entity.SampleMsgCols(20)
	err = fh.PutInput(ctx, 9, tbl)

	tbl.Data = &entity.MsgCols{}
	err = fh.GetResults(ctx, tbl)
	check(err)

	// convert to non-columnar messages and print

	mcs := tbl.Data

	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s\n", msgs)
}
