package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"

	"funhouse/entity"
	"funhouse/examples/msgdemo/msgtable"
	flt "funhouse/funlitetoo"
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

	tbl := msgtable.New("test_table_too", client)

	err = flt.DropTable(ctx, client, tbl.Table)
	check(err)
	err = flt.UpsertTable(ctx, client, tbl.Table, msgtable.Ddl, "Memory")
	check(err)

	// put some messages to the table and get them back

	err = tbl.PutColumns(ctx, 9, entity.SampleMsgCols(20))
	check(err)

	mcs, err := tbl.GetColumns(ctx)
	check(err)

	// convert to non-columnar messages and print

	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s\n", msgs)
}
