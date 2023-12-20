package main

import (
	"context"
	"fmt"

	"github.com/ClickHouse/ch-go"

	"funhouse/entity"
	"funhouse/examples/msgdemotoo/msgtable"
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

	//err = tbl.PutColumns(ctx, 9, entity.SampleMsgCols(20))
	//check(err)

	fh := flt.Fh{Client: client}

	tbl.Mcs = entity.SampleMsgCols(20)
	err = fh.PutInput(ctx, 9, tbl.Mcs.Length, "test_table_too", tbl)

	//func (fh *Fh) PutInput(ctx context.Context, chunkSize, total int, table string, cnr ColNamer) (err error) {
	//mcs, err := tbl.GetColumns(ctx)

	tbl.Mcs = &entity.MsgCols{}
	err = fh.GetResults(ctx, "select * from test_table_too", tbl)
	check(err)

	// convert to non-columnar messages and print

	mcs := tbl.Mcs
	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s\n", msgs)
}
