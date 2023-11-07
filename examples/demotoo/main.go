package main

import (
	"context"
	"fmt"

	"funhouse/entity"

	"funhouse/examples/demotoo/msg"

	"github.com/ClickHouse/ch-go"
)

func main() {

	// connect with db and create table if needed

	ctx := context.Background()
	client, err := ch.Dial(ctx, ch.Options{Address: "localhost:9000"})
	check(err)

	//err = fh.UpsertTable(ctx, msgTable)
	//check(err)

	// insert some messages and get them back

	mcs := &entity.MsgCols{}

	err = msg.PutColumns(ctx, client, 9, entity.SampleMsgCols(20))
	check(err)

	mcs, err = msg.GetColumns(ctx, client, "select * from %s")
	check(err)

	// convert to non-columnar messages and print

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
