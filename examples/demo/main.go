package main

import (
	"context"
	"fmt"
	"funhouse"
	"funhouse/entity"
)

func main() {

	ctx := context.Background()

	fh, err := funhouse.New(ctx, "localhost:9000", 9)
	if err != nil {
		panic(err)
	}

	err = fh.CreateMsgTable(ctx)
	if err != nil {
		panic(err)
	}

	//err = fh.PutColumns(ctx, entity.SampleMsgCols(44))
	//if err != nil {
	//panic(err)
	//}

	mcs := &entity.MsgCols{}
	err = fh.GetColumns(ctx, mcs)
	if err != nil {
		panic(err)
	}

	//fmt.Printf(">>> mcs: %#v\n", mcs)
	fmt.Printf(">>> got %d msgs\n", mcs.Len)
	return

	msgs := make(entity.Msgs, mcs.Len)
	for i := 0; i < mcs.Len; i++ {
		msgs[i] = mcs.Row(i)
	}

	//fmt.Printf("%s", msgs[:3])
	fmt.Printf("%s", msgs)
}
