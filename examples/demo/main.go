package main

import (
	"context"
	"fmt"

	"funhouse"
	"funhouse/colspec"
	"funhouse/entity"
	"funhouse/msgtable"
)

func main() {

	ctx := context.Background()
	msgTable := msgtable.MsgTable()

	fh, err := funhouse.New(ctx, "localhost:9000", 9)
	if err != nil {
		panic(err)
	}

	err = fh.UpsertTable(ctx, msgTable)
	if err != nil {
		panic(err)
	}

	mcs := &entity.MsgCols{}
	specs, err := colspec.New(mcs)
	if err != nil {
		panic(err)
	}

	err = fh.PutColumns(ctx, msgTable, specs, entity.SampleMsgCols(30))
	if err != nil {
		panic(err)
	}

	//func (fh *FunHouse) GetColumns(ctx context.Context, tbl table.Table, specs colspec.ColSpecs, appr Appender) (err error) {
	err = fh.GetColumns(ctx, msgTable, specs, mcs)
	if err != nil {
		panic(err)
	}

	//fmt.Printf(">>> mcs: %#v\n", mcs)
	//fmt.Printf(">>> got %d msgs\n", mcs.Len())
	//return

	msgs := make(entity.Msgs, mcs.Length)
	for i := 0; i < mcs.Length; i++ {
		msgs[i] = mcs.Row(i)
	}

	fmt.Printf("%s", msgs)
}
