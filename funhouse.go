package funhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type FunHouse struct {
	Client    *ch.Client
	ChunkSize int
}

func New(ctx context.Context, url string, chunkSize int) (fh *FunHouse, err error) {

	client, err := ch.Dial(ctx, ch.Options{Address: url})
	if err != nil {
		return
	}

	fh = &FunHouse{
		Client:    client,
		ChunkSize: chunkSize,
	}

	return
}

// unexported

// get values from different col types

func strArrayValues(cr proto.ColResult) (vals [][]string) {

	vals = make([][]string, cr.Rows())

	ca, ok := cr.(*proto.ColArr[string])
	if !ok {
		return
		// Todo: handle maybe prescan?
	}

	for i := 0; i < ca.Rows(); i++ {
		vals[i] = ca.Row(i)
	}
	return
}

func uint8Values(cr proto.ColResult) (vals []uint8) {

	ci, ok := cr.(*proto.ColUInt8)
	if !ok {
		return
	}

	return *ci
}

func dt64Values(cr proto.ColResult) (vals []time.Time) {

	vals = make([]time.Time, cr.Rows())

	cd, ok := cr.(*proto.ColDateTime64)
	if !ok {
		return
	}

	for i := 0; i < cd.Rows(); i++ {
		vals[i] = cd.Row(i)
	}

	return
}

func strValues(cr proto.ColResult) (vals []string) {

	vals = []string{}

	cs, ok := cr.(*proto.ColStr)
	if !ok {
		return
	}

	err := cs.ForEach(func(i int, str string) error {
		vals = append(vals, str)
		return nil
	})
	if err != nil {
		panic(err)
		// Todo: handle
	}

	return
}

func enumValues(cr proto.ColResult) []string {

	ce, ok := cr.(*proto.ColEnum)
	if !ok {
		return []string{}
	}

	return ce.Values
}
