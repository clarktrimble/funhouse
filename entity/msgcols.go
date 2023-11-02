package entity

import (
	"fmt"
	"math/rand"
	"time"
)

type MsgCols struct {
	Len          int
	Timestamps   []time.Time
	SeverityTxts []string
	SeverityNums []uint8
	Names        []string
	Bodies       []string
	Tagses       [][]string
}

func NewMsgCols(size int) *MsgCols {

	return &MsgCols{
		Len:          size,
		Timestamps:   make([]time.Time, size),
		SeverityTxts: make([]string, size),
		SeverityNums: make([]uint8, size),
		Names:        make([]string, size),
		Bodies:       make([]string, size),
		Tagses:       make([][]string, size),
	}
}

func (mcs *MsgCols) Append(vals *MsgCols) {

	mcs.Len += vals.Len
	mcs.Timestamps = append(mcs.Timestamps, vals.Timestamps...)
	mcs.SeverityTxts = append(mcs.SeverityTxts, vals.SeverityTxts...)
	mcs.SeverityNums = append(mcs.SeverityNums, vals.SeverityNums...)
	mcs.Names = append(mcs.Names, vals.Names...)
	mcs.Bodies = append(mcs.Bodies, vals.Bodies...)
	mcs.Tagses = append(mcs.Tagses, vals.Tagses...)

}

func (mcs *MsgCols) Row(idx int) Msg {

	return Msg{
		Timestamp: mcs.Timestamps[idx],
		Severity: Severity{
			Txt: mcs.SeverityTxts[idx],
			Num: mcs.SeverityNums[idx],
		},
		Name: mcs.Names[idx],
		Body: mcs.Bodies[idx],
		Tags: mcs.Tagses[idx],
	}
}

func SampleMsgCols(count int) (mcs *MsgCols) {

	mcs = NewMsgCols(count)

	then := time.Now().Add(-time.Hour)
	offset := 0

	for i := 0; i < count; i++ {
		mcs.Timestamps[i] = then.Add(time.Duration(offset) * time.Nanosecond)
		mcs.SeverityTxts[i] = "INFO"
		mcs.SeverityNums[i] = 3
		mcs.Names[i] = fmt.Sprintf("name-%d", i)
		mcs.Bodies[i] = "body from cols"
		mcs.Tagses[i] = []string{"sna", "foo"}

		offset += rand.Intn(33)
	}

	return
}
