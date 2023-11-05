package entity

import (
	"fmt"
	"math/rand"
	"time"
)

// MsgCols is a column-oriented take on messages.
type MsgCols struct {
	Length       int
	Timestamps   []time.Time `col:"ts"`
	SeverityTxts []string    `col:"severity_text"`
	SeverityNums []uint8     `col:"severity_number"`
	Names        []string    `col:"name"`
	Bodies       []string    `col:"body"`
	Tagses       [][]string  `col:"arr"`
}

// NewMsgCols creates MsgCols, initialized to a given size.
func NewMsgCols(size int) *MsgCols {

	return &MsgCols{
		Length:       size,
		Timestamps:   make([]time.Time, size),
		SeverityTxts: make([]string, size),
		SeverityNums: make([]uint8, size),
		Names:        make([]string, size),
		Bodies:       make([]string, size),
		Tagses:       make([][]string, size),
	}
}

// Len returns the column lengths.
func (mcs *MsgCols) Len() int {

	return mcs.Length
}

// AddLen adds to the column lengths.
func (mcs *MsgCols) AddLen(size int) {

	mcs.Length += size
}

// Row creates a Mesg for a given row.
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

// SampleMsgCols creates an example of MsgCols.
func SampleMsgCols(count int) (mcs *MsgCols) {

	mcs = NewMsgCols(count)

	then := time.Now().Add(-time.Hour)
	offset := 0

	for i := 0; i < count; i++ {
		mcs.Timestamps[i] = then.Add(time.Duration(offset) * time.Nanosecond)
		mcs.SeverityTxts[i] = "INFO"
		mcs.SeverityNums[i] = 3
		mcs.Names[i] = fmt.Sprintf("name-%d", i)
		mcs.Bodies[i] = "body from colzz"
		mcs.Tagses[i] = []string{"sna", "foo"}

		offset += rand.Intn(33)
	}

	return
}
