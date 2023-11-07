package entity

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

// Severity is a measure of a log message's impact.
type Severity struct {
	Txt string
	Num uint8
}

// Msg is a log message.
type Msg struct {
	Timestamp time.Time
	Severity  Severity
	Name      string
	Body      string
	Tags      []string
}

type Msgs []Msg

func (msgs Msgs) String() string {

	data, err := json.MarshalIndent(msgs, "", "  ")
	if err != nil {
		return "somehow failed to marshal msgs"
	}

	return string(data)
}

// MsgCols represents messages in a column-friendly manner.
// Slice fields with a "col" tag are columns.
type MsgCols struct {
	Length       int
	Timestamps   []time.Time `col:"ts"`
	SeverityTxts []string    `col:"severity_text"`
	SeverityNums []uint8     `col:"severity_number"`
	Names        []string    `col:"name"`
	Bodies       []string    `col:"body"`
	Tagses       [][]string  `col:"arr"`
}

// Row produces a msg for a given row index.
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

// Len returns expected column lengths.
func (mcs *MsgCols) Len() int {

	return mcs.Length
}

// AddLen adds to expected column lengths.
func (mcs *MsgCols) AddLen(size int) {

	mcs.Length += size
}

// MakeMsgCols creates a MsgCols, initialized to a given size.
func MakeMsgCols(size int) *MsgCols {

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

// CheckLenths for simple implementation.
func (mcs *MsgCols) CheckLen() (err error) {

	if mcs.Length != len(mcs.Timestamps) ||
		mcs.Length != len(mcs.SeverityTxts) ||
		mcs.Length != len(mcs.SeverityNums) ||
		mcs.Length != len(mcs.Names) ||
		mcs.Length != len(mcs.Bodies) ||
		mcs.Length != len(mcs.Tagses) {
		err = fmt.Errorf("MsgCols slices are not expected length")
	}

	return
}

// SampleMsgCols creates an example of MsgCols.
func SampleMsgCols(count int) (mcs *MsgCols) {

	mcs = MakeMsgCols(count)

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
