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

// Todo: len check
func (mcs *MsgCols) Chunk(name string, bgn, end int) (vals any) {
	// cols["ts"].(*proto.ColDateTime64).AppendArr(mcs.Timestamps[idx:end])
	switch name {
	case "ts":
		vals = mcs.Timestamps[bgn:end]
	case "severity_text":
		vals = mcs.SeverityTxts[bgn:end]
	case "severity_number":
		vals = mcs.SeverityNums[bgn:end]
	case "name":
		vals = mcs.Names[bgn:end]
	case "body":
		vals = mcs.Bodies[bgn:end]
	case "arr":
		vals = mcs.Tagses[bgn:end]
	default:
		panic(fmt.Errorf("oops: no such name"))
	}

	return
}

// Todo: len check?
// Todo: return err
// Todo: use struct tags??
func (mcs *MsgCols) Append(name string, vals any) {

	var ok bool
	var tt []time.Time
	var ts []string
	var tu []uint8
	var tz [][]string

	switch name {
	case "ts":
		tt, ok = vals.([]time.Time)
		mcs.Timestamps = append(mcs.Timestamps, tt...)
	case "severity_text":
		ts, ok = vals.([]string)
		mcs.SeverityTxts = append(mcs.SeverityTxts, ts...)
	case "severity_number":
		tu, ok = vals.([]uint8)
		mcs.SeverityNums = append(mcs.SeverityNums, tu...)
	case "name":
		ts, ok = vals.([]string)
		mcs.Names = append(mcs.Names, ts...)
	case "body":
		ts, ok = vals.([]string)
		mcs.Bodies = append(mcs.Bodies, ts...)
	case "arr":
		tz, ok = vals.([][]string)
		mcs.Tagses = append(mcs.Tagses, tz...)
	}
	if !ok {
		// trust, but verify
		err := fmt.Errorf("oops")
		panic(err)
	}
}

func (mcs *MsgCols) AddLen(size int) {

	mcs.Len += size
}

func (mcs *MsgCols) Row(idx int) Msg {

	// Todo: validate that cols are same len and cover idx

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
