package entity

import (
	"fmt"
	"math/rand"
	"time"
)

type MsgCols struct {
	Length       int
	Timestamps   []time.Time
	SeverityTxts []string
	SeverityNums []uint8
	Names        []string
	Bodies       []string
	Tagses       [][]string
}

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

func (mcs *MsgCols) Validate() (err error) {

	ts := len(mcs.Timestamps)
	st := len(mcs.SeverityTxts)
	sn := len(mcs.SeverityNums)
	nm := len(mcs.Names)
	bd := len(mcs.Bodies)
	tg := len(mcs.Tagses)

	if mcs.Length != ts ||
		mcs.Length != st ||
		mcs.Length != sn ||
		mcs.Length != nm ||
		mcs.Length != bd ||
		mcs.Length != tg {
		err = fmt.Errorf(
			"invalid Length:%d ts:%d st:%d sn:%d nm:%d bd:%d tg:%d",
			mcs.Length, ts, st, sn, nm, bd, tg,
		)
	}
	return
}

func (mcs *MsgCols) Chunk(name string, bgn, end int) (vals any) {

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
		vals = nil // noop but want to be obvious
	}

	return
}

func (mcs *MsgCols) Append(name string, vals any) (err error) {

	var ok bool
	var tt []time.Time
	var ts []string
	var tu []uint8
	var tz [][]string

	// Todo: use struct tags??

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
		err = fmt.Errorf("append assertion failed for %s with vals %#v", name, vals)
	}

	return
}

func (mcs *MsgCols) Len() int {

	return mcs.Length
}

func (mcs *MsgCols) AddLen(size int) {

	mcs.Length += size
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
		mcs.Bodies[i] = "body from colzz"
		mcs.Tagses[i] = []string{"sna", "foo"}

		offset += rand.Intn(33)
	}

	return
}
