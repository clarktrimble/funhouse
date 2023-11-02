package entity

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"
)

type Severity struct {
	Txt string
	Num uint8
}

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

func (msgs Msgs) Timestamps() (vals []time.Time) {

	vals = make([]time.Time, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Timestamp
	}

	return
}

func (msgs Msgs) SetTimestamps(vals []time.Time) {

	for i, val := range vals {
		msgs[i].Timestamp = val
	}
}

func (msgs Msgs) SeverityTxts() (vals []string) {

	vals = make([]string, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Severity.Txt
	}

	return
}

func (msgs Msgs) SetSeverityTxts(vals []string) {

	for i, val := range vals {
		msgs[i].Severity.Txt = val
	}
}

func (msgs Msgs) SeverityNums() (vals []uint8) {

	vals = make([]uint8, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Severity.Num
	}

	return
}

func (msgs Msgs) SetSeverityNums(vals []uint8) {

	for i, val := range vals {
		msgs[i].Severity.Num = val
	}
}

func (msgs Msgs) Names() (vals []string) {

	vals = make([]string, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Name
	}

	return
}

func (msgs Msgs) SetNames(vals []string) {

	for i, val := range vals {
		msgs[i].Name = val
	}
}

func (msgs Msgs) Bodies() (vals []string) {

	vals = make([]string, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Body
	}

	return
}

func (msgs Msgs) SetBodies(vals []string) {

	for i, val := range vals {
		msgs[i].Body = val
	}
}

func (msgs Msgs) Tagses() (vals [][]string) {

	vals = make([][]string, len(msgs))

	for i, msg := range msgs {
		vals[i] = msg.Tags
	}

	return
}

func (msgs Msgs) SetTags(vals [][]string) {

	for i, val := range vals {
		msgs[i].Tags = val
	}
}

func SampleMsgs(count int) (msgs Msgs) {

	msgs = make(Msgs, count)
	for i := 0; i < count; i++ {
		msgs[i] = Msg{
			Timestamp: time.Now(),
			Severity: Severity{
				Txt: "INFO",
				Num: 3,
			},
			Name: fmt.Sprintf("name-%d", i),
			Body: "body",
			Tags: []string{"foo", "bar"},
		}

		time.Sleep(time.Duration(rand.Intn(33)) * time.Nanosecond)
	}

	return
}
