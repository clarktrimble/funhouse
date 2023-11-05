package entity_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"funhouse/entity"
	. "funhouse/entity"
)

func TestEntity(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Entity Suite")
}

var _ = Describe("Entity", func() {

	Describe("exploring colspecs", func() {
		var (
			specs ColSpecs
			msgs  *MsgCols
			err   error
		)

		JustBeforeEach(func() {
			//func New(obj any) (colSpecs ColSpecs, err error) {
			specs, err = New(msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				msgs = SampleMsgCols(3) // Todo: can haz uninit?
			})

			It("says the nicest things", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(specs).To(Equal(entity.ColSpecs{
					{Name: "Timestamps", Tag: "ts"},
					{Name: "SeverityTxts", Tag: "severity_text"},
					{Name: "SeverityNums", Tag: "severity_number"},
					{Name: "Names", Tag: "name"},
					{Name: "Bodies", Tag: "body"},
					{Name: "Tagses", Tag: "arr"},
				}))
			})
		})
	})

	Describe("exploring chunk", func() {
		var (
			specs ColSpecs
			msgs  *MsgCols
			vals  any
			//err   error
		)

		JustBeforeEach(func() {
			//err = specs.AppendToo("ts", []time.Time{time.Time{}}, msgs)
			//err = specs.AppendToo("severity_text", []string{"INFRO", "DEBURG"}, msgs)
			//err = specs.AppendToo("arr", [][]string{{"froo", "brar"}}, msgs)
			//func (specs ColSpecs) ChunkToo(fieldName string, obj any, bgn, end int) (vals any) {
			vals = specs.ChunkToo("SeverityTxts", msgs, 0, 1)

		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{Name: "Tagses", Tag: "arr"},
				}
				msgs = SampleMsgCols(3)
			})

			FIt("says the nicest things", func() {
				//Expect(err).ToNot(HaveOccurred())
				//Expect(msgs.SeverityTxts).To(Equal([]string{"INFO", "INFO", "INFO", "INFRO", "DEBURG"}))
				Expect(vals).To(Equal([]string{"INFO"}))
				/*
					Expect(msgs.Tagses).To(Equal([][]string{
						{"sna", "foo"},
						{"sna", "foo"},
						{"sna", "foo"},
						{"froo", "brar"},
					}))
				*/
				//fmt.Printf(">>> msgs: %#v\n", msgs.Tagses)
				//fmt.Printf(">>> msgs: %#v\n", msgs)
			})
		})
	})

	Describe("exploring append", func() {
		var (
			specs ColSpecs
			msgs  *MsgCols
			err   error
		)

		JustBeforeEach(func() {
			//err = specs.AppendToo("ts", []time.Time{time.Time{}}, msgs)
			//err = specs.AppendToo("severity_text", []string{"INFRO", "DEBURG"}, msgs)
			err = specs.AppendToo("arr", [][]string{{"froo", "brar"}}, msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{Name: "Tagses", Tag: "arr"},
				}
				msgs = SampleMsgCols(3)
			})

			It("says the nicest things", func() {
				Expect(err).ToNot(HaveOccurred())
				//Expect(msgs.SeverityTxts).To(Equal([]string{"INFO", "INFO", "INFO", "INFRO", "DEBURG"}))
				Expect(msgs.Tagses).To(Equal([][]string{
					{"sna", "foo"},
					{"sna", "foo"},
					{"sna", "foo"},
					{"froo", "brar"},
				}))
				//fmt.Printf(">>> msgs: %#v\n", msgs.Tagses)
				//fmt.Printf(">>> msgs: %#v\n", msgs)
			})
		})
	})

	Describe("exploring validation", func() {
		var (
			specs ColSpecs
			msgs  *MsgCols
			err   error
		)

		JustBeforeEach(func() {
			//err = ReflectoToo(msgs)
			err = specs.ValidateToo(3, msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{Name: "Tagses", Tag: "arr"},
				}
				msgs = SampleMsgCols(3)
			})

			It("says the nicest things", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})
})
