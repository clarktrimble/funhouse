package colspec_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "funhouse/colspec"
	"funhouse/entity"
)

func TestColSpec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ColSpec Suite")
}

var _ = Describe("ColSpec", func() {
	var (
		specs ColSpecs
		msgs  *entity.MsgCols
		err   error
	)
	// Todo: pull more stuff up here yeah?

	Describe("exploring colspecs", func() {

		JustBeforeEach(func() {
			specs, err = New(msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				msgs = entity.SampleMsgCols(3) // Todo: can haz uninit struct?
			})

			It("says the nicest things", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(specs).To(ConsistOf(ColSpecs{
					{FldName: "Timestamps", ColName: "ts"},
					{FldName: "SeverityTxts", ColName: "severity_text"},
					{FldName: "SeverityNums", ColName: "severity_number"},
					{FldName: "Names", ColName: "name"},
					{FldName: "Bodies", ColName: "body"},
					{FldName: "Tagses", ColName: "arr"},
				}))
			})
		})
	})

	Describe("checking that all columns are a given length", func() {

		JustBeforeEach(func() {
			err = specs.ValidateCols(3, msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{FldName: "Tagses", ColName: "arr"},
				}
				msgs = entity.SampleMsgCols(3)
			})

			It("does not error", func() {
				Expect(err).ToNot(HaveOccurred())
			})
		})
	})

	Describe("retrieving a chunk of column", func() {
		var (
			vals any
		)

		JustBeforeEach(func() {
			vals = specs.Chunk("severity_text", msgs, 3, 6)
		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"SeverityTxts", "severity_text"},
				}
				msgs = entity.SampleMsgCols(9)
			})

			It("produces the chunk", func() {
				Expect(vals).To(Equal([]string{"INFO", "INFO", "INFO"}))
			})
		})
	})

	Describe("appending a slice to column", func() {

		// Todo: check more types?

		JustBeforeEach(func() {
			err = specs.Append("arr", [][]string{{"froo", "brar"}}, msgs)
		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{"Tagses", "arr"},
				}
				msgs = entity.SampleMsgCols(3)
			})

			It("appends the slice", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(msgs.Tagses).To(Equal([][]string{
					{"sna", "foo"},
					{"sna", "foo"},
					{"sna", "foo"},
					{"froo", "brar"},
				}))
			})
		})
	})

})
