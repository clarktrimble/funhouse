package colspec_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	. "funhouse/colspec"
	"funhouse/entity"
)

// Todo: check errors

func TestColSpec(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ColSpec Suite")
}

var _ = Describe("ColSpec", func() {
	var (
		specs ColSpec
		msgs  *entity.MsgCols
		err   error
	)
	BeforeEach(func() {
		specs = ColSpec{
			TypeName: "MsgCols",
			PkgPath:  "funhouse/entity",
			ColToFld: map[string]string{
				"ts":              "Timestamps",
				"severity_text":   "SeverityTxts",
				"severity_number": "SeverityNums",
				"name":            "Names",
				"body":            "Bodies",
				"arr":             "Tagses",
			},
		}
		msgs = entity.SampleMsgCols(3)
	})

	Describe("creating colspecs from a struct", func() {
		var (
			newSpecs ColSpec
		)

		JustBeforeEach(func() {
			newSpecs, err = New(msgs)
		})

		When("all is well", func() {
			It("says the nicest things", func() {
				Expect(err).ToNot(HaveOccurred())
				Expect(newSpecs).To(Equal(specs))
			})
		})
	})

	Describe("checking that all columns are a given length", func() {

		JustBeforeEach(func() {
			err = specs.ValLens(3, msgs)
		})

		When("all is well", func() {
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
				msgs = entity.SampleMsgCols(9)
			})

			It("produces the chunk", func() {
				Expect(vals).To(Equal([]string{"INFO", "INFO", "INFO"}))
			})
		})
	})

	Describe("appending a slice to column", func() {

		JustBeforeEach(func() {
			err = specs.Append("arr", [][]string{{"froo", "brar"}}, msgs)
		})

		When("all is well", func() {
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
