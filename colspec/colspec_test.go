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

	/*
		Describe("exploring colspecs", func() {
			var (
				specs ColSpecs
				msgs  *entity.MsgCols
				err   error
			)

			JustBeforeEach(func() {
				//func New(obj any) (colSpecs ColSpecs, err error) {
				specs, err = New(msgs)
			})

			When("all is well", func() {
				BeforeEach(func() {
					msgs = entity.SampleMsgCols(3) // Todo: can haz uninit?
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
	*/

	Describe("exploring chunk", func() {
		var (
			specs ColSpecs
			msgs  *entity.MsgCols
			vals  any
			//err   error
		)

		JustBeforeEach(func() {
			//err = specs.AppendToo("ts", []time.Time{time.Time{}}, msgs)
			//err = specs.AppendToo("severity_text", []string{"INFRO", "DEBURG"}, msgs)
			//err = specs.AppendToo("arr", [][]string{{"froo", "brar"}}, msgs)
			//func (specs ColSpecs) ChunkToo(fieldName string, obj any, bgn, end int) (vals any) {
			vals = specs.ChunkToo("SeverityTxts", msgs, 3, 6)

		})

		When("all is well", func() {
			BeforeEach(func() {
				specs = ColSpecs{
					{"Timestamps", "ts"},
					{"SeverityTxts", "severity_text"},
					{Name: "Tagses", Tag: "arr"},
				}
				msgs = entity.SampleMsgCols(9)
			})

			FIt("says the nicest things", func() {
				//Expect(err).ToNot(HaveOccurred())
				//Expect(msgs.SeverityTxts).To(Equal([]string{"INFO", "INFO", "INFO", "INFRO", "DEBURG"}))
				Expect(vals).To(Equal([]string{"INFO", "INFO", "INFO"}))
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
})
