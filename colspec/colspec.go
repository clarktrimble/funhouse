package colspec

import (
	"errors"
	"reflect"
	"time"
)

type MsgCols struct {
	Length       int
	Timestamps   []time.Time `col:"ts"`
	SeverityTxts []string    `col:"severity_text"`
	SeverityNums []uint8     `col:"severity_number"`
	Names        []string    `col:"name"`
	Bodies       []string    `col:"body"`
	Tagses       [][]string  `col:"arr"`
}

var ErrInvalidSpecification = errors.New("specification must be a struct pointer")

type ColSpec struct {
	Name string
	Tag  string
}

type ColSpecs []ColSpec

func New(obj any) (specs ColSpecs, err error) {

	s := reflect.ValueOf(obj)
	if s.Kind() != reflect.Ptr {
		err = ErrInvalidSpecification
		return
	}
	s = s.Elem()
	if s.Kind() != reflect.Struct {
		err = ErrInvalidSpecification
		return
	}
	typeOfSpec := s.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		ftype := typeOfSpec.Field(i)
		if !f.CanSet() || ftype.Tag.Get("col") == "" {
			continue
		}

		specs = append(specs, ColSpec{
			Name: ftype.Name,
			Tag:  ftype.Tag.Get("col"),
		})
	}

	return
}

func (specs ColSpecs) ChunkToo(fieldName string, obj any, bgn, end int) (vals any) {

	for _, cs := range specs {
		if cs.Name != fieldName {
			continue // Todo: lookup!
		}

		ve := reflect.ValueOf(obj).Elem()
		//fd := ve.FieldByName(fieldName)
		//fmt.Printf(">>> typeee: %#v\n", reflect.ValueOf(fd))
		//fmt.Printf(">>> typeee: %#v\n", fd.Type().String())
		//switch fd.Type().String() {
		//case "[]int":
		//fmt.Println("int here")
		//case "[]string":
		//fmt.Println("string here")
		//}

		//fmt.Printf(">>> typeee: %#v\n", reflect.TypeOf(obj).Elem())
		//return ve.FieldByName(fieldName)
		return ve.FieldByName(fieldName).Slice(bgn, end).Interface()
	}

	//default: // Todo: keep???
	//vals = nil // noop but want to be obvious

	return
}
