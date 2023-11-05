package colspec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

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

func (specs ColSpecs) ValidateCols(ln int, obj any) (err error) {

	// Todo: check for obj suitable and that fields are slice
	//       maybe store name of struct from harvesting of fields?

	// check that all columns are of a given length

	errTxt := []string{}
	ve := reflect.ValueOf(obj).Elem()

	for _, cs := range specs {
		fln := ve.FieldByName(cs.Name).Len()
		if ln != fln {
			errTxt = append(errTxt, fmt.Sprintf("%s has len %d expected %d", cs.Name, fln, ln))
		}
	}

	if len(errTxt) != 0 {
		err = fmt.Errorf("validation failed: %s", strings.Join(errTxt, ","))
	}

	return
}

// func (specs ColSpecs) Chunk(fieldName string, obj any, bgn, end int) (vals any) {
func (specs ColSpecs) Chunk(colName string, obj any, bgn, end int) (vals any) {

	fieldName := ""
	for _, cs := range specs {
		if cs.Tag != colName {
			continue // Todo: lookup! loop here is blah
		}
		fieldName = cs.Name
	}

	voe := reflect.ValueOf(obj).Elem()
	return voe.FieldByName(fieldName).Slice(bgn, end).Interface()
}

func (specs ColSpecs) Append(colName string, vals any, obj any) (err error) {

	// Todo: wring hands about vals type

	ve := reflect.ValueOf(obj).Elem()

	for _, cs := range specs {
		if cs.Tag != colName {
			continue // Todo: lookup! loop here is blah
		}

		field := ve.FieldByName(cs.Name)

		// getting real trouble wo this check
		vov := reflect.ValueOf(vals)
		if vov.Kind() != reflect.Slice {
			err = fmt.Errorf("cannot append non-slice: %#v", vals)
			return
		}

		field.Set(reflect.AppendSlice(field, vov))
	}

	return
}
