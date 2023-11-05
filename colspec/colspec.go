package colspec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

// Todo: spruce up
var ErrInvalidSpecification = errors.New("specification must be a struct pointer")

// ColSpec relates a structure's field name to it's column name.
type ColSpec struct {
	FldName string
	ColName string
}

// ColSpecs tracks column fields in a structure.
type ColSpecs []ColSpec

// New create ColSpecs for a given object.
// Slice fields with a "col" tag are picked up.
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
			FldName: ftype.Name,
			ColName: ftype.Tag.Get("col"),
		})
	}

	return
}

// ValidateCols checks that all "col" fields are of a given length.
func (specs ColSpecs) ValidateCols(ln int, obj any) (err error) {

	// Todo: check for obj suitable and that fields are slice
	//       maybe store name of struct from harvesting of fields?

	errTxt := []string{}
	ve := reflect.ValueOf(obj).Elem()

	for _, cs := range specs {
		fln := ve.FieldByName(cs.FldName).Len()
		if ln != fln {
			errTxt = append(errTxt, fmt.Sprintf("%s has len %d expected %d", cs.FldName, fln, ln))
		}
	}

	if len(errTxt) != 0 {
		err = fmt.Errorf("validation failed: %s", strings.Join(errTxt, ","))
	}

	return
}

// Chunk gets a range of values for the given column name.
func (specs ColSpecs) Chunk(colName string, obj any, bgn, end int) (vals any) {

	fieldName := ""
	for _, cs := range specs {
		if cs.ColName != colName {
			continue // Todo: lookup! loop here is blah
		}
		fieldName = cs.FldName
	}

	voe := reflect.ValueOf(obj).Elem()
	return voe.FieldByName(fieldName).Slice(bgn, end).Interface()
}

// Append adds values to obj's field corresponding to the given column name.
func (specs ColSpecs) Append(colName string, vals any, obj any) (err error) {

	// Todo: wring hands about vals type

	ve := reflect.ValueOf(obj).Elem()

	for _, cs := range specs {
		if cs.ColName != colName {
			continue // Todo: lookup! loop here is blah
		}

		field := ve.FieldByName(cs.FldName)

		// getting real trouble wo this check, maybe even a bug in reflect
		vov := reflect.ValueOf(vals)
		if vov.Kind() != reflect.Slice {
			err = fmt.Errorf("cannot append non-slice: %#v", vals)
			return
		}

		field.Set(reflect.AppendSlice(field, vov))
	}

	return
}
