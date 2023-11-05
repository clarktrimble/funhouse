package colspec

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var (
	ErrInvalidSpec = errors.New(`spec must be a struct pointer with "col" slice fields`)
)

// ColSpecs tracks column fields in a structure.
type ColSpecs map[string]string

// New create ColSpecs for a given object.
// Slice fields with a "col" tag are picked up.
func New(obj any) (specs ColSpecs, err error) {

	specs = ColSpecs{}

	s := reflect.ValueOf(obj)
	if s.Kind() != reflect.Ptr {
		err = ErrInvalidSpec
		return
	}
	s = s.Elem()
	if s.Kind() != reflect.Struct {
		err = ErrInvalidSpec
		return
	}
	typeOfSpec := s.Type()

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		ftype := typeOfSpec.Field(i)
		if !f.CanSet() || ftype.Tag.Get("col") == "" {
			continue
		}

		specs[ftype.Tag.Get("col")] = ftype.Name
	}

	return
}

// ValidateCols checks that all "col" fields are of a given length.
func (specs ColSpecs) ValidateCols(glen int, obj any) (err error) {

	// Todo: check for obj suitable and that fields are slice
	//       maybe store name of struct from harvesting of fields?

	errTxt := []string{}
	ve := reflect.ValueOf(obj).Elem()

	for _, fldName := range specs {

		flen := ve.FieldByName(fldName).Len()
		if glen != flen {
			errTxt = append(errTxt, fmt.Sprintf("%s has len %d expected %d", fldName, flen, glen))
		}
	}

	if len(errTxt) != 0 {
		err = fmt.Errorf("validation failed: %s", strings.Join(errTxt, ","))
	}

	return
}

// Chunk gets a range of values for the given column name.
func (specs ColSpecs) Chunk(colName string, obj any, bgn, end int) (vals any) {

	fldName, ok := specs[colName]
	if !ok {
		return
	}

	voe := reflect.ValueOf(obj).Elem()
	return voe.FieldByName(fldName).Slice(bgn, end).Interface()
}

// Append adds values to obj's field corresponding to the given column name.
func (specs ColSpecs) Append(colName string, vals any, obj any) (err error) {

	// Todo: wring hands about vals type

	ve := reflect.ValueOf(obj).Elem()

	fldName, ok := specs[colName]
	if !ok {
		return fmt.Errorf("unkown column name: %s", colName)
	}
	field := ve.FieldByName(fldName)

	// getting real trouble wo this check, maybe even a bug in reflect
	vov := reflect.ValueOf(vals)
	if vov.Kind() != reflect.Slice {
		err = fmt.Errorf("cannot append non-slice: %#v", vals)
		return
	}

	field.Set(reflect.AppendSlice(field, vov))
	return
}
