// Package colspec reflects on column-oriented structures.
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

// ColSpec maps column names to a struct's field names of type slice.
// Package path and type name is checked against ojb's passed in for some measure of safety.
type ColSpec struct {
	PkgPath  string
	TypeName string
	ColToFld map[string]string
}

// New creates ColSpecs by finding slice fields with a "col" tag.
func New(obj any) (specs ColSpec, err error) {

	stc, stcType, err := structType(obj)
	if err != nil {
		return
	}

	specs = ColSpec{
		TypeName: stcType.Name(),
		PkgPath:  stcType.PkgPath(),
		ColToFld: map[string]string{},
	}

	for i := 0; i < stc.NumField(); i++ {

		// only first "layer" of fields are considered

		field := stc.Field(i)
		structField := stcType.Field(i)
		colName := structField.Tag.Get("col")

		if !field.CanSet() || colName == "" {
			continue
		}

		if field.Kind() != reflect.Slice {
			err = fmt.Errorf("field: %s is not slice", structField.Name)
			return
		}

		specs.ColToFld[colName] = structField.Name
	}

	return
}

// ValLens checks that all "col" fields are of a given length.
func (specs ColSpec) ValLens(length int, obj any) (err error) {

	stc, err := specs.checkType(obj)
	if err != nil {
		return
	}

	errTxt := []string{}
	for _, fld := range specs.ColToFld {

		flen := stc.FieldByName(fld).Len()
		if length != flen {
			errTxt = append(errTxt, fmt.Sprintf("%s has len %d, expected %d", fld, flen, length))
		}
	}

	if len(errTxt) != 0 {
		err = fmt.Errorf("validation failed: %s", strings.Join(errTxt, ","))
	}

	return
}

// Chunk gets a range of values for the given column name.
func (specs ColSpec) Chunk(col string, obj any, bgn, end int) any {

	fld, ok := specs.ColToFld[col]
	if !ok {
		return nil
	}

	stc, err := specs.checkType(obj)
	if err != nil {
		return nil
	}

	return stc.FieldByName(fld).Slice(bgn, end).Interface()
}

// Append adds values to obj's field corresponding to the given column name.
func (specs ColSpec) Append(col string, vals any, obj any) (err error) {

	stc, err := specs.checkType(obj)
	if err != nil {
		return
	}

	fld, ok := specs.ColToFld[col]
	if !ok {
		return fmt.Errorf("unkown column name: %s", col)
	}
	field := stc.FieldByName(fld)

	vov := reflect.ValueOf(vals)

	// getting real trouble wo this check, even when is slice (??)
	// Todo: check type in vals against specs
	if vov.Kind() != reflect.Slice {
		err = fmt.Errorf("cannot append non-slice: %#v", vals)
		return
	}

	field.Set(reflect.AppendSlice(field, vov))
	return
}

// unexported

func structType(i any) (stc reflect.Value, stcType reflect.Type, err error) {

	ptr := reflect.ValueOf(i)
	if ptr.Kind() != reflect.Ptr {
		err = fmt.Errorf("pointer required, got: %#v", i)
		return
	}

	stc = ptr.Elem()
	if stc.Kind() != reflect.Struct {
		err = fmt.Errorf("structure required, got: %#v", i)
		return
	}

	stcType = stc.Type()
	return
}

// func (specs ColSpecs) checkType(stcType reflect.Type) (err error) {
func (specs ColSpec) checkType(i any) (stc reflect.Value, err error) {

	var stcType reflect.Type
	stc, stcType, err = structType(i)
	if err != nil {
		return
	}

	if specs.TypeName != stcType.Name() || specs.PkgPath != stcType.PkgPath() {
		err = fmt.Errorf("type does not match, expected %s %s, got: %s %s",
			specs.TypeName, specs.PkgPath, stcType.Name(), stcType.PkgPath())
	}
	return
}
