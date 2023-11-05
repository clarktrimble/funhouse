package entity

import (
	"fmt"
	"math/rand"
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

/*
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

// switch name {
// case "ts":
// tt, ok = vals.([]time.Time)
// mcs.Timestamps = append(mcs.Timestamps, tt...)
// case "severity_text":
// ts, ok = vals.([]string)
// mcs.SeverityTxts = append(mcs.SeverityTxts, ts...)
//func (mcs *MsgCols) Append(name string, vals any) (err error) {

// func (mcs *MsgCols) Chunk(name string, bgn, end int) (vals any) {
func (specs ColSpecs) ChunkToo(fieldName string, obj any, bgn, end int) (vals any) {

	for _, cs := range specs {
		if cs.Name != fieldName {
			continue // Todo: lookup!
		}

		elem := reflect.ValueOf(obj).Elem()
		val := elem.FieldByName("SeverityTxts")

		fmt.Printf(">>> elem: %#v\n", elem)
		fmt.Printf(">>> val: %#v\n", val)
		fmt.Printf(">>> len: %#v\n", val.Len())

		ve := reflect.ValueOf(obj).Elem()
		fd := ve.FieldByName(fieldName)
		fmt.Printf(">>> typeee: %#v\n", reflect.ValueOf(fd))
		fmt.Printf(">>> typeee: %#v\n", fd.Type().String())
		switch fd.Type().String() {
		case "[]int":
			fmt.Println("int here")
		case "[]string":
			fmt.Println("string here")
		}

		//fmt.Printf(">>> typeee: %#v\n", reflect.TypeOf(obj).Elem())
		//return ve.FieldByName(fieldName)
		return ve.FieldByName(fieldName).Slice(bgn, end).Interface()
	}

	return
}

func (specs ColSpecs) AppendToo(colName string, vals any, obj any) (err error) {

	// Todo: wring hands about vals type

	ve := reflect.ValueOf(obj).Elem()

	for _, cs := range specs {
		if cs.Tag != colName {
			continue // Todo: lookup!
		}

		field := ve.FieldByName(cs.Name)
		//fmt.Printf(">>> field: %#v\n", field)

		// Todo: fo loop blah
		vov := reflect.ValueOf(vals)
		//fmt.Sprintf(">>> vvo: %#v\n", reflect.ValueOf(vov))
		//fmt.Printf(">>>vovk: %#v\n", vov.Kind() == reflect.Slice)
		//fmt.Printf(">>> field: %#v\n", field)

		if vov.Kind() != reflect.Slice {
			err = fmt.Errorf("cannot append non-slice: %#v", vals)
			return
		}

		//value.Set(reflect.Append(value, reflect.ValueOf(55)))
		//field.Set(reflect.AppendSlice(field, reflect.ValueOf([]string{"DEBARG", "INFRA"})))
		//field.Set(reflect.AppendSlice(field, reflect.ValueOf(vals)))
		field.Set(reflect.AppendSlice(field, vov))

		//fmt.Printf(">>> field: %#v\n", field)
		//fmt.Printf(">>> obj: %#v\n", obj)
	}

	return
}

func (specs ColSpecs) ValidateToo(ln int, obj any) (err error) {

	// Todo: check for obj suitable and that fields are slice
	//       maybe store name of struct from harvesting of fields?

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

func ReflectoToo(spec any) (err error) {

	s := reflect.ValueOf(spec)

	if s.Kind() != reflect.Ptr {
		return ErrInvalidSpecification
	}
	s = s.Elem()
	if s.Kind() != reflect.Struct {
		return ErrInvalidSpecification
	}
	typeOfSpec := s.Type()

	//fmt.Printf(">>> tos: %#v\n", typeOfSpec)

	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)
		//val? fmt.Printf(">>> field: %#v\n", f)
		ftype := typeOfSpec.Field(i)
		//if !f.CanSet() || isTrue(ftype.Tag.Get("ignored")) {
		if !f.CanSet() || ftype.Tag.Get("col") == "" {
			continue
		}

		val := s.FieldByName(ftype.Name)
		fmt.Printf(">>> name: %s tag: %s len: %d\n", ftype.Name, ftype.Tag.Get("col"), val.Len())

		//elem := reflect.ValueOf(msgs).Elem()
		//val := elem.FieldByName(cf.Name)
		//fmt.Printf(">>> len: %#v\n", val.Len())

	}

	return
}
func Reflecto(msgs *MsgCols) (err error) {
	//func Reflecto(msgs any) (err error) {

	// reflect.ValueOf(&n).Elem().FieldByName("N").SetInt(7)

	//stuff := reflect.ValueOf(msgs).Elem().FieldByName("Timestamps").Len()
	//fmt.Printf(">>> stuff: %#v\n\n", stuff)

	// Todo: wring hands about pointer
	tp := reflect.TypeOf(*msgs)

	//fmt.Printf(">>> %#v\n", tp)

	cfs := []reflect.StructField{}
	for i := 0; i < tp.NumField(); i++ {

		cf := tp.Field(i)
		_, ok := cf.Tag.Lookup("col")
		if ok {
			// Todo: save tag val, map or struct
			// Todo: check tag is not blank
			cfs = append(cfs, cf)
		}
	}

	for _, cf := range cfs {
		fmt.Printf(">>> \n\n cf: %#v\n", cf)
		//fmt.Printf(">>> vo cf: %#v\n", reflect.ValueOf(cf))
		fmt.Printf(">>> name: %s tag: %s\n", cf.Name, cf.Tag.Get("col"))

		elem := reflect.ValueOf(msgs).Elem()
		val := elem.FieldByName(cf.Name)

		fmt.Printf(">>> elem: %#v\n", elem)
		fmt.Printf(">>> val: %#v\n", val)
		fmt.Printf(">>> len: %#v\n", val.Len())

		//gVal := reflect.ValueOf(cf)
		// not a pointer so all we can do is read it
		//fmt.Println(gVal.Interface())
	}

	return
}
*/

func NewMsgCols(size int) *MsgCols {

	return &MsgCols{
		Length:       size,
		Timestamps:   make([]time.Time, size),
		SeverityTxts: make([]string, size),
		SeverityNums: make([]uint8, size),
		Names:        make([]string, size),
		Bodies:       make([]string, size),
		Tagses:       make([][]string, size),
	}
}

/*
func (mcs *MsgCols) Validate() (err error) {

	ts := len(mcs.Timestamps)
	st := len(mcs.SeverityTxts)
	sn := len(mcs.SeverityNums)
	nm := len(mcs.Names)
	bd := len(mcs.Bodies)
	tg := len(mcs.Tagses)

	if mcs.Length != ts ||
		mcs.Length != st ||
		mcs.Length != sn ||
		mcs.Length != nm ||
		mcs.Length != bd ||
		mcs.Length != tg {
		err = fmt.Errorf(
			"invalid Length:%d ts:%d st:%d sn:%d nm:%d bd:%d tg:%d",
			mcs.Length, ts, st, sn, nm, bd, tg,
		)
	}
	return
}

func (mcs *MsgCols) Chunk(name string, bgn, end int) (vals any) {

	switch name {
	case "ts":
		vals = mcs.Timestamps[bgn:end]
	case "severity_text":
		vals = mcs.SeverityTxts[bgn:end]
	case "severity_number":
		vals = mcs.SeverityNums[bgn:end]
	case "name":
		vals = mcs.Names[bgn:end]
	case "body":
		vals = mcs.Bodies[bgn:end]
	case "arr":
		vals = mcs.Tagses[bgn:end]
	default:
		vals = nil // noop but want to be obvious
	}

	return
}

func (mcs *MsgCols) Append(name string, vals any) (err error) {

	var ok bool
	var tt []time.Time
	var ts []string
	var tu []uint8
	var tz [][]string

	// Todo: use struct tags??

	switch name {
	case "ts":
		tt, ok = vals.([]time.Time)
		mcs.Timestamps = append(mcs.Timestamps, tt...)
	case "severity_text":
		ts, ok = vals.([]string)
		mcs.SeverityTxts = append(mcs.SeverityTxts, ts...)
	case "severity_number":
		tu, ok = vals.([]uint8)
		mcs.SeverityNums = append(mcs.SeverityNums, tu...)
	case "name":
		ts, ok = vals.([]string)
		mcs.Names = append(mcs.Names, ts...)
	case "body":
		ts, ok = vals.([]string)
		mcs.Bodies = append(mcs.Bodies, ts...)
	case "arr":
		tz, ok = vals.([][]string)
		mcs.Tagses = append(mcs.Tagses, tz...)
	}
	if !ok {
		err = fmt.Errorf("append assertion failed for %s with vals %#v", name, vals)
	}

	return
}
*/

func (mcs *MsgCols) Len() int {

	return mcs.Length
}

func (mcs *MsgCols) AddLen(size int) {

	mcs.Length += size
}

func (mcs *MsgCols) Row(idx int) Msg {

	return Msg{
		Timestamp: mcs.Timestamps[idx],
		Severity: Severity{
			Txt: mcs.SeverityTxts[idx],
			Num: mcs.SeverityNums[idx],
		},
		Name: mcs.Names[idx],
		Body: mcs.Bodies[idx],
		Tags: mcs.Tagses[idx],
	}
}

func SampleMsgCols(count int) (mcs *MsgCols) {

	mcs = NewMsgCols(count)

	then := time.Now().Add(-time.Hour)
	offset := 0

	for i := 0; i < count; i++ {
		mcs.Timestamps[i] = then.Add(time.Duration(offset) * time.Nanosecond)
		mcs.SeverityTxts[i] = "INFO"
		mcs.SeverityNums[i] = 3
		mcs.Names[i] = fmt.Sprintf("name-%d", i)
		mcs.Bodies[i] = "body from colzz"
		mcs.Tagses[i] = []string{"sna", "foo"}

		offset += rand.Intn(33)
	}

	return
}
