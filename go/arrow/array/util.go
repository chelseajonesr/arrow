// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package array

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/bitutil"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/apache/arrow/go/v16/internal/hashing"
	"github.com/apache/arrow/go/v16/internal/json"
)

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type fromJSONCfg struct {
	multiDocument bool
	startOffset   int64
	useNumber     bool
}

type FromJSONOption func(*fromJSONCfg)

func WithMultipleDocs() FromJSONOption {
	return func(c *fromJSONCfg) {
		c.multiDocument = true
	}
}

// WithStartOffset attempts to start decoding from the reader at the offset
// passed in. If using this option the reader must fulfill the io.ReadSeeker
// interface, or else an error will be returned.
//
// It will call Seek(off, io.SeekStart) on the reader
func WithStartOffset(off int64) FromJSONOption {
	return func(c *fromJSONCfg) {
		c.startOffset = off
	}
}

// WithUseNumber enables the 'UseNumber' option on the json decoder, using
// the json.Number type instead of assuming float64 for numbers. This is critical
// if you have numbers that are larger than what can fit into the 53 bits of
// an IEEE float64 mantissa and want to preserve its value.
func WithUseNumber() FromJSONOption {
	return func(c *fromJSONCfg) {
		c.useNumber = true
	}
}

// FromJSON creates an arrow.Array from a corresponding JSON stream and defined data type. If the types in the
// json do not match the type provided, it will return errors. This is *not* the integration test format
// and should not be used as such. This intended to be used by consumers more similarly to the current exposing of
// the csv reader/writer. It also returns the input offset in the reader where it finished decoding since buffering
// by the decoder could leave the reader's cursor past where the parsing finished if attempting to parse multiple json
// arrays from one stream.
//
// All the Array types implement json.Marshaller and thus can be written to json
// using the json.Marshal function
//
// The JSON provided must be formatted in one of two ways:
//
//	Default: the top level of the json must be a list which matches the type specified exactly
//	Example: `[1, 2, 3, 4, 5]` for any integer type or `[[...], null, [], .....]` for a List type
//				Struct arrays are represented a list of objects: `[{"foo": 1, "bar": "moo"}, {"foo": 5, "bar": "baz"}]`
//
//	Using WithMultipleDocs:
//		If the JSON provided is multiple newline separated json documents, then use this option
//		and each json document will be treated as a single row of the array. This is most useful for record batches
//		and interacting with other processes that use json. For example:
//			`{"col1": 1, "col2": "row1", "col3": ...}\n{"col1": 2, "col2": "row2", "col3": ...}\n.....`
//
// Duration values get formated upon marshalling as a string consisting of their numeric
// value followed by the unit suffix such as "10s" for a value of 10 and unit of Seconds.
// with "ms" for millisecond, "us" for microsecond, and "ns" for nanosecond as the suffixes.
// Unmarshalling duration values is more permissive since it first tries to use Go's
// time.ParseDuration function which means it allows values in the form 3h25m0.3s in addition
// to the same values which are output.
//
// Interval types are marshalled / unmarshalled as follows:
//
//	 MonthInterval is marshalled as an object with the format:
//		 { "months": #}
//	 DayTimeInterval is marshalled using Go's regular marshalling of structs:
//		 { "days": #, "milliseconds": # }
//	 MonthDayNanoInterval values are marshalled the same as DayTime using Go's struct marshalling:
//	  { "months": #, "days": #, "nanoseconds": # }
//
// Times use a format of HH:MM or HH:MM:SS[.zzz] where the fractions of a second cannot
// exceed the precision allowed by the time unit, otherwise unmarshalling will error.
//
// # Dates use YYYY-MM-DD format
//
// Timestamps use RFC3339Nano format except without a timezone, all of the following are valid:
//
//		YYYY-MM-DD
//		YYYY-MM-DD[T]HH
//		YYYY-MM-DD[T]HH:MM
//	 YYYY-MM-DD[T]HH:MM:SS[.zzzzzzzzzz]
//
// The fractions of a second cannot exceed the precision allowed by the timeunit of the datatype.
//
// When processing structs as objects order of keys does not matter, but keys cannot be repeated.
func FromJSON(mem memory.Allocator, dt arrow.DataType, r io.Reader, opts ...FromJSONOption) (arr arrow.Array, offset int64, err error) {
	var cfg fromJSONCfg
	for _, o := range opts {
		o(&cfg)
	}

	if cfg.startOffset != 0 {
		seeker, ok := r.(io.ReadSeeker)
		if !ok {
			return nil, 0, errors.New("using StartOffset option requires reader to be a ReadSeeker, cannot seek")
		}

		seeker.Seek(cfg.startOffset, io.SeekStart)
	}

	bldr := NewBuilder(mem, dt)
	defer bldr.Release()

	dec := json.NewDecoder(r)
	defer func() {
		if errors.Is(err, io.EOF) {
			err = fmt.Errorf("failed parsing json: %w", io.ErrUnexpectedEOF)
		}
	}()

	if cfg.useNumber {
		dec.UseNumber()
	}

	if !cfg.multiDocument {
		t, err := dec.Token()
		if err != nil {
			return nil, dec.InputOffset(), err
		}

		if delim, ok := t.(json.Delim); !ok || delim != '[' {
			return nil, dec.InputOffset(), fmt.Errorf("json doc must be an array, found %s", delim)
		}
	}

	if err = bldr.Unmarshal(dec); err != nil {
		return nil, dec.InputOffset(), err
	}

	if !cfg.multiDocument {
		// consume the last ']'
		if _, err = dec.Token(); err != nil {
			return nil, dec.InputOffset(), err
		}
	}

	return bldr.NewArray(), dec.InputOffset(), nil
}

// RecordToStructArray constructs a struct array from the columns of the record batch
// by referencing them, zero-copy.
func RecordToStructArray(rec arrow.Record) *Struct {
	cols := make([]arrow.ArrayData, rec.NumCols())
	for i, c := range rec.Columns() {
		cols[i] = c.Data()
	}

	data := NewData(arrow.StructOf(rec.Schema().Fields()...), int(rec.NumRows()), []*memory.Buffer{nil}, cols, 0, 0)
	defer data.Release()

	return NewStructData(data)
}

// RecordFromStructArray is a convenience function for converting a struct array into
// a record batch without copying the data. If the passed in schema is nil, the fields
// of the struct will be used to define the record batch. Otherwise the passed in
// schema will be used to create the record batch. If passed in, the schema must match
// the fields of the struct column.
func RecordFromStructArray(in *Struct, schema *arrow.Schema) arrow.Record {
	if schema == nil {
		schema = arrow.NewSchema(in.DataType().(*arrow.StructType).Fields(), nil)
	}

	return NewRecord(schema, in.fields, int64(in.Len()))
}

// RecordFromJSON creates a record batch from JSON data. See array.FromJSON for the details
// of formatting and logic.
//
// A record batch from JSON is equivalent to reading a struct array in from json and then
// converting it to a record batch.
func RecordFromJSON(mem memory.Allocator, schema *arrow.Schema, r io.Reader, opts ...FromJSONOption) (arrow.Record, int64, error) {
	st := arrow.StructOf(schema.Fields()...)
	arr, off, err := FromJSON(mem, st, r, opts...)
	if err != nil {
		return nil, off, err
	}
	defer arr.Release()

	return RecordFromStructArray(arr.(*Struct), schema), off, nil
}

// RecordToJSON writes out the given record following the format of each row is a single object
// on a single line of the output.
func RecordToJSON(rec arrow.Record, w io.Writer) error {
	enc := json.NewEncoder(w)

	fields := rec.Schema().Fields()

	cols := make(map[string]interface{})
	for i := 0; int64(i) < rec.NumRows(); i++ {
		for j, c := range rec.Columns() {
			cols[fields[j].Name] = c.GetOneForMarshal(i)
		}
		if err := enc.Encode(cols); err != nil {
			return err
		}
	}
	return nil
}

func TableFromJSON(mem memory.Allocator, sc *arrow.Schema, recJSON []string, opt ...FromJSONOption) (arrow.Table, error) {
	batches := make([]arrow.Record, len(recJSON))
	for i, batchJSON := range recJSON {
		batch, _, err := RecordFromJSON(mem, sc, strings.NewReader(batchJSON), opt...)
		if err != nil {
			return nil, err
		}
		defer batch.Release()
		batches[i] = batch
	}
	return NewTableFromRecords(sc, batches), nil
}

func GetDictArrayData(mem memory.Allocator, valueType arrow.DataType, memoTable hashing.MemoTable, startOffset int) (*Data, error) {
	dictLen := memoTable.Size() - startOffset
	buffers := []*memory.Buffer{nil, nil}

	buffers[1] = memory.NewResizableBuffer(mem)
	defer buffers[1].Release()

	switch tbl := memoTable.(type) {
	case hashing.NumericMemoTable:
		nbytes := tbl.TypeTraits().BytesRequired(dictLen)
		buffers[1].Resize(nbytes)
		tbl.WriteOutSubset(startOffset, buffers[1].Bytes())
	case *hashing.BinaryMemoTable:
		switch valueType.ID() {
		case arrow.BINARY, arrow.STRING:
			buffers = append(buffers, memory.NewResizableBuffer(mem))
			defer buffers[2].Release()

			buffers[1].Resize(arrow.Int32Traits.BytesRequired(dictLen + 1))
			offsets := arrow.Int32Traits.CastFromBytes(buffers[1].Bytes())
			tbl.CopyOffsetsSubset(startOffset, offsets)

			valuesz := offsets[len(offsets)-1] - offsets[0]
			buffers[2].Resize(int(valuesz))
			tbl.CopyValuesSubset(startOffset, buffers[2].Bytes())
		case arrow.LARGE_BINARY, arrow.LARGE_STRING:
			buffers = append(buffers, memory.NewResizableBuffer(mem))
			defer buffers[2].Release()

			buffers[1].Resize(arrow.Int64Traits.BytesRequired(dictLen + 1))
			offsets := arrow.Int64Traits.CastFromBytes(buffers[1].Bytes())
			tbl.CopyLargeOffsetsSubset(startOffset, offsets)

			valuesz := offsets[len(offsets)-1] - offsets[0]
			buffers[2].Resize(int(valuesz))
			tbl.CopyValuesSubset(startOffset, buffers[2].Bytes())
		default: // fixed size
			bw := int(bitutil.BytesForBits(int64(valueType.(arrow.FixedWidthDataType).BitWidth())))
			buffers[1].Resize(dictLen * bw)
			tbl.CopyFixedWidthValues(startOffset, bw, buffers[1].Bytes())
		}
	default:
		return nil, fmt.Errorf("arrow/array: dictionary unifier unimplemented type: %s", valueType)
	}

	var nullcount int
	if idx, ok := memoTable.GetNull(); ok && idx >= startOffset {
		buffers[0] = memory.NewResizableBuffer(mem)
		defer buffers[0].Release()
		nullcount = 1
		buffers[0].Resize(int(bitutil.BytesForBits(int64(dictLen))))
		memory.Set(buffers[0].Bytes(), 0xFF)
		bitutil.ClearBit(buffers[0].Bytes(), idx)
	}

	return NewData(valueType, dictLen, buffers, nil, nullcount, 0), nil
}

func DictArrayFromJSON(mem memory.Allocator, dt *arrow.DictionaryType, indicesJSON, dictJSON string) (arrow.Array, error) {
	indices, _, err := FromJSON(mem, dt.IndexType, strings.NewReader(indicesJSON))
	if err != nil {
		return nil, err
	}
	defer indices.Release()

	dict, _, err := FromJSON(mem, dt.ValueType, strings.NewReader(dictJSON))
	if err != nil {
		return nil, err
	}
	defer dict.Release()

	return NewDictionaryArray(dt, indices, dict), nil
}

func ChunkedFromJSON(mem memory.Allocator, dt arrow.DataType, chunkStrs []string, opts ...FromJSONOption) (*arrow.Chunked, error) {
	chunks := make([]arrow.Array, len(chunkStrs))
	defer func() {
		for _, c := range chunks {
			if c != nil {
				c.Release()
			}
		}
	}()

	var err error
	for i, c := range chunkStrs {
		chunks[i], _, err = FromJSON(mem, dt, strings.NewReader(c), opts...)
		if err != nil {
			return nil, err
		}
	}

	return arrow.NewChunked(dt, chunks), nil
}

func getMaxBufferLen(dt arrow.DataType, length int) int {
	bufferLen := int(bitutil.BytesForBits(int64(length)))

	maxOf := func(bl int) int {
		if bl > bufferLen {
			return bl
		}
		return bufferLen
	}

	switch dt := dt.(type) {
	case *arrow.DictionaryType:
		bufferLen = maxOf(getMaxBufferLen(dt.ValueType, length))
		return maxOf(getMaxBufferLen(dt.IndexType, length))
	case *arrow.FixedSizeBinaryType:
		return maxOf(dt.ByteWidth * length)
	case arrow.FixedWidthDataType:
		return maxOf(int(bitutil.BytesForBits(int64(dt.BitWidth()))) * length)
	case *arrow.StructType:
		for _, f := range dt.Fields() {
			bufferLen = maxOf(getMaxBufferLen(f.Type, length))
		}
		return bufferLen
	case *arrow.SparseUnionType:
		// type codes
		bufferLen = maxOf(length)
		// creates children of the same length of the union
		for _, f := range dt.Fields() {
			bufferLen = maxOf(getMaxBufferLen(f.Type, length))
		}
		return bufferLen
	case *arrow.DenseUnionType:
		// type codes
		bufferLen = maxOf(length)
		// offsets
		bufferLen = maxOf(arrow.Int32SizeBytes * length)
		// create children of length 1
		for _, f := range dt.Fields() {
			bufferLen = maxOf(getMaxBufferLen(f.Type, 1))
		}
		return bufferLen
	case arrow.OffsetsDataType:
		return maxOf(dt.OffsetTypeTraits().BytesRequired(length + 1))
	case *arrow.FixedSizeListType:
		return maxOf(getMaxBufferLen(dt.Elem(), int(dt.Len())*length))
	case arrow.ExtensionType:
		return maxOf(getMaxBufferLen(dt.StorageType(), length))
	default:
		panic(fmt.Errorf("arrow/array: arrayofnull not implemented for type %s", dt))
	}
}

type nullArrayFactory struct {
	mem memory.Allocator
	dt  arrow.DataType
	len int
	buf *memory.Buffer
}

func (n *nullArrayFactory) create() *Data {
	if n.buf == nil {
		bufLen := getMaxBufferLen(n.dt, n.len)
		n.buf = memory.NewResizableBuffer(n.mem)
		n.buf.Resize(bufLen)
		defer n.buf.Release()
	}

	var (
		dt        = n.dt
		bufs      = []*memory.Buffer{memory.SliceBuffer(n.buf, 0, int(bitutil.BytesForBits(int64(n.len))))}
		childData []arrow.ArrayData
		dictData  arrow.ArrayData
	)
	defer bufs[0].Release()

	if ex, ok := dt.(arrow.ExtensionType); ok {
		dt = ex.StorageType()
	}

	if nf, ok := dt.(arrow.NestedType); ok {
		childData = make([]arrow.ArrayData, nf.NumFields())
	}

	switch dt := dt.(type) {
	case *arrow.NullType:
	case *arrow.DictionaryType:
		bufs = append(bufs, n.buf)
		arr := MakeArrayOfNull(n.mem, dt.ValueType, 0)
		defer arr.Release()
		dictData = arr.Data()
	case arrow.FixedWidthDataType:
		bufs = append(bufs, n.buf)
	case arrow.BinaryDataType:
		bufs = append(bufs, n.buf, n.buf)
	case arrow.OffsetsDataType:
		bufs = append(bufs, n.buf)
		childData[0] = n.createChild(dt, 0, 0)
		defer childData[0].Release()
	case *arrow.FixedSizeListType:
		childData[0] = n.createChild(dt, 0, n.len*int(dt.Len()))
		defer childData[0].Release()
	case *arrow.StructType:
		for i := range dt.Fields() {
			childData[i] = n.createChild(dt, i, n.len)
			defer childData[i].Release()
		}
	case *arrow.RunEndEncodedType:
		bldr := NewBuilder(n.mem, dt.RunEnds())
		defer bldr.Release()

		switch b := bldr.(type) {
		case *Int16Builder:
			b.Append(int16(n.len))
		case *Int32Builder:
			b.Append(int32(n.len))
		case *Int64Builder:
			b.Append(int64(n.len))
		}

		childData[0] = bldr.newData()
		defer childData[0].Release()
		childData[1] = n.createChild(dt.Encoded(), 1, 1)
		defer childData[1].Release()
	case arrow.UnionType:
		bufs[0].Release()
		bufs[0] = nil
		bufs = append(bufs, n.buf)
		// buffer is zeroed, but 0 may not be a valid type code
		if dt.TypeCodes()[0] != 0 {
			bufs[1] = memory.NewResizableBuffer(n.mem)
			bufs[1].Resize(n.len)
			defer bufs[1].Release()
			memory.Set(bufs[1].Bytes(), byte(dt.TypeCodes()[0]))
		}

		// for sparse unions we create children with the same length
		childLen := n.len
		if dt.Mode() == arrow.DenseMode {
			// for dense unions, offsets are all 0 and make children
			// with length 1
			bufs = append(bufs, n.buf)
			childLen = 1
		}
		for i := range dt.Fields() {
			childData[i] = n.createChild(dt, i, childLen)
			defer childData[i].Release()
		}
	}

	out := NewData(n.dt, n.len, bufs, childData, n.len, 0)
	if dictData != nil {
		out.SetDictionary(dictData)
	}
	return out
}

func (n *nullArrayFactory) createChild(dt arrow.DataType, i, length int) *Data {
	childFactory := &nullArrayFactory{
		mem: n.mem, dt: n.dt.(arrow.NestedType).Fields()[i].Type,
		len: length, buf: n.buf}
	return childFactory.create()
}

// MakeArrayOfNull creates an array of size length which is all null of the given data type.
func MakeArrayOfNull(mem memory.Allocator, dt arrow.DataType, length int) arrow.Array {
	if dt.ID() == arrow.NULL {
		return NewNull(length)
	}

	data := (&nullArrayFactory{mem: mem, dt: dt, len: length}).create()
	defer data.Release()
	return MakeFromData(data)
}

// ReflectMappingFromStruct generates a simple mapping of struct field indices between go structs and arrow structs,
// skipping any fields with tag `parquet:"-"`
// If toArrow is true, this can be used with builder AppendReflectValue() to build an Arrow array from Go structs,
// assuming the builder's schema was generated with schema.NewSchemaFromStruct() and pqarrow.FromParquet()
// Or if toArrow is false, this can be used with array SetReflectValue() to build a Go struct from an Arrow array.
// This function does not handle more complex cases, such as the Arrow and Go structs having fields in
// different orders, or Arrow containing fields that are not present in the Go struct.
func ReflectMappingFromStruct[T any](toArrow bool) arrow.ReflectMapping {
	var v [0]T
	t := reflect.TypeOf(v).Elem()
	mapping := make(map[int]arrow.ReflectMapping)

	// Recursively add fields to mapping
	addFieldToMapping(t, mapping, toArrow)

	return arrow.ReflectMapping{NestedMappingsBySourceIndex: mapping}
}

// example, go -> arrow {a (field 0), b (field 1), c (parquet:"-") (field 2), d (field 3)}
// to
// {0, map { 0: {0, nil}, 1: {1, nil}, 3: {2, nil}}}
// to go the other way we would want...
// {0, map { 0: {0, nil}, 1: {1, nil}, 2: {3, nil}}}

func addFieldToMapping(t reflect.Type, mapping map[int]arrow.ReflectMapping, toArrow bool) {
	for t.Kind() == reflect.Pointer || t.Kind() == reflect.Array || t.Kind() == reflect.Slice || t.Kind() == reflect.Map {
		t = t.Elem()
	}
	if t.Kind() == reflect.Struct {
		arrowIndex := 0
		for goIndex := 0; goIndex < t.NumField(); goIndex++ {
			exclude := false
			field := t.Field(goIndex)
			// Look for a parquet tag for this field
			tag := field.Tag
			if ptags, ok := tag.Lookup("parquet"); ok {
				if ptags == "-" {
					exclude = true
				}
			}
			if !exclude {
				var (
					destinationIndex int
					sourceIndex      int
				)
				if toArrow {
					destinationIndex = arrowIndex
					sourceIndex = goIndex
				} else {
					destinationIndex = goIndex
					sourceIndex = arrowIndex
				}
				mapping[sourceIndex] = arrow.ReflectMapping{DestinationIndex: destinationIndex, NestedMappingsBySourceIndex: make(map[int]arrow.ReflectMapping)}
				addFieldToMapping(field.Type, mapping[sourceIndex].NestedMappingsBySourceIndex, toArrow)
				arrowIndex++
			}
		}
	}
}

func instantiateReflectValuePointer(v reflect.Value) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.CanSet() {
			pointedAtType := v.Type().Elem()
			switch pointedAtType.Kind() {
			case reflect.Map, reflect.Slice:
				// TODO should array go here also?
				containerPtr := reflect.New(pointedAtType)
				containerPtr.Elem().Set(newReflectValueByType(pointedAtType))
				v.Set(containerPtr)
			default:
				v.Set(newReflectValueByType(pointedAtType))
			}
		}
		v = v.Elem()
	}
	return v
}

func instantiateReflectValuePointers(v reflect.Value) reflect.Value {
	// First navigate through pointers
	for v.Kind() == reflect.Pointer {
		v = instantiateReflectValuePointer(v)
	}

	// If there was a pointer to a map or slice, it will have been set above
	// but if the map or slice was passed in directly it needs to get set here
	switch v.Kind() {
	case reflect.Map, reflect.Slice:
		// TODO array here?
		if v.IsNil() {
			v.Set(newReflectValueByType(v.Type()))
		}
	}
	return v
}

func traverseAndInstantiateIndirections(goValue reflect.Value) reflect.Value {
	elem := goValue
	for elem.Type().Kind() == reflect.Pointer {
		if !elem.CanSet() {
			elem = elem.Elem()
			continue
		}
		elemType := elem.Type().Elem()
		switch elemType.Kind() {
		case reflect.Map:
			newMap := reflect.MakeMap(elemType)
			newMapPtr := reflect.New(elemType)
			newMapPtr.Elem().Set(newMap)
			elem.Set(newMapPtr)
		case reflect.Slice:
			newSlice := reflect.MakeSlice(elemType, 0, 10)
			newSlicePtr := reflect.New(elemType)
			newSlicePtr.Elem().Set(newSlice)
			elem.Set(newSlicePtr)
		default:
			newElem := newReflectValueByType(elemType)
			elem.Set(newElem)
		}
		if !elem.Elem().IsValid() {
			break
		}
		elem = elem.Elem()
	}
	if elem.Type().Kind() == reflect.Map && elem.IsNil() {
		newMap := newReflectValueByType(elem.Type())
		elem.Set(newMap)
	}
	if elem.Type().Kind() == reflect.Slice && elem.IsNil() {
		newSlice := newReflectValueByType(elem.Type())
		elem.Set(newSlice)
	}
	if elem.Type().Kind() == reflect.Pointer && elem.Elem().IsValid() {
		elem = elem.Elem()
	}
	return elem
}

// Create a new value based on the given type
func newReflectValueByType(elemType reflect.Type) reflect.Value {
	var entry reflect.Value
	switch elemType.Kind() {
	case reflect.Map:
		entry = reflect.MakeMap(elemType)
	case reflect.Slice:
		entry = reflect.MakeSlice(elemType, 0, 10)
	case reflect.Pointer:
		entry = reflect.New(elemType)
		// todo test array
	default:
		entry = reflect.New(elemType)
	}
	return entry
}
