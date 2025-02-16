package godb

//This file defines methods for working with tuples, including defining
// the types DBType, FieldType, TupleDesc, DBValue, and Tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/mitchellh/hashstructure/v2"
)

// DBType is the type of a tuple field, in GoDB, e.g., IntType or StringType
type DBType int

const (
	IntType     DBType = iota
	StringType  DBType = iota
	UnknownType DBType = iota //used internally, during parsing, because sometimes the type is unknown
)

var typeNames map[DBType]string = map[DBType]string{IntType: "int", StringType: "string"}

// FieldType is the type of a field in a tuple, e.g., its name, table, and [godb.DBType].
// TableQualifier may or may not be an emtpy string, depending on whether the table
// was specified in the query
type FieldType struct {
	Fname          string
	TableQualifier string
	Ftype          DBType
}

// TupleDesc is "type" of the tuple, e.g., the field names and types
type TupleDesc struct {
	Fields []FieldType
}

// Compare two tuple descs, and return true iff
// all of their field objects are equal and they
// are the same length
func (d1 *TupleDesc) equals(d2 *TupleDesc) bool {
	//TODO: some code goes here
	if len(d1.Fields) != len(d2.Fields) {
		return false
	}
	for i := 0; i < len(d1.Fields); i++ {
		if d1.Fields[i] != d2.Fields[i] {
			return false
		}
	}
	return true
}

// Given a FieldType f and a TupleDesc desc, find the best
// matching field in desc for f.  A match is defined as
// having the same Ftype and the same name, preferring a match
// with the same TableQualifier if f has a TableQualifier
// We have provided this implementation because it's details are
// idiosyncratic to the behavior of the parser, which we are not
// asking you to write
func findFieldInTd(field FieldType, desc *TupleDesc) (int, error) {
	best := -1
	for i, f := range desc.Fields {
		if f.Fname == field.Fname && (f.Ftype == field.Ftype || field.Ftype == UnknownType) {
			if field.TableQualifier == "" && best != -1 {
				return 0, GoDBError{AmbiguousNameError, fmt.Sprintf("select name %s is ambiguous", f.Fname)}
			}
			if f.TableQualifier == field.TableQualifier || best == -1 {
				best = i
			}
		}
	}
	if best != -1 {
		return best, nil
	}
	return -1, GoDBError{IncompatibleTypesError, fmt.Sprintf("field %s.%s not found", field.TableQualifier, field.Fname)}
}

// Make a copy of a tuple desc.  Note that in go, assignment of a slice to
// another slice object does not make a copy of the contents of the slice.
// Look at the built-in function "copy".
func (td *TupleDesc) copy() *TupleDesc {
	// TODO: some code goes here
	dest := TupleDesc{}
	dest.Fields = make([]FieldType, len(td.Fields))
	copy(dest.Fields, td.Fields)
	return &dest //replace me
}

// Assign the TableQualifier of every field in the TupleDesc to be the
// supplied alias.  We have provided this function as it is only used
// by the parser.
func (td *TupleDesc) setTableAlias(alias string) {
	fields := make([]FieldType, len(td.Fields))
	copy(fields, td.Fields)
	for i := range fields {
		fields[i].TableQualifier = alias
	}
	td.Fields = fields
}

// Merge two TupleDescs together.  The resulting TupleDesc
// should consist of the fields of desc2
// appended onto the fields of desc.
func (desc *TupleDesc) merge(desc2 *TupleDesc) *TupleDesc {
	// TODO: some code goes here
	result := desc.copy()
	for _, field := range desc2.Fields {
		result.Fields = append(result.Fields, field)
	}
	return result //replace me
}

// ================== Tuple Methods ======================

// Interface used for tuple field values
// Since it implements no methods, any object can be used
// but having an interface for this improves code readability
// where tuple values are used
type DBValue interface {
}

// Integer field value
type IntField struct {
	Value int64
}

// String field value
type StringField struct {
	Value string
}

// Tuple represents the contents of a tuple read from a database
// It includes the tuple descriptor, and the value of the fields
type Tuple struct {
	Desc   TupleDesc
	Fields []DBValue
	Rid    recordID //used to track the page and position this page was read from
}

type recordID interface {
}

// Serialize the contents of the tuple into a byte array Since all tuples are of
// fixed size, this method should simply write the fields in sequential order
// into the supplied buffer.
//
// See the function [binary.Write].  Objects should be serialized in little
// endian oder.
//
// Strings can be converted to byte arrays by casting to []byte. Note that all
// strings need to be padded to StringLength bytes (set in types.go). For
// example if StringLength is set to 5, the string 'mit' should be written as
// 'm', 'i', 't', 0, 0
//
// May return an error if the buffer has insufficient capacity to store the
// tuple.
func (t *Tuple) writeTo(b *bytes.Buffer) error {
	// TODO: some code goes here
	for _, field := range t.Fields {
		switch field.(type) {
		case IntField:
			err := binary.Write(b, binary.LittleEndian, field.(IntField).Value)
			if err != nil {
				return err
			}
		case StringField:
			stringField := field.(StringField).Value
			byteField := [32]byte{}
			for idx, c := range stringField {
				byteField[idx] = byte(c)
			}
			err := binary.Write(b, binary.LittleEndian, byteField)
			if err != nil {
				return err
			}
		}
	}
	return nil //replace me
}

// Read the contents of a tuple with the specified [TupleDesc] from the
// specified buffer, returning a Tuple.
//
// See [binary.Read]. Objects should be deserialized in little endian oder.
//
// All strings are stored as StringLength byte objects.
//
// Strings with length < StringLength will be padded with zeros, and these
// trailing zeros should be removed from the strings.  A []byte can be cast
// directly to string.
//
// May return an error if the buffer has insufficent data to deserialize the
// tuple.
func readTupleFrom(b *bytes.Buffer, desc *TupleDesc) (*Tuple, error) {
	// TODO: some code goes here
	tuple := &Tuple{}
	tuple.Desc = *desc
	tuple.Fields = make([]DBValue, 0)
	for _, field := range desc.Fields {
		switch field.Ftype {
		case IntType:
			intField := IntField{}
			err := binary.Read(b, binary.LittleEndian, &intField)
			if err != nil {
				return nil, err
			}
			tuple.Fields = append(tuple.Fields, intField)
		case StringType:
			byteRed := [StringLength]byte{}
			err := binary.Read(b, binary.LittleEndian, &byteRed)
			if err != nil {
				return nil, err
			}
			temp := make([]byte, 0)
			for _, c := range byteRed {
				if c == 0 {
					break
				}
				temp = append(temp, c)
			}
			stringField := StringField{
				string(temp),
			}
			tuple.Fields = append(tuple.Fields, stringField)
		}
	}
	return tuple, nil //replace me
}

// Compare two tuples for equality.  Equality means that the TupleDescs are equal
// and all of the fields are equal.  TupleDescs should be compared with
// the [TupleDesc.equals] method, but fields can be compared directly with equality
// operators.
func (t1 *Tuple) equals(t2 *Tuple) bool {
	// TODO: some code goes here
	if !t1.Desc.equals(&t2.Desc) || len(t1.Fields) != len(t2.Fields) {
		return false
	}
	for idx, f1 := range t1.Fields {
		if f1 != t2.Fields[idx] {
			return false
		}
	}
	return true
}

// Merge two tuples together, producing a new tuple with the fields of t2 appended to t1.
func joinTuples(t1 *Tuple, t2 *Tuple) *Tuple {
	// TODO: some code goes here
	result := &Tuple{}
	if t1 != nil {
		result.Desc = *t1.Desc.copy()
		result.Fields = make([]DBValue, len(t1.Fields))
		copy(result.Fields, t1.Fields)
	}
	if t2 == nil {
		return result
	}
	for idx, f := range t2.Fields {
		result.Fields = append(result.Fields, f)
		result.Desc.Fields = append(result.Desc.Fields, t2.Desc.Fields[idx])
	}
	return result
}

type orderByState int

const (
	OrderedLessThan    orderByState = iota
	OrderedEqual       orderByState = iota
	OrderedGreaterThan orderByState = iota
)

// Apply the supplied expression to both t and t2, and compare the results,
// returning an orderByState value.
//
// Takes an arbitrary expressions rather than a field, because, e.g., for an
// ORDER BY SQL may ORDER BY arbitrary expressions, e.g., substr(name, 1, 2)
//
// Note that in most cases Expr will be a [godb.FieldExpr], which simply
// extracts a named field from a supplied tuple.
//
// Calling the [Expr.EvalExpr] method on a tuple will return the value of the
// expression on the supplied tuple.
func (t *Tuple) compareField(t2 *Tuple, field Expr) (orderByState, error) {
	// TODO: some code goes here
	r1, err := field.EvalExpr(t)
	r2, err := field.EvalExpr(t2)
	if err != nil {
		return OrderedEqual, err
	}
	state := OrderedEqual
	r1StringField, ok1 := r1.(StringField)
	r2StringField, ok2 := r2.(StringField)
	r1IntField, ok3 := r1.(IntField)
	r2IntField, ok4 := r2.(IntField)
	if ok1 && ok2 {
		if r1StringField.Value < r2StringField.Value {
			state = OrderedLessThan
		} else if r1StringField.Value > r2StringField.Value {
			state = OrderedGreaterThan
		}
	} else if !ok3 && !ok4 {
		if r1IntField.Value < r1IntField.Value {
			state = OrderedLessThan
		} else if r1IntField.Value > r2IntField.Value {
			state = OrderedGreaterThan
		}
	} else {
		return state, GoDBError{IncompatibleTypesError, "different type"}
	}
	return state, nil // replace me
}

// Project out the supplied fields from the tuple. Should return a new Tuple
// with just the fields named in fields.
//
// Should not require a match on TableQualifier, but should prefer fields that
// do match on TableQualifier (e.g., a field  t1.name in fields should match an
// entry t2.name in t, but only if there is not an entry t1.name in t)
func (t *Tuple) project(fields []FieldType) (*Tuple, error) {
	// TODO: some code goes here
	result := &Tuple{}
	result.Desc = TupleDesc{make([]FieldType, 0)}
	result.Fields = make([]DBValue, 0)
	for _, field := range fields {
		best := -1
		for idx, sourceField := range t.Desc.Fields {
			if sourceField.Fname == field.Fname && (sourceField.TableQualifier == field.TableQualifier || best == -1) {
				best = idx
			}
		}
		if best != -1 {
			result.Fields = append(result.Fields, t.Fields[best])
			result.Desc.Fields = append(result.Desc.Fields, t.Desc.Fields[best])
		}
	}
	return result, nil //replace me
}

// Compute a key for the tuple to be used in a map structure
func (t *Tuple) tupleKey() any {

	//todo efficiency here is poor - hashstructure is probably slow
	hash, _ := hashstructure.Hash(t, hashstructure.FormatV2, nil)

	return hash
}

var winWidth int = 120

func fmtCol(v string, ncols int) string {
	colWid := winWidth / ncols
	nextLen := len(v) + 3
	remLen := colWid - nextLen
	if remLen > 0 {
		spacesRight := remLen / 2
		spacesLeft := remLen - spacesRight
		return strings.Repeat(" ", spacesLeft) + v + strings.Repeat(" ", spacesRight) + " |"
	} else {
		return " " + v[0:colWid-4] + " |"
	}
}

// Return a string representing the header of a table for a tuple with the
// supplied TupleDesc.
//
// Aligned indicates if the tuple should be foramtted in a tabular format
func (d *TupleDesc) HeaderString(aligned bool) string {
	outstr := ""
	for i, f := range d.Fields {
		tableName := ""
		if f.TableQualifier != "" {
			tableName = f.TableQualifier + "."
		}

		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(tableName+f.Fname, len(d.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, tableName+f.Fname)
		}
	}
	return outstr
}

// Return a string representing the tuple
// Aligned indicates if the tuple should be formatted in a tabular format
func (t *Tuple) PrettyPrintString(aligned bool) string {
	outstr := ""
	for i, f := range t.Fields {
		str := ""
		switch f := f.(type) {
		case IntField:
			str = fmt.Sprintf("%d", f.Value)
		case StringField:
			str = f.Value
		}
		if aligned {
			outstr = fmt.Sprintf("%s %s", outstr, fmtCol(str, len(t.Fields)))
		} else {
			sep := ","
			if i == 0 {
				sep = ""
			}
			outstr = fmt.Sprintf("%s%s%s", outstr, sep, str)
		}
	}
	return outstr

}
