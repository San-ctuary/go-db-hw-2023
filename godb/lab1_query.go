package godb

import (
	"os"
	"strings"
)

// This function should load the csv file in fileName into a heap file (see
// [HeapFile.LoadFromCSV]) and then compute the sum of the integer field in
// string and return its value as an int The supplied csv file is comma
// delimited and has a header If the file doesn't exist or can't be opened, or
// the field doesn't exist, or the field is not and integer, should return an
// err. Note that when you create a HeapFile, you will need to supply a file
// name;  you can supply a non-existant file, in which case it will be created.
// However, subsequent invocations of this method will result in tuples being
// reinserted into this file unless you delete (e.g., with [os.Remove] it before
// calling NewHeapFile.
func computeFieldSum(fileName string, td TupleDesc, sumField string) (int, error) {
	bp := NewBufferPool(10)
	split := strings.Split(fileName, ".")
	heapfileName := split[0] + ".dat"
	os.Remove(heapfileName)
	hf, err := NewHeapFile(heapfileName, &td, bp)
	if err != nil {
		return 0, err
	}
	f, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	hf.LoadFromCSV(f, true, ",", false)
	idx, err := findFieldNum(td, sumField)
	if err != nil {
		return -1, err
	}
	tid := NewTID()
	iter, _ := hf.Iterator(tid)
	cnt := 0
	for {
		t, _ := iter()
		if t == nil {
			break
		}
		fieldValue := t.Fields[idx].(IntField).Value
		cnt += int(fieldValue)
	}
	return cnt, nil // replace me
}

func findFieldNum(td TupleDesc, sumField string) (int, error) {
	for idx, field := range td.Fields {
		if field.Fname == sumField && field.Ftype == IntType {
			return idx, nil
		}
	}
	return -1, GoDBError{IncompatibleTypesError, "can not find suited field"}
}
