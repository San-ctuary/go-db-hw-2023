package godb

// TODO: some code goes here
type InsertOp struct {
	// TODO: some code goes here
	dbFile DBFile
	child  Operator
}

// Construtor.  The insert operator insert the records in the child
// Operator into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	return &InsertOp{dbFile: insertFile, child: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	f := FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}
	fs := []FieldType{f}
	return &TupleDesc{Fields: fs}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	endFlag := false
	return func() (*Tuple, error) {
		if endFlag {
			return nil, nil
		}
		iter, err := iop.child.Iterator(tid)
		if err != nil {
			return nil, err
		}
		cnt := 0
		for {
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				field := IntField{Value: int64(cnt)}
				rntTup := &Tuple{Desc: *iop.Descriptor(), Fields: []DBValue{field}, Rid: nil}
				endFlag = true
				return rntTup, nil
			}
			cnt += 1
			err = iop.dbFile.insertTuple(tuple, tid)
			if err != nil {
				return nil, err
			}
		}
	}, nil
}
