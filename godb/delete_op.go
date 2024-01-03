package godb

type DeleteOp struct {
	// TODO: some code goes here
	dbFile DBFile
	child  Operator
}

// Construtor.  The delete operator deletes the records in the child
// Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{dbFile: deleteFile, child: child}
}

// The delete TupleDesc is a one column descriptor with an integer field named "count"
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	f := FieldType{Fname: "count", TableQualifier: "", Ftype: IntType}
	fs := []FieldType{f}
	return &TupleDesc{Fields: fs}
}

// Return an iterator function that deletes all of the tuples from the child
// iterator from the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were deleted.  Tuples should be deleted using the [DBFile.deleteTuple]
// method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	endFlag := false
	return func() (*Tuple, error) {
		if endFlag {
			return nil, nil
		}
		iter, err := dop.child.Iterator(tid)
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
				rntTup := &Tuple{Desc: *dop.Descriptor(), Fields: []DBValue{field}, Rid: nil}
				endFlag = true
				return rntTup, nil
			}
			cnt += 1
			err = dop.dbFile.deleteTuple(tuple, tid)
			if err != nil {
				return nil, err
			}
		}
	}, nil
}
