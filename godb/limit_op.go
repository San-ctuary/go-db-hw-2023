package godb

type LimitOp struct {
	child     Operator //required fields for parser
	limitTups Expr
	//add additional fields here, if needed
}

// Limit constructor -- should save how many tuples to return and the child op.
// lim is how many tuples to return and child is the child op.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	// TODO: some code goes here
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the
// results of the child iterator, and limit the result set to the first
// [lim] tuples it sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	lim, err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, nil
	}
	limit := lim.(IntField).Value
	iter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
	return func() (*Tuple, error) {
		for limit > 0 {
			tuple, err := iter()
			if err != nil {
				return nil, nil
			}
			if tuple == nil {
				limit = 0
				break
			}
			limit -= 1
			return tuple, nil
		}
		return nil, nil
	}, nil
}
