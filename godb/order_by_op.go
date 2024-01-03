package godb

import "sort"

// TODO: some code goes here
type tupleInfo struct {
	tuple     *Tuple
	values    []DBValue
	ascending []bool
}

func compareValue(a, b DBValue) bool {
	aValue, ok := a.(IntField)
	bValue, _ := b.(IntField)
	if ok {
		return aValue.Value < bValue.Value
	} else {
		return a.(StringField).Value < b.(StringField).Value
	}
}

type tuples []*tupleInfo

func (t tuples) Len() int {
	return len(t)
}

func (t tuples) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t tuples) Less(i, j int) bool {
	cnt := len(t[i].values)
	first, second := t[i], t[j]
	for idx := 0; idx < cnt; idx++ {
		if first.values[idx] != second.values[idx] {
			if first.ascending[idx] {
				return compareValue(first.values[idx], second.values[idx])
			} else {
				return compareValue(second.values[idx], first.values[idx])
			}
		}
	}
	return true
}

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	//add additional fields here
	ascending []bool
}

// Order by constructor -- should save the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extacted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields
// list should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	return &OrderBy{orderByFields, child, ascending}, nil
}

func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
}

// Return a function that iterators through the results of the child iterator in
// ascending/descending order, as specified in the construtor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort pacakge and the [sort.Sort] method for this purpose.  To
// use this you will need to implement three methods:  Len, Swap, and Less that
// the sort algorithm will invoke to preduce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at: https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
	tps := make([]*tupleInfo, 0)
	//tps := make(tuples, 0)
	for {
		tuple, err := iter()
		if err != nil {
			return nil, nil
		}
		if tuple == nil {
			break
		}
		values := make([]DBValue, 0)
		for _, field := range o.orderBy {
			value, err := field.EvalExpr(tuple)
			if err != nil {
				return nil, err
			}
			values = append(values, value)
		}
		info := &tupleInfo{tuple, values, o.ascending}
		tps = append(tps, info)
	}
	sort.Sort(tuples(tps))
	idx := 0
	return func() (*Tuple, error) {
		for idx < len(tps) {
			info := tps[idx]
			idx += 1
			return info.tuple, nil
		}
		return nil, nil
	}, nil
}
