package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	//add additional fields here
	// TODO: some code goes here
	distinct bool
}

// Project constructor -- should save the list of selected field, child, and the child op.
// Here, selectFields is a list of expressions that represents the fields to be selected,
// outputNames are names by which the selected fields are named (should be same length as
// selectFields; throws error if not), distinct is for noting whether the projection reports
// only distinct results, and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	if len(selectFields) != len(outputNames) {
		return nil, nil
	}
	p := &Project{selectFields: selectFields, outputNames: outputNames, distinct: distinct, child: child}
	return p, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should contain
// fields for each field in the constructor selectFields list with outputNames
// as specified in the constructor.
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := make([]FieldType, 0)
	for idx, field := range p.selectFields {
		fName := p.outputNames[idx]
		fieldInfo := field.GetExprType()
		ft := FieldType{Ftype: fieldInfo.Ftype, TableQualifier: fieldInfo.TableQualifier, Fname: fName}
		fts = append(fts, ft)
	}
	desc := &TupleDesc{fts}
	return desc
}

// Project operator implementation.  This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed.
// To implement this you will need to record in some data structure with the
// distinct tuples seen so far.  Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	mp := make(map[any]bool, 0)
	iter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, nil
	}
	//fields := make([]FieldType, 0)
	//for _, field := range p.selectFields {
	//	f := field.GetExprType()
	//	fields = append(fields, f)
	//}
	return func() (*Tuple, error) {
		for {
			tuple, err := iter()
			if err != nil {
				return nil, err
			}
			if tuple == nil {
				return nil, nil
			}
			var fields []DBValue = make([]DBValue, 0)
			for _, field := range p.selectFields {
				dbValue, err := field.EvalExpr(tuple)
				if err != nil {
					return nil, err
				}
				fields = append(fields, dbValue)
			}
			projectTup := &Tuple{*p.Descriptor(), fields, nil}
			//projectTup, err := tuple.project(fields)
			//projectTup.Desc = *p.Descriptor()
			if err != nil {
				return nil, err
			}
			if p.distinct {
				key := projectTup.tupleKey()
				if _, ok := mp[key]; !ok {
					mp[key] = true
					return projectTup, nil
				}
			} else {
				return projectTup, nil
			}
		}
	}, nil
}
