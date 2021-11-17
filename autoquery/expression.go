package autoquery

// Expression contains conditions and filters to be used in a query.
type Expression struct {
	filters map[string]expressionFilter

	attributesSpecified bool
	attributes          []string

	orderSpecified bool
	orderAttribute string
	orderAscending bool

	consistentRead bool
}

// NewExpression creates a new Expression instance.
func NewExpression() *Expression {
	return &Expression{
		attributes: []string{},
	}
}

// Equal adds a new equal condition to the expression. Only items where the value of the attribute
// attr equals v will be returned. All query expressions require at least one equal condition
// where the specified attribute attr is an index partition key.
func (expr *Expression) Equal(attr string, v interface{}) *Expression {
	// TODO: implement
	return expr
}

// LessThan adds a new less than condition to the expression. Only items where the value of the
// attribute attr is less than v will be returned.
func (expr *Expression) LessThan(attr string, v interface{}) *Expression {
	// TODO: implement
	return expr
}

// GreaterThan adds a new greater than condition to the expression. Only items where the value of
// the attribute attr is greater than v will be returned.
func (expr *Expression) GreaterThan(attr string, v interface{}) *Expression {
	// TODO: implement
	return expr
}

// LessThanEqual adds a new less than or equal condition to the expression. Only items where the
// value of the attribute attr is less than or equal to v will be returned.
func (expr *Expression) LessThanEqual(attr string, v interface{}) *Expression {
	// TODO: implement
	return expr
}

// GreaterThanEqual adds a new greater than or equal condition to the expression. Only items where
// the value of the attribute attr is greater than or equal to v will be returned.
func (expr *Expression) GreaterThanEqual(attr string, v interface{}) *Expression {
	// TODO: implement
	return expr
}

// Between adds a new between condition to the expression. Only items where the value of the
// attribute attr is between lowval and highval will be returned.
func (expr *Expression) Between(attr string, lowval, highval interface{}) *Expression {
	// TODO: implement
	return expr
}

// BeginsWith adds a new begins-with condition to the expression. Only items where the value of
// the attribute attr begins with the specified prefix will be returned.
func (expr *Expression) BeginsWith(attr string, prefix string) *Expression {
	// TODO: implement
	return expr
}

// OrderBy sets attr as the sort attribute. If ascending is true, items will be returned starting
// with the lowest value for the attribute. If ascending is false, the highest value will be
// returned first. OrderBy may only be used on sort key attributes of indexes which satisfy all
// other expression criteria.
func (expr *Expression) OrderBy(attr string, ascending bool) *Expression {
	expr.orderSpecified = true
	expr.orderAttribute = attr
	expr.orderAscending = ascending
	return expr
}

// Select specifies attributes that should be returned in queried items. Subsequent calls to
// Select will append to the existing selected attributes for the expression.
//
// If Select is not specified for an expression, the query will project all attributes for each
// returned item, but can only use indexes which project all attributes. When Select is specified,
// any indexes which include every selected attribute and satisfy all other expression criteria
// will be considered for the query index.
func (expr *Expression) Select(attrs ...string) *Expression {
	expr.attributesSpecified = true
	expr.attributes = append(expr.attributes, attrs...)
	return expr
}

// ConsistentRead sets the read consistency of each query page request.
// Note that consistent read only guarantees consistency within each page.
// Consistent read is not supported across all items in the query when pagination is required
// to parse all items (i.e. when the query evaluates more than 1MB of data).
// Consistent read is not supported on global secondary indexes.
func (expr *Expression) ConsistentRead(val bool) *Expression {
	expr.consistentRead = val
	return expr
}

// TODO: implement
// func (expr *Expression) Filter(filterExpr expression.ConditionBuilder) *Expression {
// 	return expr
// }

// func (expr *Expression) getKeysOfFilterType(v interface{}) []string {
// 	vType := reflect.TypeOf(v)

// 	// create set of all keys with specific filters
// 	keys := []string{}
// 	for key, filter := range expr.filters {
// 		fType := reflect.TypeOf(filter)
// 		if fType == vType {
// 			keys = append(keys, key)
// 		}
// 	}

// 	return keys
// }
