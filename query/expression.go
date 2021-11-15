package query

// Expression contains conditions and filters to be used in a query.
type Expression struct {
	attributesSpecified bool
	attributes          []string

	consistentRead bool
}

// NewExpression creates a new Expression instance.
func NewExpression() *Expression {
	return &Expression{
		attributes: []string{},
	}
}

// Equal adds a new equal condition to the expression.
// All query expressions require at least one equal condition where the specified attribute is an
// index partition key.
func (expr *Expression) Equal(attr string, value interface{}) *Expression {
	// TODO: implement
	return expr
}

// LessThan adds a new less than condition to the expression.
func (expr *Expression) LessThan(attr string, value interface{}) *Expression {
	// TODO: implement
	return expr
}

// GreaterThan adds a new greater than condition to the expression.
func (expr *Expression) GreaterThan(attr string, value interface{}) *Expression {
	// TODO: implement
	return expr
}

// LessThanEqual adds a new less than or equal condition to the expression.
func (expr *Expression) LessThanEqual(attr string, value interface{}) *Expression {
	// TODO: implement
	return expr
}

// GreaterThanEqual adds a new greater than or equal condition to the expression.
func (expr *Expression) GreaterThanEqual(attr string, value interface{}) *Expression {
	// TODO: implement
	return expr
}

// Between adds a new between condition to the expression.
func (expr *Expression) Between(attr string, lowval, highval interface{}) *Expression {
	// TODO: implement
	return expr
}

// BeginsWith adds a new begins with condition to the expression.
func (expr *Expression) BeginsWith(attr string, prefix string) *Expression {
	// TODO: implement
	return expr
}

// OrderBy sets the sort attribute, in either ascending or descending order.
// Ordering by a specific attribute restricts the viable table indexes to those which use the
// attribute as a sort key.
func (expr *Expression) OrderBy(attr string, ascending bool) *Expression {
	return expr
}

// Select adds projection attributes to the query. Subsequent calls to Select will append to the
// existing attributes.
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
