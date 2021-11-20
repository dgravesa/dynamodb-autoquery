package autoquery

// ConditionKey forms part of a condition of an expression.
//
// The ConditionKey should be followed by a value condition in order to form a complete
// expression.
type ConditionKey struct {
	expr *Expression
	attr string
}

// Key begins a new expression with the key part of the condition.
//
// The resulting ConditionKey should be followed by a condition in order to form a complete
// expression.
func Key(attr string) *ConditionKey {
	return &ConditionKey{
		expr: NewExpression(),
		attr: attr,
	}
}

// Equal adds a new equal condition to the expression. Only items where the value of the key
// attribute equals v will be returned. All query expressions require at least one equal condition
// where the specified key attribute is an index partition key.
func (key *ConditionKey) Equal(v interface{}) *Expression {
	return key.expr.Equal(key.attr, v)
}

// LessThan adds a new less than condition to the expression. Only items where the value of the
// key attribute is less than v will be returned.
func (key *ConditionKey) LessThan(v interface{}) *Expression {
	return key.expr.LessThan(key.attr, v)
}

// GreaterThan adds a new greater than condition to the expression. Only items where the value of
// the key attribute is greater than v will be returned.
func (key *ConditionKey) GreaterThan(v interface{}) *Expression {
	return key.expr.GreaterThan(key.attr, v)
}

// LessThanEqual adds a new less than or equal condition to the expression. Only items where the
// value of the key attribute is less than or equal to v will be returned.
func (key *ConditionKey) LessThanEqual(v interface{}) *Expression {
	return key.expr.LessThanEqual(key.attr, v)
}

// GreaterThanEqual adds a new greater than or equal condition to the expression. Only items where
// the value of the key attribute is greater than or equal to v will be returned.
func (key *ConditionKey) GreaterThanEqual(v interface{}) *Expression {
	return key.expr.GreaterThanEqual(key.attr, v)
}

// Between adds a new between condition to the expression. Only items where the value of the
// key attribute is between lowval and highval will be returned.
func (key *ConditionKey) Between(lowval, highval interface{}) *Expression {
	return key.expr.Between(key.attr, lowval, highval)
}

// BeginsWith adds a new begins-with condition to the expression. Only items where the value of
// the key attribute begins with the specified prefix will be returned.
func (key *ConditionKey) BeginsWith(prefix string) *Expression {
	return key.expr.BeginsWith(key.attr, prefix)
}
