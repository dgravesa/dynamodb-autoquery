package autoquery

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

// Expression contains conditions and filters to be used in a query.
type Expression struct {
	filters map[string]conditionFilter

	attributesSpecified bool
	attributes          []string

	orderSpecified bool
	orderAttribute string
	orderAscending bool

	consistentRead bool

	additionalConditions []expression.ConditionBuilder
}

// NewExpression creates a new Expression instance.
func NewExpression() *Expression {
	return &Expression{
		attributes:           []string{},
		additionalConditions: []expression.ConditionBuilder{},
	}
}

// Equal adds a new equal condition to the expression. Only items where the value of the attribute
// attr equals v will be returned. All query expressions require at least one equal condition
// where the specified attribute attr is an index partition key.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) Equal(attr string, v interface{}) *Expression {
	expr.filters[attr] = &equalsFilter{value: v}
	return expr
}

// LessThan adds a new less than condition to the expression. Only items where the value of the
// attribute attr is less than v will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) LessThan(attr string, v interface{}) *Expression {
	expr.filters[attr] = &lessThanFilter{value: v}
	return expr
}

// GreaterThan adds a new greater than condition to the expression. Only items where the value of
// the attribute attr is greater than v will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) GreaterThan(attr string, v interface{}) *Expression {
	expr.filters[attr] = &greaterThanFilter{value: v}
	return expr
}

// LessThanEqual adds a new less than or equal condition to the expression. Only items where the
// value of the attribute attr is less than or equal to v will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) LessThanEqual(attr string, v interface{}) *Expression {
	expr.filters[attr] = &lessThanEqualFilter{value: v}
	return expr
}

// GreaterThanEqual adds a new greater than or equal condition to the expression. Only items where
// the value of the attribute attr is greater than or equal to v will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) GreaterThanEqual(attr string, v interface{}) *Expression {
	expr.filters[attr] = &greaterThanEqualFilter{value: v}
	return expr
}

// Between adds a new between condition to the expression. Only items where the value of the
// attribute attr is between lowval and highval will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) Between(attr string, lowval, highval interface{}) *Expression {
	expr.filters[attr] = &betweenFilter{lowval: lowval, highval: highval}
	return expr
}

// BeginsWith adds a new begins-with condition to the expression. Only items where the value of
// the attribute attr begins with the specified prefix will be returned.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) BeginsWith(attr string, prefix string) *Expression {
	expr.filters[attr] = &beginsWithFilter{prefix: prefix}
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

// And begins a new condition on an existing expression.
//
// The resulting ConditionKey should be followed by a condition in order to form a complete
// expression.
//
// If multiple filter conditions are specified on the same attribute, only the most recent
// condition will apply to the expression.
func (expr *Expression) And(attr string) *ConditionKey {
	return &ConditionKey{
		expr: expr,
		attr: attr,
	}
}

// Filter applies a condition from the DynamoDB expression package to an expression. Subsequent
// calls to Filter will append additional filters, and all filters will be applied as part of the
// expression.
//
// These filters may be used to enable conditions that are otherwise not supported, such as OR
// conditions.
func (expr *Expression) Filter(filterCondition expression.ConditionBuilder) *Expression {
	expr.additionalConditions = append(expr.additionalConditions, filterCondition)
	return expr
}

func (expr *Expression) constructQueryInputGivenIndex(
	index *tableIndex) (*dynamodb.QueryInput, error) {

	dynamodbExprBuilder := expression.NewBuilder()

	// copy expression filters into local map
	filters := map[string]conditionFilter{}
	for k, v := range expr.filters {
		filters[k] = v
	}

	// initialize partition equals part of key condition expression
	kce := expression.Key(index.PartitionKey).
		Equal(expression.Value(expr.filters[index.PartitionKey].(*equalsFilter).value))
	delete(filters, index.PartitionKey)

	// apply sort key condition to key condition expression if applicable
	if index.IsComposite {
		filter, hasSortKeyFilter := filters[index.SortKey]
		if hasSortKeyFilter {
			builder := expression.Key(index.SortKey)
			switch f := filter.(type) {
			case *equalsFilter:
				kce = kce.And(builder.Equal(expression.Value(f.value)))
			case *lessThanFilter:
				kce = kce.And(builder.LessThan(expression.Value(f.value)))
			case *greaterThanFilter:
				kce = kce.And(builder.GreaterThan(expression.Value(f.value)))
			case *lessThanEqualFilter:
				kce = kce.And(builder.LessThanEqual(expression.Value(f.value)))
			case *greaterThanEqualFilter:
				kce = kce.And(builder.GreaterThanEqual(expression.Value(f.value)))
			case *betweenFilter:
				kce = kce.And(builder.Between(
					expression.Value(f.lowval), expression.Value(f.highval)))
			case *beginsWithFilter:
				kce = kce.And(builder.BeginsWith(f.prefix))
			}
			delete(filters, index.SortKey)
		}
	}

	dynamodbExprBuilder = dynamodbExprBuilder.WithKeyCondition(kce)

	// apply remaining filters as filter conditions
	filterConditions := []expression.ConditionBuilder{}
	for key, filter := range filters {
		var fc expression.ConditionBuilder
		switch f := filter.(type) {
		case *equalsFilter:
			fc = expression.Name(key).Equal(expression.Value(f.value))
		case *lessThanFilter:
			fc = expression.Name(key).LessThan(expression.Value(f.value))
		case *greaterThanFilter:
			fc = expression.Name(key).GreaterThan(expression.Value(f.value))
		case *lessThanEqualFilter:
			fc = expression.Name(key).LessThanEqual(expression.Value(f.value))
		case *greaterThanEqualFilter:
			fc = expression.Name(key).GreaterThanEqual(expression.Value(f.value))
		case *betweenFilter:
			fc = expression.Name(key).Between(
				expression.Value(f.lowval), expression.Value(f.highval))
		case *beginsWithFilter:
			fc = expression.Name(key).BeginsWith(f.prefix)
		}
		filterConditions = append(filterConditions, fc)
	}

	// apply additional filter conditions, if specified
	filterConditions = append(filterConditions, expr.additionalConditions...)

	if len(filterConditions) == 1 {
		dynamodbExprBuilder = dynamodbExprBuilder.WithFilter(filterConditions[0])
	} else if len(filterConditions) > 1 {
		dynamodbExprBuilder = dynamodbExprBuilder.WithFilter(expression.And(
			filterConditions[0],
			filterConditions[1],
			filterConditions[2:]...))
	}

	// set projection if specified
	if expr.attributesSpecified {
		names := []expression.NameBuilder{}
		for _, attribute := range expr.attributes {
			names = append(names, expression.Name(attribute))
		}
		proj := expression.NamesList(names[0], names[1:]...)
		dynamodbExprBuilder = dynamodbExprBuilder.WithProjection(proj)
	}

	dynamodbExpr, err := dynamodbExprBuilder.Build()
	if err != nil {
		return nil, err
	}

	queryInput := &dynamodb.QueryInput{
		KeyConditionExpression:    dynamodbExpr.KeyCondition(),
		FilterExpression:          dynamodbExpr.Filter(),
		ExpressionAttributeNames:  dynamodbExpr.Names(),
		ExpressionAttributeValues: dynamodbExpr.Values(),
		ProjectionExpression:      dynamodbExpr.Projection(),
	}

	if index.Name != tablePrimaryIndexName {
		queryInput.IndexName = aws.String(index.Name)
	}

	if expr.consistentRead {
		queryInput.ConsistentRead = aws.Bool(true)
	}

	if expr.orderSpecified {
		queryInput.ScanIndexForward = aws.Bool(expr.orderAscending)
	}

	return queryInput, nil
}
