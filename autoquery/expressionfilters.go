package autoquery

type expressionFilter interface{}

type equalsFilter struct {
	value interface{}
}

type lessThanFilter struct {
	value interface{}
}

type greaterThanFilter struct {
	value interface{}
}

type lessThanEqualFilter struct {
	value interface{}
}

type greaterThanEqualFilter struct {
	value interface{}
}

type beginsWithFilter struct {
	prefix string
}

type betweenFilter struct {
	lowval, highval interface{}
}
