package autoquery

import "reflect"

func typesMatch(a, b interface{}) bool {
	return reflect.TypeOf(a) == reflect.TypeOf(b)
}
