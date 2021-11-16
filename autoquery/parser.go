package autoquery

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Parser is used for parsing query results.
type Parser struct {
	maxPagesSpecified bool
	maxPages          int

	limitPerPageSpecified bool
	limitPerPage          int

	exclusiveStartkey map[string]*dynamodb.AttributeValue
}

// Next retrieves the next item in the query.
//
// The first call to Next on a Parser always makes a query call to DynamoDB.
// On subsequent calls, the remaining buffered items will be returned in order until all buffered
// items have been returned.
// Next will make subsequent paginated query calls to DynamoDB to refill the internal buffer until
// all max pagination has been reached or until all items in the query have been returned.
//
// Once all items have been returned or max pagination has been reached, the query will return an
// ErrParsingComplete instance.
func (parser *Parser) Next(ctx context.Context, returnItem interface{}) error {
	// TODO: implement
	return fmt.Errorf("not yet implemented")
}

// SetMaxPagination sets the maximum number of pages to query.
// By default, the parser will consume additional pages until all query items have been read.
func (parser *Parser) SetMaxPagination(maxPages int) *Parser {
	parser.maxPagesSpecified = true
	parser.maxPages = maxPages
	return parser
}

// UnsetMaxPagination unsets the maximum pagination limit.
func (parser *Parser) UnsetMaxPagination() *Parser {
	parser.maxPagesSpecified = false
	return parser
}

// SetLimitPerPage sets the limit parameter for each page query call to DynamoDB.
// The limit parameter restricts the number of evaluated items, not the number of returned items.
func (parser *Parser) SetLimitPerPage(limit int) *Parser {
	parser.limitPerPageSpecified = true
	parser.limitPerPage = limit
	return parser
}

// UnsetLimitPerPage unsets the limit parameter for each page query call to DynamoDB.
func (parser *Parser) UnsetLimitPerPage() *Parser {
	parser.limitPerPageSpecified = false
	return parser
}

// SetExclusiveStartKey sets the exclusive start key for the next page query call to DynamoDB.
func (parser *Parser) SetExclusiveStartKey(
	exclusiveStartKey map[string]*dynamodb.AttributeValue) *Parser {
	parser.exclusiveStartkey = exclusiveStartKey
	return parser
}

// TODO: is this possible?
// // LastParsedKey returns the key of the most recent item parsed by Next.
// //
// // The last parsed key may be used in a subsequent request as the exclusive start key in order
// // to return additional values without needing to manage underlying pagination.
// func (parser *Parser) LastParsedKey() map[string]*dynamodb.AttributeValue {
// 	return parser.exclusiveStartkey
// }
