package autoquery

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Parser is used for parsing query results.
type Parser struct {
	client *Client

	tableName string
	expr      *Expression

	maxPagesSpecified bool
	maxPages          int
	currentPage       int

	limitPerPageSpecified bool
	limitPerPage          int

	exclusiveStartkey map[string]*dynamodb.AttributeValue

	queryInput *dynamodb.QueryInput

	bufferedItems      []map[string]*dynamodb.AttributeValue
	currentBufferIndex int
}

// Next retrieves the next item in the query. The returnItem is unmarshaled with "dynamodbav"
// struct tags.
//
// On the first call to Next with a new table, the table's index metadata will be retrieved using
// the underlying metadata provider. For the default client created by NewClient, this requires
// IAM permissions to describe the table. The metadata is cached for subsequent queries to the
// table through the client instance used in the call to Query.
//
// The first call to Next on a new Parser always makes a query call to DynamoDB. The query
// automatically selects an index based on the table metadata and any expression restrictions. On
// subsequent calls, the remaining buffered items will be returned in order until all buffered
// items have been returned. Next will make subsequent paginated query calls to DynamoDB to refill
// the internal buffer as necessary until max pages have been parsed completely or until all items
// in the query have been returned, whichever comes first. If no viable indexes are found, the
// call returns an ErrNoViableIndexes error.
//
// Once all items have been returned or max pagination has been reached, the query will return
// ErrParsingComplete.
func (parser *Parser) Next(ctx context.Context, returnItem interface{}) error {
	// refill buffer if necessary, including first call
	for parser.currentBufferIndex == len(parser.bufferedItems) {
		// check for parsing complete conditions
		if parser.allItemsParsed() {
			return &ErrParsingComplete{reason: "all items have been parsed"}
		} else if parser.maxPaginationReached() {
			return &ErrParsingComplete{reason: "max pagination has been reached"}
		}

		// construct query input using table metadata and expression on first call
		if err := parser.buildQueryInput(ctx); err != nil {
			return err
		}

		// execute new query to refill buffer
		queryOutput, err := parser.client.dynamodbService.QueryWithContext(ctx, parser.queryInput)
		if err != nil {
			return err
		}

		parser.exclusiveStartkey = queryOutput.LastEvaluatedKey
		parser.currentPage++
		parser.bufferedItems = queryOutput.Items
		parser.currentBufferIndex = 0
	}

	currentItem := parser.bufferedItems[parser.currentBufferIndex]
	parser.currentBufferIndex++

	return dynamodbattribute.UnmarshalMap(currentItem, returnItem)
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

func (parser *Parser) lastEvaluatedKeyIsEmpty() bool {
	return parser.exclusiveStartkey == nil || len(parser.exclusiveStartkey) == 0
}

func (parser *Parser) allItemsParsed() bool {
	return parser.currentPage > 0 && parser.lastEvaluatedKeyIsEmpty()
}

func (parser *Parser) maxPaginationReached() bool {
	return parser.maxPagesSpecified && (parser.currentPage >= parser.maxPages)
}

func (parser *Parser) buildQueryInput(ctx context.Context) error {
	// select index and construct expression on first call
	if parser.queryInput == nil {
		queryIndex, err := parser.client.chooseIndex(ctx, parser.tableName, parser.expr)
		if err != nil {
			return err
		}

		parser.queryInput, err = parser.expr.constructQueryInputGivenIndex(queryIndex)
		if err != nil {
			return err
		}
	}

	parser.queryInput.TableName = aws.String(parser.tableName)

	if parser.limitPerPageSpecified {
		parser.queryInput.Limit = aws.Int64(int64(parser.limitPerPage))
	} else {
		parser.queryInput.Limit = nil
	}

	parser.queryInput.ExclusiveStartKey = parser.exclusiveStartkey

	return nil
}
