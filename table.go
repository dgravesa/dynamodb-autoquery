package autoquery

import "context"

// Table represents a specific DynamoDB table.
type Table struct {
	autoqueryClient *Client
	name            string
}

// Table initializes a new Table instance from an autoquery client.
func (client *Client) Table(tableName string) *Table {
	return &Table{
		autoqueryClient: client,
		name:            tableName,
	}
}

// Get retrieves a single item by its key. The key is specified in itemKey and should be a struct
// with the appropriate dynamodbav attribute tags pertaining to the table's primary key.
// The item is returned in returnItem, which should have dynamodbav attribute tags pertaining to
// the desired return attributes in the table.
//
// If the item is not found, ErrItemNotFound is returned.
func (table Table) Get(ctx context.Context, itemKey, returnItem interface{}) error {
	return table.autoqueryClient.Get(ctx, table.name, itemKey, returnItem)
}

// Put inserts a new item into the table, or replaces it if an item with the same primary key
// already exists. The item should be a struct with the appropriate dynamodbav attribute tags.
func (table Table) Put(ctx context.Context, item interface{}) error {
	return table.autoqueryClient.Put(ctx, table.name, item)
}

// Query initializes a query defined by expr on a table. The returned parser may be used to
// retrieve items using Parser.Next.
func (table Table) Query(expr *Expression) *Parser {
	return table.autoqueryClient.Query(table.name, expr)
}
