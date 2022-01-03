package autoquery

import "context"

// Table represents a specific DynamoDB table.
type Table struct {
	autoqueryClient *Client
	name            string
}

func (client *Client) Table(tableName string) *Table {
	return &Table{
		autoqueryClient: client,
		name:            tableName,
	}
}

func (table Table) Get(ctx context.Context, itemKey, returnItem interface{}) error {
	return table.autoqueryClient.Get(ctx, table.name, itemKey, returnItem)
}

func (table Table) Put(ctx context.Context, item interface{}) error {
	return table.autoqueryClient.Put(ctx, table.name, item)
}

func (table Table) Query(expr *Expression) *Parser {
	return table.autoqueryClient.Query(table.name, expr)
}
