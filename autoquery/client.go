package autoquery

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"github.com/dgravesa/dynamodb-helpers/query"
)

// Client is a querying client for DynamoDB that enables automatic index selection.
// The client caches table metadata to optimize calls on previously-queried tables.
type Client struct {
	dynamodbService dynamodbiface.DynamoDBAPI

	metadataProvider TableMetadataProvider

	// TODO: cache table metadata
	tableIndexMetadataCache map[string]*TableMetadata
}

// NewClient creates a new Client instance.
func NewClient(service dynamodbiface.DynamoDBAPI) *Client {
	return NewClientWithMetadataProvider(service, newDescribeMetadataProvider(service))
}

// NewClientWithMetadataProvider creates a new Client instance with a specified metadata provider.
//
// Specifying alternate metadata providers is an advanced feature.
// Most users should use NewClient when creating an auto-querying client, which uses DescribeTable
// as the metadata provider.
//
// An alternative TableMetadataProvider may be needed in cases where the table cannot be described
// using DescribeTable.
func NewClientWithMetadataProvider(
	service dynamodbiface.DynamoDBAPI, provider TableMetadataProvider) *Client {
	return &Client{
		dynamodbService:         service,
		metadataProvider:        newDescribeMetadataProvider(service),
		tableIndexMetadataCache: map[string]*TableMetadata{},
	}
}

// NewQuery initializes a query on a table using automatic index selection.
// On the first call to a new table, the client will populate the table's index metadata using
// a DescribeTable call. The executing role must have permissions to describe the table, or this
// call will return an error.
func (client *Client) NewQuery(ctx context.Context,
	tableName string, expr *query.Expression) (*query.Parser, error) {

	// TODO: implement
	return nil, fmt.Errorf("not yet implemented")
}
