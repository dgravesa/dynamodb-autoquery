package autoquery

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Client is a querying client for DynamoDB that enables automatic index selection.
// The client caches table metadata to optimize calls on previously-queried tables.
type Client struct {
	dynamodbService dynamodbiface.DynamoDBAPI

	metadataProvider TableDescriptionProvider

	// TODO: cache table metadata
	tableIndexMetadataCache map[string]*tableIndexMetadata
}

// NewClient creates a new Client instance.
func NewClient(service dynamodbiface.DynamoDBAPI) *Client {
	return NewClientWithMetadataProvider(service, newDefaultDescriptionProvider(service))
}

// NewClientWithMetadataProvider creates a new Client instance with a specified metadata provider.
//
// Specifying alternate metadata providers is an advanced feature.
// Most users should use NewClient when creating an auto-querying client, which uses DescribeTable
// as the metadata provider.
//
// An alternative TableDescriptionProvider may be needed in cases where the table cannot be
// described using DescribeTable.
func NewClientWithMetadataProvider(
	service dynamodbiface.DynamoDBAPI, provider TableDescriptionProvider) *Client {
	return &Client{
		dynamodbService:         service,
		metadataProvider:        provider,
		tableIndexMetadataCache: map[string]*tableIndexMetadata{},
	}
}

// NewQuery initializes a query on a table using automatic index selection.
// On the first call to a new table, the client will populate the table's index metadata using
// the underlying metadata provider.
func (client *Client) NewQuery(ctx context.Context,
	tableName string, expr *Expression) (*Parser, error) {

	// pull metadata from cache
	indexMetadata, err := client.pullIndexMetadata(ctx, tableName)
	if err != nil {
		return nil, err
	}

	fmt.Println("TODO remove", indexMetadata)

	// TODO: implement
	return nil, fmt.Errorf("not yet implemented")
}

func (client *Client) pullIndexMetadata(
	ctx context.Context, tableName string) (*tableIndexMetadata, error) {

	indexMetadata, found := client.tableIndexMetadataCache[tableName]
	if !found {
		// attempt to pull table description from metadata provider
		tableDescription, err := client.metadataProvider.Get(ctx, tableName)
		if err != nil {
			return nil, err
		}
		indexMetadata = parseTableIndexMetadata(tableDescription)
		// add metadata to cache
		client.tableIndexMetadataCache[tableName] = indexMetadata
	}

	return indexMetadata, nil
}
