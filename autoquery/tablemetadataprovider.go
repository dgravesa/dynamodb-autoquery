package autoquery

import "context"

// TableMetadataProvider is used to gather index metadata on DynamoDB tables.
type TableMetadataProvider interface {
	FetchMetadata(ctx context.Context, tableName string) (*TableMetadata, error)
}
