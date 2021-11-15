package autoquery

// TableMetadataProvider is used to gather index metadata on DynamoDB tables.
type TableMetadataProvider interface {
	FetchMetadata(tableName string) (*TableMetadata, error)
}
