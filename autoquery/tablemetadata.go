package autoquery

// TableMetadata includes all necessary metadata for table auto-querying.
type TableMetadata struct {
	Indexes map[string]*TableIndex
}
