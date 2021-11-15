package autoquery

// TableIndex defines a single logical index of a table.
type TableIndex struct {
	Name                  string
	PartitionKey          string
	SortKey               string
	IsComposite           bool
	AttributeSet          map[string]struct{}
	IncludesAllAttributes bool
	Size                  int
	ConsistentReadable    bool
}
