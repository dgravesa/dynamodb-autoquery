package autoquery

import (
	"math"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

const tablePrimaryIndexName = "#primary"

type tableIndex struct {
	Name                  string
	PartitionKey          string
	SortKey               string
	IsComposite           bool
	AttributeSet          map[string]struct{}
	IncludesAllAttributes bool
	Size                  int
	ConsistentReadable    bool

	IsSparse                 bool
	Sparsity                 float64
	SparsityMultiplier       float64
	HasMaxSparsityMultiplier bool
}

func (index *tableIndex) loadKeysFromSchema(keySchema []*dynamodb.KeySchemaElement) {
	index.IsComposite = false
	for _, keyElement := range keySchema {
		switch *keyElement.KeyType {
		case "HASH":
			index.PartitionKey = *keyElement.AttributeName
		case "RANGE":
			index.SortKey = *keyElement.AttributeName
			index.IsComposite = true
		}
	}
}

func (index tableIndex) getKeys() []string {
	if index.IsComposite {
		return []string{index.PartitionKey, index.SortKey}
	}
	return []string{index.PartitionKey}
}

func (index *tableIndex) loadAttributesFromProjection(
	projection *dynamodb.Projection, tablePrimaryIndexKeys []string) {

	if projection == nil || *projection.ProjectionType == "ALL" {
		index.IncludesAllAttributes = true
	} else {
		index.IncludesAllAttributes = false
		index.AttributeSet = map[string]struct{}{}
		// include keys
		for _, key := range append(index.getKeys(), tablePrimaryIndexKeys...) {
			index.AttributeSet[key] = struct{}{}
		}
		// include additional specified attributes
		if *projection.ProjectionType == "INCLUDE" {
			for _, attribute := range projection.NonKeyAttributes {
				index.AttributeSet[*attribute] = struct{}{}
			}
		}
	}
}

func (index *tableIndex) inferSparseness(primaryTableIndex *tableIndex, threshold float64) {
	tableSize := primaryTableIndex.Size
	if tableSize == 0 {
		// special case, assume index has no sparsity benefit used for index selection
		index.Sparsity = 0.0
		index.SparsityMultiplier = 1.0
	} else if index.Size == 0 {
		// index has no items, any expression for which it is viable suggests no items will be
		// returned
		index.Sparsity = 0.0
		index.SparsityMultiplier = math.MaxFloat64
		index.HasMaxSparsityMultiplier = true
	} else {
		index.Sparsity = float64(index.Size) / float64(tableSize)
		index.SparsityMultiplier = float64(tableSize) / float64(index.Size)
	}

	// determine if index should be considered sparse vs non-sparse
	if !index.IsComposite {
		index.IsSparse = false
	} else {
		// an index should not be considered sparse if its sort key is a primary table index key
		sortKeyIsPrimaryTableKey := (index.SortKey == primaryTableIndex.PartitionKey) ||
			(primaryTableIndex.IsComposite && index.SortKey == primaryTableIndex.SortKey)
		index.IsSparse = !sortKeyIsPrimaryTableKey && index.Sparsity < threshold
	}
}
