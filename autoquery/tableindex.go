package autoquery

import "github.com/aws/aws-sdk-go/service/dynamodb"

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
	IsSparse              bool
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

func (index *tableIndex) loadAttributesFromProjection(projection *dynamodb.Projection, tablePrimaryIndexKeys []string) {
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

func (index *tableIndex) inferSparseness(tableSize int, threshold float64) {
	if !index.IsComposite {
		index.IsSparse = false
	} else {
		var sparsenessRatio float64
		if tableSize == 0 {
			sparsenessRatio = 0.0
		} else {
			sparsenessRatio = float64(index.Size) / float64(tableSize)
		}
		index.IsSparse = (sparsenessRatio < threshold)
	}
}
