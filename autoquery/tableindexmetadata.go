package autoquery

import (
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type tableIndexMetadata struct {
	Indexes []*tableIndex
}

func parseTableIndexMetadata(table *dynamodb.TableDescription) *tableIndexMetadata {
	output := &tableIndexMetadata{
		Indexes: []*tableIndex{},
	}

	appendIndex := func(index *tableIndex) {
		output.Indexes = append(output.Indexes, index)
	}

	// extract primary key index
	tablePrimaryIndex := &tableIndex{
		Name:                  tablePrimaryIndexName,
		Size:                  int(*table.ItemCount),
		IncludesAllAttributes: true,
		ConsistentReadable:    true,
	}
	tablePrimaryIndex.loadKeysFromSchema(table.KeySchema)
	appendIndex(tablePrimaryIndex)

	tablePrimaryIndexKeys := tablePrimaryIndex.getKeys()

	// extract global secondary indexes
	if table.GlobalSecondaryIndexes != nil {
		for _, gsi := range table.GlobalSecondaryIndexes {
			index := &tableIndex{
				Name:               *gsi.IndexName,
				Size:               int(*gsi.ItemCount),
				ConsistentReadable: false, // global secondary indexes do not support consistent read
			}
			index.loadKeysFromSchema(gsi.KeySchema)
			index.loadAttributesFromProjection(gsi.Projection, tablePrimaryIndexKeys)
			appendIndex(index)
		}
	}

	// extract local secondary indexes
	if table.LocalSecondaryIndexes != nil {
		for _, lsi := range table.LocalSecondaryIndexes {
			index := &tableIndex{
				Name:               *lsi.IndexName,
				Size:               int(*lsi.ItemCount),
				ConsistentReadable: true,
			}
			index.loadKeysFromSchema(lsi.KeySchema)
			index.loadAttributesFromProjection(lsi.Projection, tablePrimaryIndexKeys)
			appendIndex(index)
		}
	}

	return output
}
