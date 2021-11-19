package autoquery

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

// Client is a querying client for DynamoDB that enables automatic index selection.
// The client caches table metadata to optimize calls on previously-queried tables.
type Client struct {
	dynamodbService dynamodbiface.DynamoDBAPI

	metadataProvider TableDescriptionProvider

	tableIndexMetadataCache map[string]*tableIndexMetadata

	// SecondaryIndexSparsenessThreshold sets the threshold for secondary indexes to be considered
	// sparse vs non-sparse.
	//
	// A sparse index is only viable with expressions that include conditions for both the
	// partition key (which must be an Equal condition) and the sort key.
	//
	// The table's primary index is always non-sparse and is viable with any expression that
	// includes an Equal condition on the partition key.
	//
	// When a table's metadata is gathered, if the ratio of number of items in the secondary index
	// to number of items in the table is greater than or equal to
	// SecondaryIndexSparsenessThreshold, then the index will be considered non-sparse for
	// purposes of index selection.
	//
	// If the SecondaryIndexSparsenessThreshold is set to a value less than or equal to 0.0, then
	// all secondary indexes will be considered non-sparse; if set to 1.0, then each secondary
	// index will be considered non-sparse if the number of items in the index matches the total
	// number of items in the table. If set to a value greater than 1.0, then all secondary
	// indexes will be considered sparse. If the table is empty when sparseness is determined,
	// then every secondary index will be considered sparse unless the threshold is 0.0 or less.
	//
	// By default, all secondary indexes are considered sparse. If non-default behavior is
	// desired, this value should be set before any queries are parsed with Parser.Next.
	SecondaryIndexSparsenessThreshold float64
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
		// by default, all secondary indexes are considered sparse
		SecondaryIndexSparsenessThreshold: 1.1,
	}
}

// NewQuery initializes a query defined by expr on a table. The returned parser may be used to
// retrieve items using Parser.Next.
//
// On the first call to a new table, the client will populate the table's index metadata using
// the underlying metadata provider. The metadata is cached for subsequent queries to the table
// through the same Client instance. The query automatically selects an index based on the table
// metadata and any expression restrictions.
func (client *Client) NewQuery(tableName string, expr *Expression) *Parser {
	return &Parser{
		client:        client,
		tableName:     tableName,
		expr:          expr,
		bufferedItems: []map[string]*dynamodb.AttributeValue{},
	}
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
		indexMetadata = client.parseTableIndexMetadata(tableDescription)
		// add metadata to cache
		client.tableIndexMetadataCache[tableName] = indexMetadata
	}

	return indexMetadata, nil
}

func (client *Client) parseTableIndexMetadata(table *dynamodb.TableDescription) *tableIndexMetadata {
	output := &tableIndexMetadata{
		Indexes: []*tableIndex{},
	}

	appendIndex := func(index *tableIndex) {
		output.Indexes = append(output.Indexes, index)
	}

	// extract primary key index
	tableSize := int(*table.ItemCount)
	tablePrimaryIndex := &tableIndex{
		Name:                  tablePrimaryIndexName,
		Size:                  tableSize,
		IncludesAllAttributes: true,
		ConsistentReadable:    true,
		IsSparse:              false,
		Sparsity:              1.0,
		SparsityMultiplier:    1.0,
	}
	tablePrimaryIndex.loadKeysFromSchema(table.KeySchema)
	appendIndex(tablePrimaryIndex)

	tablePrimaryIndexKeys := tablePrimaryIndex.getKeys()

	// extract global secondary indexes
	if table.GlobalSecondaryIndexes != nil {
		for _, gsi := range table.GlobalSecondaryIndexes {
			index := &tableIndex{
				Name: *gsi.IndexName,
				Size: int(*gsi.ItemCount),
				// global secondary indexes do not support consistent read
				ConsistentReadable: false,
			}
			index.loadKeysFromSchema(gsi.KeySchema)
			index.loadAttributesFromProjection(gsi.Projection, tablePrimaryIndexKeys)
			index.inferSparseness(tablePrimaryIndex, client.SecondaryIndexSparsenessThreshold)
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
				IsSparse:           true,
			}
			index.loadKeysFromSchema(lsi.KeySchema)
			index.loadAttributesFromProjection(lsi.Projection, tablePrimaryIndexKeys)
			index.inferSparseness(tablePrimaryIndex, client.SecondaryIndexSparsenessThreshold)
			appendIndex(index)
		}
	}

	return output
}

func (client *Client) chooseIndex(ctx context.Context,
	tableName string, expr *Expression) (*tableIndex, error) {

	// pull metadata from cache
	indexMetadata, err := client.pullIndexMetadata(ctx, tableName)
	if err != nil {
		return nil, err
	}

	var bestIndex *tableIndex
	bestIndexScore := 0.0

	// select index with best score based on the expression
	inviableErrs := []*ErrIndexNotViable{}
	for _, index := range indexMetadata.Indexes {
		indexScore, inviableErr := client.scoreIndexOnExpr(index, expr)
		if inviableErr != nil {
			inviableErrs = append(inviableErrs, inviableErr)
		} else if indexScore > bestIndexScore {
			bestIndex = index
			bestIndexScore = indexScore
		}
	}

	// no viable indexes found
	if bestIndex == nil {
		return nil, &ErrNoViableIndexes{IndexErrs: inviableErrs}
	}

	return bestIndex, nil
}

func (client *Client) scoreIndexOnExpr(
	index *tableIndex, expr *Expression) (float64, *ErrIndexNotViable) {

	indexNotViableReasons := client.listIndexViabilityInfractions(index, expr)
	if len(indexNotViableReasons) > 0 {
		return 0.0, &ErrIndexNotViable{
			IndexName:        index.Name,
			NotViableReasons: indexNotViableReasons,
		}
	}

	// Every viable index should return the same values (unless sparseness threshold is reduced).
	// Remaining indexes should be scored with a reasonable best guess that puts the majority of
	// the filtering on the partition and sort keys of the index.

	// Viable sparse indexes are generally better than viable non-sparse indexes since the items
	// are already filtered by the sparsity of the index, so viable indexes with fewer items are
	// generally preferable.
	if index.HasMaxSparsityMultiplier {
		// if index is viable and has zero sparsity, then it suggests the expression has zero
		// result items.
		// TODO: if index starts out with zero items but gains items over a Client instance's
		// lifetime, then the index size metadata will become outdated and may lead to non-optimal
		// index selection. Consider metadata cache invalidation after some time.
		return math.MaxFloat64, nil
	}

	// Some expression conditions may filter items more quickly than others. Equal conditions are
	// the most restrictive. Between and prefix conditions are typically more restrictive than
	// less than (equal) or greater than (equal) conditions.
	defaultFilterTypeScore := 1.0
	sortKeyFilterTypeScoreMap := map[reflect.Type]float64{
		reflect.TypeOf(&equalsFilter{}):     2.5, // equals filter is 2.5x preferred
		reflect.TypeOf(&betweenFilter{}):    1.8, // between filter is 1.8x preferred
		reflect.TypeOf(&beginsWithFilter{}): 1.5, // prefix filter is 1.5x preferred
		reflect.TypeOf(nil):                 0.2, // no filter on sort key is not preferable
	}
	var exprSortKeyFilter conditionFilter = nil
	if index.IsComposite {
		exprSortKeyFilter = expr.filters[index.SortKey]
	}
	sortKeyFilterTypeScore, found := sortKeyFilterTypeScoreMap[reflect.TypeOf(exprSortKeyFilter)]
	if !found {
		sortKeyFilterTypeScore = defaultFilterTypeScore
	}

	indexScore := index.SparsityMultiplier * sortKeyFilterTypeScore

	return indexScore, nil
}

func (client *Client) listIndexViabilityInfractions(
	index *tableIndex, expr *Expression) []string {

	notViableReasons := []string{}

	// for index to be viable, there must be an equals filter on the index's partition key
	if !typesMatch(expr.filters[index.PartitionKey], &equalsFilter{}) {
		reason := fmt.Sprintf(
			"expression does not contain an equals condition on attribute: %s",
			index.PartitionKey)
		notViableReasons = append(notViableReasons, reason)
	}

	// if consistent read is specified, index must be consistent-readable
	if expr.consistentRead && !index.ConsistentReadable {
		notViableReasons = append(notViableReasons,
			"global secondary index does not support consistent read")
	}

	// if order is specified, index must sort on that attribute
	if expr.orderSpecified && expr.orderAttribute != index.SortKey {
		reason := fmt.Sprintf(
			"expression specifies order, so it requires an index with sort key: %s",
			expr.orderAttribute)
		notViableReasons = append(notViableReasons, reason)
	}

	// index must include selected attributes, or project all attributes if not specified
	if !index.IncludesAllAttributes {
		if expr.attributesSpecified {
			indexMissingAttrs := []string{}
			for _, selectedAttr := range expr.attributes {
				if _, found := index.AttributeSet[selectedAttr]; !found {
					indexMissingAttrs = append(indexMissingAttrs, selectedAttr)
				}
			}
			if len(indexMissingAttrs) > 0 {
				reason := fmt.Sprintf("index does not include attributes: %s",
					strings.Join(indexMissingAttrs, ", "))
				notViableReasons = append(notViableReasons, reason)
			}
		} else {
			notViableReasons = append(notViableReasons,
				"expression does not select attributes, so it requires an index that projects all")
		}
	}

	// if index is sparse, then both partition and sort attributes must appear in expression
	if index.IsSparse {
		// equals condition on partition key takes precedence, so only need to check sort key
		_, sortKeyInFilters := expr.filters[index.SortKey]
		if !sortKeyInFilters && expr.orderAttribute != index.SortKey {
			reason := fmt.Sprintf(
				"expression does not filter on sparse secondary index's sort key: %s",
				index.SortKey)
			notViableReasons = append(notViableReasons, reason)
		}
	}

	return notViableReasons
}
