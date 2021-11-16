package autoquery

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// TableDescriptionProvider is used to gather DynamoDB table metadata.
type TableDescriptionProvider interface {
	Get(ctx context.Context, tableName string) (*dynamodb.TableDescription, error)
}
