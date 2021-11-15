package autoquery

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type describeMetadataProvider struct{}

func newDescribeMetadataProvider(service dynamodbiface.DynamoDBAPI) *describeMetadataProvider {
	// TODO: implement
	return nil
}

func (p *describeMetadataProvider) FetchMetadata(
	ctx context.Context, tableName string) (*TableMetadata, error) {
	// TODO: implement
	return nil, fmt.Errorf("not yet implemented")
}
