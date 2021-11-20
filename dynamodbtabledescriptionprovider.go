package autoquery

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
)

type dynamoDBTableDescriptionProvider struct {
	dynamodbService dynamodbiface.DynamoDBAPI
}

func newDefaultDescriptionProvider(service dynamodbiface.DynamoDBAPI) *dynamoDBTableDescriptionProvider {
	return &dynamoDBTableDescriptionProvider{
		dynamodbService: service,
	}
}

func (p *dynamoDBTableDescriptionProvider) Get(
	ctx context.Context, tableName string) (*dynamodb.TableDescription, error) {

	// call DynamoDB to retrieve table description
	describeInput := &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	}
	describeOutput, err := p.dynamodbService.DescribeTableWithContext(ctx, describeInput)
	if err != nil {
		return nil, err
	}

	return describeOutput.Table, nil
}
