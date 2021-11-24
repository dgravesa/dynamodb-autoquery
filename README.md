# dynamodb-autoquery
DynamoDB querying with automatic index selection and a friendly interface.

## Installation

```
go get -v github.com/dgravesa/dynamodb-autoquery
```

## About

`autoquery` leverages table index metadata provided through a DescribeTable call (or optionally, another metadata provider)
to automatically select a query index based on viability and scoring against a particular query expression.

## Executing a query

1) Initialize an `autoquery.Client` instance using a DynamoDB service instance.

```go
svc := dynamodb.New(session.New())
aq := autoquery.NewClient(svc)
```

2) Build an `autoquery.Expression`, and initialize a new query parser using the client.

```go
expr := autoquery.Key("director").Equal("Clint Eastwood").
    And("title").BeginsWith("The ").
    OrderBy("rating", false). // high to low
    Select("title", "year", "rating")

tableName := "Movies"
parser := aq.Query(tableName, expr)
```

3) Parse result items using the `autoquery.Parser` instance.

```go
var movie struct {
    Title  string  `dynamodbav:"title"`
    Year   int     `dynamodbav:"year"`
    Rating float64 `dynamodbav:"rating"`
}

var err error
maxPrintCount := 10
for count := 0; count < maxPrintCount; count++ {
    // parse movie items until max count is reached or all have been parsed
    err = parser.Next(context.Background(), &movie)
    if err != nil {
        break
    }

    fmt.Printf("Year: %d, Title: %s, Rating: %.1f\n", movie.Year, movie.Title, movie.Rating)
}

// check error type
switch err.(type) {
case nil:
    break
case *autoquery.ErrParsingComplete:
    // all query items have been parsed
    fmt.Println(err)
default:
    fmt.Println("unexpected error:", err)
}
```

## Viability rules for index selection

In order for a given expression to be executed on a table, at least one index must meet all of the following criteria:

* The partition key attribute of the index must be used in an `Equal` condition in the expression.
* If an `OrderBy` attribute is specified on the expression, then the index must have the same attribute as its sort key.
* If the expression contains a `Select` clause,
then the index must include all selected attributes in its projection or project all attributes.
* If the expression does not select attributes, then the index must project all attributes.
* If the index is considered sparse (see below), then both the partition key and sort key attributes must appear in the expression.
The sort key attribute may appear as the `OrderBy` clause for the index to be considered viable.
It is not sufficient for the attribute to appear only in the `Select` clause.
* If the expression specifies `ConsistentRead(true)`, then the index must not be a global secondary index.

In general, `autoquery` should not be expected as a means of enabling full SQL-like flexibility.
The expression capability still depends on the indexes defined for a table.
Expression builds should be controlled in ways that guarantee there will be viable indexes for any expression build,
such as requiring at least one attribute from a set of index partition keys.

## Index Sparsity (advanced)

Sparse indexes do not contain all entries in the table.
Consequently, indexes that are non-sparse are considered viable for a wider range of expressions
as they only require the partition key attribute to appear in the expression as an `Equal` condition.
However, sparse indexes may be preferred when they are viable as they will generally return all results while evaluating fewer items.

By default, the primary table index is considered non-sparse and all secondary indexes are considered sparse, with one exception\*.
This behavior can be modified at the client level by setting `Client.SecondaryIndexSparsenessThreshold`.
If this threshold value is set to 1.0, then any secondary indexes whose size matches the total size of the table (at the time the metadata is queried) is considered non-sparse,
and all other secondary indexes will be considered sparse, with one exception\*.
If this value is set between 0.0 and 1.0, then any index whose ratio of index items to total table items is greater than or equal to the threshold will be considered non-sparse,
and all other secondary indexes will be considered sparse, with one exception\*.
If the value is set to 0.0 or less, then all indexes will be considered non-sparse.

\*There is one exception to secondary index sparseness: since the primary table index attributes must be present in all items, any secondary index which uses either the table's primary partition key or primary sort key as its own sort key will always be non-sparse for purposes of index selection.

It is possible for the metadata and sparseness classification to become stale if items are added to the table which do not contain the secondary index's sort key attribute.
If unsure about which indexes may be considered non-sparse, then it is recommended not to change `SecondaryIndexSparsenessThreshold`.
