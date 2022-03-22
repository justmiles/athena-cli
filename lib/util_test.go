package lib

import (
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/athena/athenaiface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
)

// Define a mock struct to be used in your unit tests of myFunc.
type mockAthenaClient struct {
	athenaiface.AthenaAPI
}

func (m *mockAthenaClient) StartQueryExecution(input *athena.StartQueryExecutionInput) (*athena.StartQueryExecutionOutput, error) {
	// mock response/functionality
	return &athena.StartQueryExecutionOutput{
		QueryExecutionId: aws.String("xxxyyyzzz"),
	}, nil
}

func (m *mockAthenaClient) GetQueryExecution(input *athena.GetQueryExecutionInput) (*athena.GetQueryExecutionOutput, error) {
	// mock response/functionality
	return &athena.GetQueryExecutionOutput{
		QueryExecution: &athena.QueryExecution{
			QueryExecutionId: aws.String("xxxyyyzzz"),
			Status: &athena.QueryExecutionStatus{
				State: aws.String(athena.QueryExecutionStateSucceeded),
			},
		},
	}, nil
}

// mock the downloader
type mockDownloaderAPI struct {
	s3manageriface.DownloaderAPI
}

func (m *mockDownloaderAPI) Download(io.WriterAt, *s3.GetObjectInput, ...func(*s3manager.Downloader)) (int64, error) {
	return 100, nil
}
