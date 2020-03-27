package lib

import (
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	csvmap "github.com/recursionpharma/go-csv-map"
	"github.com/sirupsen/logrus"
)

// Query ...
type Query struct {
	OutputBucket string
	Database     string
	SQL          string
}

// Execute a SQL query against Athena
func (q *Query) Execute() (*os.File, error) {

	result, err := svc.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryString: &q.SQL,
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: &q.Database,
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://" + q.OutputBucket),
		},
	})

	if err != nil {
		return nil, err
	}

	queryExecutionInput := athena.GetQueryExecutionInput{
		QueryExecutionId: result.QueryExecutionId,
	}

	var qrop *athena.GetQueryExecutionOutput
	duration := time.Duration(2) * time.Second

	for {
		qrop, err = svc.GetQueryExecution(&queryExecutionInput)
		if err != nil {
			return nil, err
		}

		if *qrop.QueryExecution.Status.State == athena.QueryExecutionStateSucceeded || *qrop.QueryExecution.Status.State == athena.QueryExecutionStateFailed || *qrop.QueryExecution.Status.State == athena.QueryExecutionStateCancelled {
			break
		}

		logrus.Debugf("Query Execution Status: %s\n", *qrop.QueryExecution.Status.State)

		time.Sleep(duration)

	}

	if *qrop.QueryExecution.Status.State == "SUCCEEDED" {

		file, err := ioutil.TempFile("", "athena-query-results-"+*result.QueryExecutionId)
		if err != nil {
			return nil, fmt.Errorf("Unable to create temp file %q, %v", *result.QueryExecutionId, err)
		}

		downloader := s3manager.NewDownloader(sess)
		numBytes, err := downloader.Download(file, &s3.GetObjectInput{
			Bucket: aws.String(q.OutputBucket),
			Key:    aws.String(*result.QueryExecutionId + ".csv"),
		})

		if err != nil {
			return nil, fmt.Errorf("Unable to download query results for  %q, %v", *result.QueryExecutionId, err)
		}

		logrus.Debugf("results cached to disk %s (%d bytes)", file.Name(), numBytes)

		return file, nil
	}

	return nil, fmt.Errorf("query %s", *qrop.QueryExecution.Status.State)

}

// ExecuteToStdout executes the query and returns the results to stdout
func (q *Query) ExecuteToStdout() error {

	file, err := q.Execute()
	if err != nil {
		return err
	}

	// Clean up
	defer cleanCache(file.Name())

	sb, err := ioutil.ReadFile(file.Name())
	if err != nil {
		return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
	}

	fmt.Println(string(sb))

	return nil
}

// ExecuteToMap returns Athena query results as a map
func (q *Query) ExecuteToMap() ([]map[string]string, error) {
	file, err := q.Execute()
	if err != nil {
		return nil, err
	}

	// Clean up
	defer cleanCache(file.Name())

	reader := csvmap.NewReader(file)

	reader.Columns, err = reader.ReadHeader()
	if err != nil {
		return nil, err
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	return records, nil
}

func cleanCache(fileName string) {
	logrus.Debugf("deleting cached query results %s", fileName)
	err := os.Remove(fileName)
	if err != nil {
		logrus.Debugf("error deleting cached query results %s, %v", fileName, err)
	}
}
