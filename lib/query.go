package lib

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/sirupsen/logrus"
)

// Query ...
type Query struct {
	OutputBucket string
	OutputPrefix string
	Database     string
	SQL          string
	Format       string
	JMESPath     string
	Statistics   bool
}

// Format is an enumeration of available query output formats
// ENUM(
// json, csv, table
// )
type Format int

// Execute a SQL query against Athena
func (q *Query) Execute() (*os.File, error) {

	result, err := svc.StartQueryExecution(&athena.StartQueryExecutionInput{
		QueryString: &q.SQL,
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: &q.Database,
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://" + path.Join(q.OutputBucket, q.OutputPrefix)),
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

	// Wait until query finishes
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

	if q.Statistics {
		println(fmt.Sprintf(
			"Data Scanned: %d\nExecution Time: %d\n",
			*qrop.QueryExecution.Statistics.DataScannedInBytes,
			*qrop.QueryExecution.Statistics.TotalExecutionTimeInMillis,
		))
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
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					return nil, fmt.Errorf("Unable to download query results for %q. Bucket %s does not exist", *result.QueryExecutionId, q.OutputBucket)
				case s3.ErrCodeNoSuchKey:
					return nil, nil
				default:
					return nil, fmt.Errorf("Unable to download query results for %q, %v", *result.QueryExecutionId, err)
				}
			}
		}

		logrus.Debugf("results cached to disk %s (%d bytes)", file.Name(), numBytes)

		return file, nil
	}

	return nil, fmt.Errorf("query state: %s\n\t%s", *qrop.QueryExecution.Status.State, *qrop.QueryExecution.Status.StateChangeReason)
}

// ExecuteToStdout executes the query and returns the results to stdout
func (q *Query) ExecuteToStdout() error {

	file, err := q.Execute()
	if err != nil {
		return err
	}

	// Clean up
	defer CleanCache(file.Name())

	OutputData(q.Format, q.JMESPath, file)

	return nil
}

// CleanCache deletes any tmp files used
func CleanCache(fileName string) {
	logrus.Debugf("deleting cached query results %s", fileName)
	err := os.Remove(fileName)
	if err != nil {
		logrus.Debugf("error deleting cached query results %s, %v", fileName, err)
	}
}
