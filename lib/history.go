package lib

import (
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
)

// HistoricalExecution ..
type HistoricalExecution struct {
	Query                      string
	Catalog                    string
	Database                   string
	QueryExecutionID           string
	OutputLocation             string
	State                      string
	WorkGroup                  string
	TotalExecutionTimeInMillis int64
	DataScannedInBytes         int64
	Cost                       float64
	SubmissionDateTime         time.Time
}

// ListHistory returns query results per workgroup
func ListHistory(maxResults int) (h []HistoricalExecution, err error) {
	params := &athena.ListQueryExecutionsInput{
		// WorkGroup
	}

	if maxResults < 50 {
		params.MaxResults = aws.Int64(int64(maxResults))
	}

	err = svc.ListQueryExecutionsPages(params, func(page *athena.ListQueryExecutionsOutput, lastPage bool) bool {
		h = append(h, getQueryExecution(page.QueryExecutionIds)...)
		return len(h) <= maxResults
	})
	if err != nil {
		return h, err
	}

	if len(h) > maxResults {
		return h[0:maxResults], nil
	}
	return h, nil
}

func getQueryExecution(ids []*string) (h []HistoricalExecution) {
	o, err := svc.BatchGetQueryExecution(&athena.BatchGetQueryExecutionInput{
		QueryExecutionIds: ids,
	})

	if err != nil {
		log.Fatal(err)
	}

	// fmt.Println(o.GoString(), err)
	for _, e := range o.QueryExecutions {

		he := HistoricalExecution{
			Query:                      *e.Query,
			QueryExecutionID:           *e.QueryExecutionId,
			OutputLocation:             *e.ResultConfiguration.OutputLocation,
			State:                      *e.Status.State,
			WorkGroup:                  *e.WorkGroup,
			TotalExecutionTimeInMillis: *e.Statistics.TotalExecutionTimeInMillis,
			SubmissionDateTime:         *e.Status.SubmissionDateTime,
		}

		if e.Statistics.DataScannedInBytes != nil {
			he.DataScannedInBytes = *e.Statistics.DataScannedInBytes
			he.Cost = float64((float64(*e.Statistics.DataScannedInBytes) / 1024 / 1024 / 1024 / 1024) * 5)
		}

		if e.QueryExecutionContext.Database != nil {
			he.Database = *e.QueryExecutionContext.Database
		}

		if e.QueryExecutionContext.Catalog != nil {
			he.Catalog = *e.QueryExecutionContext.Catalog
		} else {
			he.Catalog = "AwsDataCatalog"
		}

		h = append(h, he)
	}

	return h
}

// RenderHistoryResults ..
func RenderHistoryResults(h []HistoricalExecution, outputFormat string) error {
	switch outputFormat {
	case "csv":
		return RenderAsCSV(h)
	case "table":
		return RenderAsTable(h)
	default:
		return RenderAsJSON(h)
	}
}
