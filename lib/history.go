package lib

import (
	"encoding/base64"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// HistoricalExecution ..
type HistoricalExecution struct {
	Query                      string  `parquet:"name=query, type=UTF8, encoding=PLAIN"`
	Catalog                    string  `parquet:"name=catalog, type=UTF8, encoding=PLAIN"`
	Database                   string  `parquet:"name=database, type=UTF8, encoding=PLAIN"`
	QueryExecutionID           string  `parquet:"name=queryexecutionid, type=UTF8, encoding=PLAIN"`
	OutputLocation             string  `parquet:"name=outputlocation, type=UTF8, encoding=PLAIN"`
	State                      string  `parquet:"name=state, type=UTF8, encoding=PLAIN"`
	WorkGroup                  string  `parquet:"name=workgroup, type=UTF8, encoding=PLAIN"`
	TotalExecutionTimeInMillis int64   `parquet:"name=total_execution_time_in_millis, type=INT64"`
	DataScannedInBytes         int64   `parquet:"name=data_scanned_in_bytes, type=INT64"`
	Cost                       float64 `parquet:"name=cost, type=DOUBLE"`
	SubmissionDateTime         time.Time
}

// ListHistory returns query results per workgroup
func ListHistory(maxResults int, historyCmdWorkgroup string) (h []HistoricalExecution, err error) {
	params := &athena.ListQueryExecutionsInput{
		WorkGroup: aws.String(historyCmdWorkgroup),
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
			Query:                      base64.StdEncoding.EncodeToString([]byte(*e.Query)),
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

// RenderHistoricalExecutionAsParquet will render an interface to parquet.
func RenderHistoricalExecutionAsParquet(h []HistoricalExecution) error {
	const outputFilename = "output.parquet"
	var err error
	fw, err := local.NewLocalFileWriter(outputFilename)
	if err != nil {
		return fmt.Errorf("Can't create local file: %s", err)
	}

	//write
	// n := reflect.TypeOf(iface)
	pw, err := writer.NewParquetWriter(fw, new(HistoricalExecution), 4)
	if err != nil {
		return fmt.Errorf("Can't create parquet writer: %s", err)
	}
	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.PageSize = 8 * 1024              //8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	for _, ss := range h {
		if err = pw.Write(ss); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		return fmt.Errorf("WriteStop error: %s", err)
	}
	fmt.Printf("Parquet file written to %s\n", outputFilename)
	fw.Close()

	return nil
}

// RenderHistoryResults ..
func RenderHistoryResults(h []HistoricalExecution, outputFormat string) error {
	switch outputFormat {
	case "csv":
		return RenderAsCSV(h)
	case "table":
		return RenderAsTable(h)
	case "parquet":
		return RenderHistoricalExecutionAsParquet(h)
	default:
		return RenderAsJSON(h)
	}
}
