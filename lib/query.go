package lib

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	csvmap "github.com/recursionpharma/go-csv-map"
	"github.com/sirupsen/logrus"
	"github.com/xuri/excelize/v2"
)

// Query ...
type Query struct {
	OutputFile         string
	QueryResultsBucket string
	QueryResultsPrefix string
	Database           string
	SQL                string
	Format             string
	JMESPath           string
	Statistics         bool
	WorkGroup          string
}

// Format is an enumeration of available query output formats
// ENUM(
// json, jsonl, csv, table, tsv, xlsx
// )
type Format int

// Execute a SQL query against Athena
func (q *Query) Execute() (*os.File, error) {
	// Check to see if `--sql` points to a file
	if _, err := os.Stat(q.SQL); err == nil {
		queryFromFile, err := ioutil.ReadFile(q.SQL)
		if err != nil {
			return nil, fmt.Errorf("unable to read query from file %s", q.SQL)
		}
		q.SQL = string(queryFromFile)
	}

	startQueryExecutionInput := athena.StartQueryExecutionInput{
		QueryString: &q.SQL,
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: &q.Database,
		},
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: aws.String("s3://" + path.Join(q.QueryResultsBucket, q.QueryResultsPrefix)),
		},
	}

	if q.WorkGroup != "" {
		startQueryExecutionInput.WorkGroup = aws.String(q.WorkGroup)
	}

	result, err := svc.StartQueryExecution(&startQueryExecutionInput)
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
			"Data Scanned: %s\nExecution Time: %s\n",
			humanize.Bytes(uint64(*qrop.QueryExecution.Statistics.DataScannedInBytes)),
			humanizeDuration(time.Duration(*qrop.QueryExecution.Statistics.TotalExecutionTimeInMillis)*time.Millisecond),
		))
	}

	if *qrop.QueryExecution.Status.State == "SUCCEEDED" {

		file, err := ioutil.TempFile("", "athena-query-results-"+*result.QueryExecutionId)
		if err != nil {
			return nil, fmt.Errorf("unable to create temp file %q, %v", *result.QueryExecutionId, err)
		}

		numBytes, err := downloader.Download(file, &s3.GetObjectInput{
			Bucket: aws.String(q.QueryResultsBucket),
			Key:    aws.String(*result.QueryExecutionId + ".csv"),
		})
		if err != nil {
			if aerr, ok := err.(awserr.Error); ok {
				switch aerr.Code() {
				case s3.ErrCodeNoSuchBucket:
					return nil, fmt.Errorf("unable to download query results for %q. Bucket %s does not exist", *result.QueryExecutionId, q.QueryResultsBucket)
				case s3.ErrCodeNoSuchKey:
					return nil, nil
				default:
					return nil, fmt.Errorf("unable to download query results for %q, %v", *result.QueryExecutionId, err)
				}
			}
		}

		logrus.Debugf("results cached to disk %s (%d bytes)", file.Name(), numBytes)

		return file, nil
	}

	return nil, fmt.Errorf("query state: %s\n\t%s", *qrop.QueryExecution.Status.State, *qrop.QueryExecution.Status.StateChangeReason)
}

// RenderQueryResults formats query results a in the
// desired format and sends to stdout
func (q *Query) RenderQueryResults(file *os.File) error {
	var err error

	var outFile *os.File

	if q.OutputFile == "" {
		outFile = os.Stdout
	} else {
		outFile, err = os.Create(q.OutputFile)
		if err != nil {
			return err
		}
		defer outFile.Close()
	}

	writer := bufio.NewWriter(outFile)
	defer writer.Flush()

	if q.Format == FormatJson.String() {
		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		output, err := json.MarshalIndent(records, "", "  ")
		if err != nil {
			return fmt.Errorf("Unable to marshal json %v", err)
		}

		writer.Write(output)
		return nil

	}

	if q.Format == FormatJsonl.String() {

		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
			}

			output, err := json.Marshal(record)
			if err != nil {
				return fmt.Errorf("Unable to convert output to jsonl, %v", err)
			}
			writer.WriteString(string(output) + "\n")
		}

	}

	if q.Format == FormatTable.String() {
		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		table := tablewriter.NewWriter(outFile)
		table.SetHeader(reader.Columns)
		table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		table.SetCenterSeparator("|")

		for _, record := range records {
			table.Append(values(reader.Columns, record))
		}

		table.Render()

	}

	if q.Format == FormatCsv.String() {
		records, err := ioutil.ReadFile(file.Name())
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		writer.Write(records)
	}

	if q.Format == FormatTsv.String() {

		csvFile, err := os.Open(file.Name())
		if err != nil {
			fmt.Println(err)
		}
		defer csvFile.Close()

		reader := csv.NewReader(csvFile)
		w := csv.NewWriter(writer)
		defer w.Flush()

		w.Comma = '\t'

		for {
			record, err := reader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
			}

			if err := w.Write(record); err != nil {
				return fmt.Errorf("error writing record to output %v", err)
			}
		}

	}

	if q.Format == FormatXlsx.String() {

		sheetName := "Sheet1"
		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		f := excelize.NewFile()

		// populate the header row
		for i, value := range reader.Columns {
			col, _ := excelize.ColumnNumberToName(i + 1)
			f.SetCellStr(sheetName, fmt.Sprintf("%s%d", col, 1), value)
		}

		// populate the spreadsheet
		for row, record := range records {
			for i, value := range values(reader.Columns, record) {
				col, _ := excelize.ColumnNumberToName(i + 1)
				f.SetCellStr(sheetName, fmt.Sprintf("%s%d", col, row+2), value)
			}
		}

		// Make the first row bold
		headerStyle, err := f.NewStyle(`{"font":{"bold":true}}`)
		if err != nil {
			fmt.Println(err)
		}
		lastColumn, _ := excelize.ColumnNumberToName(len(reader.Columns))
		f.SetCellStyle(sheetName, "A1", fmt.Sprintf("%s%d", lastColumn, 1), headerStyle)

		// Ensure we write a .xlsx file
		if q.OutputFile == "" {
			q.OutputFile = "athena-query-results.xlsx"
		}

		// Save spreadsheet by the given path.
		if err := f.SaveAs(q.OutputFile); err != nil {
			fmt.Println(err)
			return err
		}

	}
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
