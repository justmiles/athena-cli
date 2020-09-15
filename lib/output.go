package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/gocarina/gocsv"
	"github.com/olekukonko/tablewriter"
	csvmap "github.com/recursionpharma/go-csv-map"
)

// RenderQueryResults formats query results a in the
// desired format and sends to stdout
func RenderQueryResults(outputFormat string, query string, file *os.File) error {
	var err error

	if outputFormat == "json" {

		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)

		}

		output, _ := json.MarshalIndent(records, "", "  ")

		fmt.Println(string(output))
	}

	if outputFormat == "table" {
		reader := csvmap.NewReader(file)
		reader.Columns, err = reader.ReadHeader()
		if err != nil {
			return fmt.Errorf("Unable to read header from %q, %v", file.Name(), err)
		}

		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(reader.Columns)
		table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
		table.SetCenterSeparator("|")

		for _, record := range records {
			table.Append(values(reader.Columns, record))
		}

		table.Render()

	}

	if outputFormat == "csv" {

		sb, err := ioutil.ReadFile(file.Name())
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)
		}

		fmt.Println(string(sb))

	}
	return nil
}

func values(c []string, m map[string]string) []string {
	values := make([]string, 0, len(m))

	for _, k := range c {
		values = append(values, m[k])
	}

	return values
}

// RenderAsTable will render an interfact to table.
func RenderAsTable(i interface{}) error {

	data, _ := json.Marshal(i)

	var d []map[string]interface{}
	err := json.Unmarshal(data, &d)

	if err != nil {
		return err
	}

	if len(d) == 0 {
		return nil
	}

	table := tablewriter.NewWriter(os.Stdout)

	var headers []string
	for key := range d[0] {
		headers = append(headers, key)
	}
	table.SetHeader(headers)
	table.SetBorders(tablewriter.Border{Left: true, Top: false, Right: true, Bottom: false})
	table.SetCenterSeparator("|")

	for _, record := range d {
		var row []string
		for _, key := range headers {
			row = append(row, fmt.Sprintln(record[key]))
		}
		table.Append(row)
	}

	table.Render()
	return nil

}

// RenderAsCSV will render an interfact to table
func RenderAsCSV(i interface{}) error {

	data, err := gocsv.MarshalBytes(i)
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil

}

// RenderAsJSON will render an interface as json
func RenderAsJSON(i interface{}) error {

	data, err := json.MarshalIndent(i, "", "  ")

	if err != nil {
		return err
	}

	fmt.Println(string(data))
	return nil

}
