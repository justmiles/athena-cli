package lib

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"

	"github.com/olekukonko/tablewriter"
	csvmap "github.com/recursionpharma/go-csv-map"
)

// OutputData formats json data as the desired output format
// and sends the results to stdout
func OutputData(outputFormat string, query string, file *os.File) error {

	if outputFormat == "json" {

		reader := csvmap.NewReader(file)
		reader.Columns, _ = reader.ReadHeader()
		records, err := reader.ReadAll()
		if err != nil {
			return fmt.Errorf("Unable to read query results from %q, %v", file.Name(), err)

		}

		output, _ := json.MarshalIndent(records, "", "  ")

		fmt.Println(string(output))
	}

	if outputFormat == "table" {

		reader := csvmap.NewReader(file)
		reader.Columns, _ = reader.ReadHeader()
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
