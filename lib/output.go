package lib

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/360EntSecGroup-Skylar/excelize/v2"
	"github.com/gocarina/gocsv"
	"github.com/olekukonko/tablewriter"
)

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
	fmt.Print(string(data))
	return nil

}

// RenderAsJSON will render an interface as json
func RenderAsJSON(i interface{}) error {

	data, err := json.MarshalIndent(i, "", "  ")

	if err != nil {
		return err
	}

	fmt.Print(string(data))
	return nil

}

// RenderAsXLSX will render an interface as XLSX
func RenderAsXLSX(i interface{}) error {
	f := excelize.NewFile()
	// Create a new sheet.
	index := f.NewSheet("Sheet2")
	// Set value of a cell.
	f.SetCellValue("Sheet2", "A2", "Hello world.")
	f.SetCellValue("Sheet1", "B2", 100)
	// Set active sheet of the workbook.
	f.SetActiveSheet(index)
	// Save spreadsheet by the given path.
	if err := f.SaveAs("Book1.xlsx"); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
