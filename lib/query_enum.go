// Code generated by go-enum DO NOT EDIT.
// Version:
// Revision:
// Build Date:
// Built By:

package lib

import (
	"fmt"
)

const (
	// FormatJson is a Format of type Json.
	FormatJson Format = iota
	// FormatCsv is a Format of type Csv.
	FormatCsv
	// FormatTable is a Format of type Table.
	FormatTable
	// FormatTsv is a Format of type Tsv.
	FormatTsv
	// FormatXlsx is a Format of type Xlsx.
	FormatXlsx
)

const _FormatName = "jsoncsvtabletsvxlsx"

var _FormatMap = map[Format]string{
	FormatJson:  _FormatName[0:4],
	FormatCsv:   _FormatName[4:7],
	FormatTable: _FormatName[7:12],
	FormatTsv:   _FormatName[12:15],
	FormatXlsx:  _FormatName[15:19],
}

// String implements the Stringer interface.
func (x Format) String() string {
	if str, ok := _FormatMap[x]; ok {
		return str
	}
	return fmt.Sprintf("Format(%d)", x)
}

var _FormatValue = map[string]Format{
	_FormatName[0:4]:   FormatJson,
	_FormatName[4:7]:   FormatCsv,
	_FormatName[7:12]:  FormatTable,
	_FormatName[12:15]: FormatTsv,
	_FormatName[15:19]: FormatXlsx,
}

// ParseFormat attempts to convert a string to a Format.
func ParseFormat(name string) (Format, error) {
	if x, ok := _FormatValue[name]; ok {
		return x, nil
	}
	return Format(0), fmt.Errorf("%s is not a valid Format", name)
}
