package lib

import (
	"fmt"
	"testing"
)

func init() {
	svc = &mockAthenaClient{}
	downloader = &mockDownloaderAPI{}
}

func Test_QueryExecute(t *testing.T) {
	q := Query{
		Database:           "default",
		SQL:                "SELECT 1",
		OutputFile:         "/dev/stdout",
		QueryResultsBucket: "test",
		Format:             "csv",
		Statistics:         false,
	}

	f, err := q.Execute()
	if err != nil {
		t.Errorf("Could note Execute() query: %v", err.Error())
		return
	}

	fmt.Println(f.Name())

}
