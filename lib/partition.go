package lib

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/elliotchance/orderedmap"
)

// envOr returns the OS environment variable's value or a default value
func envOr(s, e string) string {
	envVar := os.Getenv(s)
	if envVar != "" {
		return envVar
	}
	return e
}

var (
	glueSvc = glue.New(sess)
)

// Partition partitions the things
func Partition(daysback int, databaseName, tableName, tmplate string, partitionsMap []string) {

	getPartitionsOutput, err := glueSvc.GetPartitions(&glue.GetPartitionsInput{
		MaxResults:   aws.Int64(1),
		DatabaseName: &databaseName,
		TableName:    &tableName,
	})
	if err != nil {
		log.Fatal(err)
	}
	if len(getPartitionsOutput.Partitions) == 0 {
		log.Fatal("please create at least one partition before using this tool")
	}

	partitionTemplate := getPartitionsOutput.Partitions[0]

	now := time.Now()
	start := now.AddDate(0, 0, (daysback * -1))
	end := now

	var partitions []*glue.PartitionInput

	uniquePartitions := [][]string{}

	for _, partitionList := range partitionsMap {
		s := strings.Split(partitionList, "=")
		partitions := []string{}
		for _, p := range strings.Split(s[1], ",") {
			partitions = append(partitions, fmt.Sprintf("%s=%s", s[0], p))
		}
		uniquePartitions = append(uniquePartitions, partitions)
	}

	for _, uniquePartition := range permutate2dSlice(uniquePartitions) {
		// Partition(daysback, databaseName, tableName, tmplate, uniquePartition)
		for rd := rangeDate(start, end); ; {
			date := rd()
			if date.IsZero() {
				break
			}

			buf := new(bytes.Buffer)
			options := orderedmap.NewOrderedMap()

			// Sort the partitions by how they appear in the template
			sort.SliceStable(uniquePartition, func(i, j int) bool {
				su := strings.Split(uniquePartition[i], "=")
				sj := strings.Split(uniquePartition[j], "=")
				return strings.Index(tmplate, su[0]) < strings.Index(tmplate, sj[0])
			})

			for _, partMap := range uniquePartition {
				s := strings.Split(partMap, "=")
				options.Set(s[0], s[1])
			}

			if strings.Contains(tmplate, "Year") {
				options.Set("Year", date.Format("2006"))
			}

			if strings.Contains(tmplate, "Month") {
				options.Set("Month", date.Format("01"))
			}

			if strings.Contains(tmplate, "Day") {
				options.Set("Day", date.Format("02"))
			}

			tmpl, err := template.New("test").Parse(tmplate)
			tmpl.Execute(buf, asStringMap(options))
			if err != nil {
				log.Fatal(err)
			}
			partitions = append(partitions, &glue.PartitionInput{
				Parameters: partitionTemplate.Parameters,
				StorageDescriptor: &glue.StorageDescriptor{
					Columns:                partitionTemplate.StorageDescriptor.Columns,
					InputFormat:            partitionTemplate.StorageDescriptor.InputFormat,
					OutputFormat:           partitionTemplate.StorageDescriptor.OutputFormat,
					SerdeInfo:              partitionTemplate.StorageDescriptor.SerdeInfo,
					NumberOfBuckets:        partitionTemplate.StorageDescriptor.NumberOfBuckets,
					Compressed:             partitionTemplate.StorageDescriptor.Compressed,
					StoredAsSubDirectories: partitionTemplate.StorageDescriptor.StoredAsSubDirectories,
					Location:               aws.String(buf.String())},
				Values: aws.StringSlice(orderedValues(options)),
			})
		}
	}

	for _, partition := range partitions {
		fmt.Println(*partition.StorageDescriptor.Location)
	}

	batch := 100

	for i := 0; i < len(partitions); i += batch {
		j := i + batch
		if j > len(partitions) {
			j = len(partitions)
		}

		output, err := glueSvc.BatchCreatePartition(&glue.BatchCreatePartitionInput{
			CatalogId:          partitionTemplate.CatalogId,
			DatabaseName:       aws.String(databaseName),
			TableName:          &tableName,
			PartitionInputList: partitions[i:j],
		})

		if err != nil {
			fmt.Println(err)
		}

		for _, errMsg := range output.Errors {
			if *errMsg.ErrorDetail.ErrorCode == "AlreadyExistsException" {
				continue
			}
			fmt.Println(errMsg)
		}

	}
}

func permutate2dSlice(ss [][]string) [][]string {
	if len(ss) == 0 {
		return ss
	}
	returnSS := [][]string{}

	if len(ss) == 1 {
		for _, vid := range ss[0] {
			returnSS = append(returnSS, []string{vid})
		}
		return returnSS
	}

	t := permutate2dSlice(ss[1:])
	for _, vid := range ss[0] {
		for _, perm := range t {
			returnSS = append(returnSS, append([]string{vid}, perm...))
		}
	}

	return returnSS
}

func asStringMap(m *orderedmap.OrderedMap) map[string]string {
	v := make(map[string]string)
	for _, key := range m.Keys() {

		value, _ := m.Get(key)

		v[fmt.Sprintf("%s", key)] = fmt.Sprintf("%s", value)
	}
	return v
}

func orderedValues(m *orderedmap.OrderedMap) (v []string) {
	for _, key := range m.Keys() {
		value, _ := m.Get(key)
		v = append(v, fmt.Sprintf("%s", value))
	}
	return v
}

func rangeDate(start, end time.Time) func() time.Time {
	y, m, d := start.Date()
	start = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
	y, m, d = end.Date()
	end = time.Date(y, m, d, 0, 0, 0, 0, time.UTC)

	return func() time.Time {
		if start.After(end) {
			return time.Time{}
		}
		date := start
		start = start.AddDate(0, 0, 1)
		return date
	}
}
