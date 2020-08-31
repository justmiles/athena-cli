package cmd

import (
	"fmt"
	"log"

	lib "github.com/justmiles/athena-cli/lib"

	"github.com/spf13/cobra"
)

var q lib.Query

const outputBucketDefault = "aws-athena-query-results-<account>-<region>"

// QueryCmd ...
var QueryCmd = &cobra.Command{
	Use:   "query",
	Short: "query Athena",
	Run: func(cmd *cobra.Command, args []string) {

		if q.OutputBucket == outputBucketDefault {
			q.OutputBucket = fmt.Sprintf("aws-athena-query-results-%s-%s", lib.AccountID(), lib.Region())
		}

		file, err := q.Execute()
		if err != nil {
			log.Fatal(err)
		}

		// Clean up
		defer lib.CleanCache(file.Name())

		err = lib.OutputData(q.Format, q.JMESPath, file)

		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	QueryCmd.PersistentFlags().StringVar(&q.OutputBucket, "output-bucket", outputBucketDefault, "S3 bucket for Athena query results")
	QueryCmd.PersistentFlags().StringVar(&q.OutputPrefix, "output-prefix", "", "S3 key prefix for Athena query results")
	QueryCmd.PersistentFlags().StringVarP(&q.Database, "database", "d", "default", "Athena database to query")
	QueryCmd.PersistentFlags().StringVarP(&q.SQL, "sql", "s", "", "SQL query to execute. Can be a file or raw query")
	QueryCmd.PersistentFlags().StringVarP(&q.Format, "format", "f", "csv", "format the output as either json, csv, or table")
	// QueryCmd.PersistentFlags().StringVar(&q.JMESPath, "jmespath", "", "optional JMESPath to further filter or format results. See jmespath.org for more.")
	QueryCmd.PersistentFlags().BoolVar(&q.Statistics, "statistics", false, "print query statistics to stderr")
}
