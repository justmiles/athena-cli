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
		_, err := lib.ParseFormat(q.Format)
		if err != nil {
			log.Fatal(err)
		}
		if q.QueryResultsBucket == outputBucketDefault {
			q.QueryResultsBucket = fmt.Sprintf("aws-athena-query-results-%s-%s", lib.AccountID(), lib.Region())
		}

		file, err := q.Execute()
		if err != nil {
			log.Fatal(err)
		}

		// Don't choke on empty queries
		if file == nil {
			return
		}

		// Clean up
		defer lib.CleanCache(file.Name())

		err = q.RenderQueryResults(file)

		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	QueryCmd.PersistentFlags().StringVar(&q.QueryResultsBucket, "query-results-bucket", outputBucketDefault, "S3 bucket for Athena query results")
	QueryCmd.PersistentFlags().StringVar(&q.QueryResultsPrefix, "query-results-prefix", "", "S3 key prefix for Athena query results")
	QueryCmd.PersistentFlags().StringVarP(&q.Database, "database", "d", "default", "Athena database to query")
	QueryCmd.PersistentFlags().StringVarP(&q.SQL, "sql", "s", "", "SQL query to execute. Can be a file or raw query")
	QueryCmd.PersistentFlags().StringVarP(&q.Format, "format", "f", "csv", "format the output as either json, csv, or table")
	QueryCmd.PersistentFlags().StringVarP(&q.OutputFile, "output", "o", "", "(optional) file name to write this content to (defaults to standard output)")
	QueryCmd.PersistentFlags().StringVarP(&q.WorkGroup, "workgroup", "w", "", "(optional) WorkGroup (defaults to primary)")
	QueryCmd.PersistentFlags().BoolVar(&q.Statistics, "statistics", false, "print query statistics to stderr")
	// QueryCmd.PersistentFlags().StringVar(&q.JMESPath, "jmespath", "", "optional JMESPath to further filter or format results. See jmespath.org for more.")
}
