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

		err := q.ExecuteToStdout()
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	QueryCmd.PersistentFlags().StringVar(&q.OutputBucket, "s3-output-bucket", outputBucketDefault, "S3 output bucket for Athena query results")
	QueryCmd.PersistentFlags().StringVar(&q.Database, "database", "default", "Athena database to query")
	QueryCmd.PersistentFlags().StringVar(&q.SQL, "sql", "", "SQL query to execute")
}
