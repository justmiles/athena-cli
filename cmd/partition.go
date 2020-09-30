package cmd

import (
	lib "github.com/justmiles/athena-cli/lib"

	"github.com/spf13/cobra"
)

var (
	databaseName, tableName, locationTemplate string
	partitions                                []string
	daysback                                  int
)

// PartitionCmd ..
var PartitionCmd = &cobra.Command{
	Use:   "partition",
	Short: "add partitions to a table",
	Long: `Add partitions to a table by specifing an S3 location template and the various partitions for each template value. 
If included in your template Year, Month, and Day partitions will be added for each applicable date in the past.

	athena partition \
	  --database default \
	  --table cloudtrail \
	  --location-template "s3://mycloudtrailbucket/AWSLogs/{{.Account}}/CloudTrail/{{.Region}}/{{.Year}}/{{.Month}}/{{.Day}}" \
	  --partition Account=000000000,1111111111 \
	  --partition Region=us-east-1,us-west-2
    
	# Would add the following partitions to the default.cloudtrail table:
	# 
	# s3://mycloudtrailbucket/AWSLogs/000000000/CloudTrail/us-east-1/2020/09/29
	# s3://mycloudtrailbucket/AWSLogs/000000000/CloudTrail/us-east-1/2020/09/30
	# s3://mycloudtrailbucket/AWSLogs/000000000/CloudTrail/us-west-2/2020/09/29
	# s3://mycloudtrailbucket/AWSLogs/000000000/CloudTrail/us-west-2/2020/09/30
	# s3://mycloudtrailbucket/AWSLogs/1111111111/CloudTrail/us-east-1/2020/09/29
	# s3://mycloudtrailbucket/AWSLogs/1111111111/CloudTrail/us-east-1/2020/09/30
	# s3://mycloudtrailbucket/AWSLogs/1111111111/CloudTrail/us-west-2/2020/09/29
	# s3://mycloudtrailbucket/AWSLogs/1111111111/CloudTrail/us-west-2/2020/09/30
`,
	Run: func(cmd *cobra.Command, args []string) {
		lib.Partition(daysback, databaseName, tableName, locationTemplate, partitions)
	},
}

func init() {
	PartitionCmd.PersistentFlags().IntVar(&daysback, "days", 1, "how many days in the past to add partitions")
	PartitionCmd.PersistentFlags().StringVarP(&databaseName, "database", "d", "", "database to update")
	PartitionCmd.PersistentFlags().StringVarP(&tableName, "table", "t", "", "table to update")
	PartitionCmd.PersistentFlags().StringVarP(&locationTemplate, "location-template", "l", "", "s3 location template for your partitions")
	PartitionCmd.PersistentFlags().StringArrayVarP(&partitions, "partition", "p", partitions, "list of partitions to to include. key=one,two,three")
	cobra.MarkFlagRequired(PartitionCmd.PersistentFlags(), "location-template")
}
