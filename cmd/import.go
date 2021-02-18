package cmd

import (
	"log"

	"github.com/spf13/cobra"
)

// Import is used to import all of these package's commands
func Import(rootCmd *cobra.Command) {
	log.SetFlags(0)
	rootCmd.AddCommand(QueryCmd, HistoryCmd, PartitionCmd)
}
