package cmd

import "github.com/spf13/cobra"

// Import is used to import all of these package's commands
func Import(rootCmd *cobra.Command) {
	rootCmd.AddCommand(QueryCmd, HistoryCmd)
}
