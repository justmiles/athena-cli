package cmd

import (
	"log"

	lib "github.com/justmiles/athena-cli/lib"

	"github.com/spf13/cobra"
)

var (
	historyCmdMaxResults int
	historyCmdWorkgroup  string
)

// HistoryCmd ...
var HistoryCmd = &cobra.Command{
	Use:   "history",
	Short: "export Athena execution history",
	Run: func(cmd *cobra.Command, args []string) {

		h, err := lib.ListHistory(historyCmdMaxResults, historyCmdWorkgroup)

		if err != nil {
			log.Fatal(err)
		}

		err = lib.RenderHistoryResults(h, q.Format)

		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	HistoryCmd.PersistentFlags().IntVarP(&historyCmdMaxResults, "max", "m", 100, "maximum results")
	HistoryCmd.PersistentFlags().StringVarP(&q.Format, "format", "f", "csv", "format the output as either json, csv, or table")
	// TODO: Add support for selecting a workgroup
	HistoryCmd.PersistentFlags().StringVarP(&historyCmdWorkgroup, "workgroup", "w", "primary", "workgroup")
}
