package main

import (
	"fmt"
	"os"

	cmd "github.com/justmiles/athena-cli/cmd"

	"github.com/spf13/cobra"
)

// version of github.com/justmiles/athena-cli. Overwritten during build
var version = "development"

var rootCmd = &cobra.Command{
	Use:   "athena",
	Short: "query athena and other tools",
}

func main() {
	rootCmd.Version = version
	rootCmd.SetVersionTemplate(`{{printf "%s" .version}}
`)
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

// Import other commands
func init() {
	cmd.Import(rootCmd)
}
