package commands

import (
	"fmt"

	"github.com/spf13/cobra"
)

// NewVersionCommand creates the version command
func NewVersionCommand(version, commit, date string) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version information",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("media-toolkit version %s\n", version)
			fmt.Printf("Git commit: %s\n", commit)
			fmt.Printf("Built: %s\n", date)
		},
	}
}
