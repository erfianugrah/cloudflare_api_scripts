package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"media-toolkit-go/cmd/toolkit/commands"
	"media-toolkit-go/pkg/config"
)

var (
	version = "dev"
	commit  = "unknown"
	date    = "unknown"
)

func main() {
	// Set up signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-signalCh
		fmt.Println("\nReceived interrupt signal, shutting down gracefully...")
		cancel()
	}()

	// Create root command
	rootCmd := &cobra.Command{
		Use:   "media-toolkit",
		Short: "Media Resizer Pre-Warmer and Optimizer",
		Long: `A comprehensive media processing toolkit for:
- Pre-warming Cloudflare KV cache for media transformations
- Analyzing file sizes in remote storage  
- Optimizing video files using FFmpeg
- Testing image resizer variants and transformations
- Native load testing with detailed performance metrics`,
		Version: fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date),
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Initialize logging before running any command
			verbose, _ := cmd.Flags().GetBool("verbose")
			logger, err := config.SetupLogging(verbose)
			if err != nil {
				return fmt.Errorf("failed to setup logging: %w", err)
			}

			// Bind dry-run flag to viper
			dryRun, _ := cmd.Flags().GetBool("dry-run")
			viper.Set("dry-run", dryRun)

			// Store logger in context
			ctx = context.WithValue(ctx, "logger", logger)
			cmd.SetContext(ctx)

			return nil
		},
	}

	// Add global flags
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "Enable verbose logging")
	rootCmd.PersistentFlags().String("config", "", "Path to config file")
	rootCmd.PersistentFlags().Bool("dry-run", false, "Run in dry-run mode (no actual changes will be made)")

	// Add commands
	rootCmd.AddCommand(commands.NewPrewarmCommand())
	rootCmd.AddCommand(commands.NewOptimizeCommand())
	rootCmd.AddCommand(commands.NewValidateCommand())
	rootCmd.AddCommand(commands.NewLoadTestCommand())
	rootCmd.AddCommand(commands.NewAnalyzeCommand())
	rootCmd.AddCommand(commands.NewEnhancedWorkflowCommand())
	rootCmd.AddCommand(commands.NewVersionCommand(version, commit, date))

	// Set context
	rootCmd.SetContext(ctx)

	// Execute
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
