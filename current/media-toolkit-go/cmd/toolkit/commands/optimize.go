package commands

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"media-toolkit-go/pkg/ffmpeg"
	"media-toolkit-go/pkg/reporting"
	"media-toolkit-go/pkg/storage"
	"media-toolkit-go/pkg/utils"
)

// NewOptimizeCommand creates the optimize command
func NewOptimizeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "optimize",
		Short: "Optimize video files using FFmpeg",
		Long: `Optimize large video files using FFmpeg for better performance and reduced size.
Supports hardware acceleration and various codecs (H.264, H.265, VP9, AV1).`,
		RunE: runOptimize,
	}

	addOptimizeFlags(cmd)
	return cmd
}

func addOptimizeFlags(cmd *cobra.Command) {
	// Required flags
	cmd.Flags().String("remote", "", "rclone remote name (required)")
	cmd.Flags().String("bucket", "", "S3 bucket name (required)")

	// Optional flags
	cmd.Flags().String("directory", "", "Directory path within bucket")
	cmd.Flags().Bool("optimize-videos", false, "Enable video optimization")
	cmd.Flags().Bool("optimize-in-place", false, "Re-encode and replace files in-place")
	cmd.Flags().String("codec", "h264", "Video codec (h264, h265, vp9, vp8, av1)")
	cmd.Flags().String("quality", "balanced", "Quality profile (maximum, high, balanced, efficient, minimum)")
	cmd.Flags().String("target-resolution", "1080p", "Target resolution (4k, 1080p, 720p, 480p, 360p)")
	cmd.Flags().String("fit", "contain", "Fit mode (contain, cover, pad, stretch)")
	cmd.Flags().String("audio-profile", "medium", "Audio profile (high, medium, low, minimum)")
	cmd.Flags().String("output-format", "mp4", "Output format (mp4, webm, mkv)")
	cmd.Flags().Bool("create-webm", false, "Also create WebM version")
	cmd.Flags().String("hardware-acceleration", "auto", "Hardware acceleration (auto, nvidia, intel, amd, apple, none)")
	cmd.Flags().Bool("disable-hardware-acceleration", false, "Disable hardware acceleration")
	cmd.Flags().Bool("browser-compatible", true, "Ensure browser compatibility")
	cmd.Flags().String("optimized-videos-dir", "optimized_videos", "Directory for optimized videos")
	cmd.Flags().Int("size-threshold", 256, "Size threshold in MiB for optimization")
	cmd.Flags().Int("workers", 5, "Number of concurrent workers")
	// Add common filtering flags
	utils.AddFilteringFlags(cmd)

	// Mark required flags
	cmd.MarkFlagRequired("remote")
	cmd.MarkFlagRequired("bucket")

	// Bind flags to viper
	viper.BindPFlags(cmd.Flags())
}

func runOptimize(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	// Get logger from context
	logger, ok := ctx.Value("logger").(*zap.Logger)
	if !ok {
		return fmt.Errorf("logger not found in context")
	}

	logger.Info("Starting video optimization process")

	// Execute optimization workflow
	return executeOptimization(ctx, logger)
}

// executeOptimization runs the actual optimization logic
func executeOptimization(ctx context.Context, logger *zap.Logger) error {
	// Get configuration values from viper
	remote := viper.GetString("remote")
	bucket := viper.GetString("bucket")
	directory := viper.GetString("directory")
	dryRun := viper.GetBool("dry-run")

	if remote == "" || bucket == "" {
		return fmt.Errorf("remote and bucket are required for optimization")
	}

	// Create storage client
	storageConfig := storage.StorageConfig{
		Remote:       remote,
		RcloneBinary: "rclone",
	}
	storageClient := storage.NewRcloneStorage(storageConfig, logger)

	// Create FFmpeg optimizer
	optimizer := ffmpeg.NewVideoOptimizer(logger)

	// Configure optimization
	optimizationConfig := ffmpeg.OptimizationConfig{
		CRF:              23, // Balanced quality
		Preset:           "medium",
		Profile:          "high",
		Level:            "4.0",
		ScaleAlgorithm:   "lanczos",
		AudioCodec:       "aac",
		AudioBitrate:     128,
		AudioSampleRate:  44100,
		AudioChannels:    2,
		OutputFormat:     viper.GetString("output-format"),
		OutputCodec:      getCodecFromName(viper.GetString("codec")),
		Threads:          0, // Auto-detect
		Overwrite:        false,
		PreserveMetadata: true,
	}

	// Get list of video files to optimize
	listReq := storage.ListRequest{
		Bucket:    bucket,
		Directory: directory,
	}

	objects, err := storageClient.ListObjects(ctx, listReq)
	if err != nil {
		return fmt.Errorf("failed to list objects: %w", err)
	}

	logger.Info("Found objects for optimization", zap.Int("count", len(objects)))

	// Apply filters using shared utility
	filterConfig := &utils.FilterConfig{
		Extensions:         viper.GetStringSlice("extensions"),
		MediaType:          viper.GetString("media-type"),
		ExcludeExtensions:  viper.GetStringSlice("exclude-extensions"),
		ExcludePatterns:    viper.GetStringSlice("exclude-patterns"),
		ExcludeDirectories: viper.GetStringSlice("exclude-directories"),
		ExcludeMinSize:     viper.GetInt("exclude-min-size"),
		ExcludeMaxSize:     viper.GetInt("exclude-max-size"),
	}

	// Convert to FileInfo type for filtering
	fileInfos := make([]storage.FileInfo, len(objects))
	for i, obj := range objects {
		fileInfos[i] = storage.FileInfo(obj)
	}

	filteredFiles := utils.ApplyFileFilters(fileInfos, filterConfig)

	// Filter by size threshold for optimization
	sizeThreshold := int64(viper.GetInt("size-threshold") * 1024 * 1024) // Convert MiB to bytes
	var videoFiles []storage.Object
	for _, file := range filteredFiles {
		if file.Size > sizeThreshold {
			videoFiles = append(videoFiles, storage.Object(file))
		}
	}

	logger.Info("Filtered video files for optimization",
		zap.Int("total_files", len(objects)),
		zap.Int("video_files", len(videoFiles)),
		zap.Int64("size_threshold_mb", sizeThreshold/(1024*1024)))

	if len(videoFiles) == 0 {
		logger.Info("No video files found above size threshold")
		return nil
	}

	if dryRun {
		logger.Info("Running in dry-run mode - no files will be modified")
	}

	// Create working directory
	workDir := "./temp_optimization"
	if !dryRun {
		if err := os.MkdirAll(workDir, 0755); err != nil {
			return fmt.Errorf("failed to create working directory: %w", err)
		}
		defer os.RemoveAll(workDir)
	}

	// Process files
	var results []ffmpeg.OptimizationResult
	optimizeInPlace := viper.GetBool("optimize-in-place")

	for i, videoFile := range videoFiles {
		logger.Info("Processing video file",
			zap.Int("progress", i+1),
			zap.Int("total", len(videoFiles)),
			zap.String("file", videoFile.Path),
			zap.Int64("size_mb", videoFile.Size/(1024*1024)))

		// Download file for processing
		localPath := fmt.Sprintf("%s/%s", workDir, strings.ReplaceAll(videoFile.Path, "/", "_"))
		remotePath := fmt.Sprintf("%s/%s", bucket, videoFile.Path)

		if dryRun {
			logger.Info("[DRY-RUN] Would download file",
				zap.String("remote", remotePath),
				zap.String("local", localPath))
		} else {
			if err := storageClient.DownloadObject(ctx, remotePath, localPath); err != nil {
				logger.Error("Failed to download file", zap.String("file", videoFile.Path), zap.Error(err))
				continue
			}
		}

		// Create output path
		var outputPath string
		if optimizeInPlace {
			outputPath = localPath + ".optimized"
		} else {
			outputPath = localPath + ".opt." + viper.GetString("output-format")
		}

		// Optimize the video
		var result *ffmpeg.OptimizationResult
		if dryRun {
			logger.Info("[DRY-RUN] Would optimize video",
				zap.String("input", localPath),
				zap.String("output", outputPath),
				zap.String("codec", viper.GetString("codec")),
				zap.String("format", viper.GetString("output-format")))

			// Create simulated result for dry-run
			result = &ffmpeg.OptimizationResult{
				InputFile:        localPath,
				OutputFile:       outputPath,
				Success:          true,
				OriginalSize:     videoFile.Size,
				OptimizedSize:    int64(float64(videoFile.Size) * 0.7), // Estimate 30% reduction
				SizeReductionPct: 30.0,
				StartTime:        time.Now(),
				EndTime:          time.Now(),
			}
		} else {
			result, err = optimizer.OptimizeVideo(ctx, localPath, outputPath, optimizationConfig)
			if err != nil {
				logger.Error("Failed to optimize video", zap.String("file", videoFile.Path), zap.Error(err))
				result = &ffmpeg.OptimizationResult{
					InputFile:    localPath,
					OutputFile:   outputPath,
					Success:      false,
					Error:        err.Error(),
					OriginalSize: videoFile.Size,
					StartTime:    time.Now(),
					EndTime:      time.Now(),
				}
			}
		}

		results = append(results, *result)

		// Upload optimized file back if successful
		if result.Success {
			uploadPath := videoFile.Path
			if !optimizeInPlace {
				// Create new path for optimized version
				uploadPath = strings.TrimSuffix(videoFile.Path, filepath.Ext(videoFile.Path)) +
					"_optimized." + viper.GetString("output-format")
			}

			remoteUploadPath := fmt.Sprintf("%s/%s", bucket, uploadPath)
			if dryRun {
				logger.Info("[DRY-RUN] Would upload optimized file",
					zap.String("local", outputPath),
					zap.String("remote", remoteUploadPath),
					zap.Int64("estimated_size_mb", result.OptimizedSize/(1024*1024)))
			} else {
				if err := storageClient.UploadObject(ctx, outputPath, remoteUploadPath); err != nil {
					logger.Error("Failed to upload optimized file",
						zap.String("local", outputPath),
						zap.String("remote", remoteUploadPath),
						zap.Error(err))
				} else {
					logger.Info("Uploaded optimized file",
						zap.String("remote", uploadPath),
						zap.Int64("original_size_mb", result.OriginalSize/(1024*1024)),
						zap.Int64("optimized_size_mb", result.OptimizedSize/(1024*1024)),
						zap.Float64("reduction_pct", result.SizeReductionPct))
				}
			}
		}

		// Clean up local files
		if !dryRun {
			os.Remove(localPath)
			os.Remove(outputPath)
		}
	}

	// Generate optimization report
	if dryRun {
		logger.Info("[DRY-RUN] Would generate optimization report with", zap.Int("results", len(results)))
	} else {
		if err := generateOptimizationReport(results, logger); err != nil {
			logger.Warn("Failed to generate optimization report", zap.Error(err))
		}
	}

	// Log summary
	successCount := 0
	totalSaved := int64(0)
	for _, result := range results {
		if result.Success {
			successCount++
			totalSaved += result.SizeReduction
		}
	}

	logger.Info("Video optimization completed",
		zap.Int("total_files", len(videoFiles)),
		zap.Int("successful", successCount),
		zap.Int("failed", len(results)-successCount),
		zap.Int64("total_saved_mb", totalSaved/(1024*1024)))

	return nil
}

// getCodecFromName converts codec name to FFmpeg codec string
func getCodecFromName(codecName string) string {
	switch strings.ToLower(codecName) {
	case "h264":
		return "libx264"
	case "h265", "hevc":
		return "libx265"
	case "vp9":
		return "libvpx-vp9"
	case "vp8":
		return "libvpx"
	case "av1":
		return "libaom-av1"
	default:
		return "libx264" // Default
	}
}

// generateOptimizationReport creates a report of optimization results
func generateOptimizationReport(results []ffmpeg.OptimizationResult, logger *zap.Logger) error {
	// Create report data
	reportData := reporting.ReportData{
		GeneratedAt:         time.Now(),
		ReportName:          "video-optimization",
		OptimizationResults: results,
	}

	// Generate report
	generator := reporting.NewGenerator(logger)
	reportConfig := reporting.ReportConfig{
		OutputDir:       ".",
		ReportName:      "optimization-report",
		Format:          []string{"markdown", "json"},
		IncludeSections: []string{"summary", "optimization"},
		Timestamp:       true,
	}

	return generator.GenerateReport(reportData, reportConfig)
}
