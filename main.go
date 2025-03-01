package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	"github.com/cheggaaa/pb/v3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config holds the configuration for the S3 uploader
type Config struct {
	// AWS Configuration
	AWSProfile string `json:"aws_profile"`
	AccessKey  string `json:"access_key"`
	SecretKey  string `json:"secret_key"`
	Region     string `json:"region"`
	
	// S3 Configuration
	BucketName string `json:"bucket_name"`
	S3Prefix   string `json:"s3_prefix"`
	
	// Local Configuration
	LocalPath  string `json:"local_path"`
	
	// Optional Configuration
	Pattern        string `json:"pattern,omitempty"`
	MaxConcurrency int    `json:"max_concurrency,omitempty"`
	LogLevel       string `json:"log_level,omitempty"`
}

// Uploader handles the S3 upload process
type Uploader struct {
	s3Client *s3.Client
	config   *Config
	logger   *zap.Logger
}

// LoadConfig loads configuration from a JSON file
func LoadConfig(configPath string) (*Config, error) {
	// Open the config file
	file, err := os.Open(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	// Decode the JSON file into the Config struct
	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	// Set default values for optional fields
	if config.Pattern == "" {
		config.Pattern = "*" // Match all files by default
	}
	
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = runtime.NumCPU() * 2
	}
	
	if config.LogLevel == "" {
		config.LogLevel = "info"
	}

	return &config, nil
}

// NewUploader creates a new S3 uploader with validation
func NewUploader(cfg *Config) (*Uploader, error) {
	// Validate required fields
	if cfg.BucketName == "" {
		return nil, errors.New("bucket_name is required in config")
	}
	
	if cfg.LocalPath == "" {
		return nil, errors.New("local_path is required in config")
	}
	
	// Verify source directory exists
	if _, err := os.Stat(cfg.LocalPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("local_path directory does not exist: %s", cfg.LocalPath)
	}
	
	// Ensure region is set
	if cfg.Region == "" {
		cfg.Region = "us-east-1" // Default region
	}

	// Configure AWS SDK options
	var awsConfigOptions []func(*config.LoadOptions) error
	
	// Set region
	awsConfigOptions = append(awsConfigOptions, config.WithRegion(cfg.Region))

	// Set credentials if provided
	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		staticProvider := credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")
		awsConfigOptions = append(awsConfigOptions, config.WithCredentialsProvider(staticProvider))
	} else if cfg.AWSProfile != "" {
		// Use named profile if specified
		awsConfigOptions = append(awsConfigOptions, config.WithSharedConfigProfile(cfg.AWSProfile))
	}
	
	// Load AWS configuration
	awsConfig, err := config.LoadDefaultConfig(context.TODO(), awsConfigOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}

	// Create S3 client
	s3Options := []func(*s3.Options){
		func(o *s3.Options) {
			o.UsePathStyle = true
		},
	}
	s3Client := s3.NewFromConfig(awsConfig, s3Options...)
	
	// Create logger
	logger, err := createLogger(cfg.LogLevel)
	if err != nil {
		return nil, fmt.Errorf("failed to create logger: %w", err)
	}

	return &Uploader{
		s3Client: s3Client,
		config:   cfg,
		logger:   logger,
	}, nil
}

// Upload starts the upload process
func (u *Uploader) Upload() error {
	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 24*time.Hour)
	defer cancel()

	u.logger.Info("Starting upload",
		zap.String("source", u.config.LocalPath),
		zap.String("bucket", u.config.BucketName),
		zap.String("prefix", u.config.S3Prefix),
		zap.String("region", u.config.Region))

	// Find files to upload
	files, err := u.findFiles()
	if err != nil {
		return fmt.Errorf("failed to find files: %w", err)
	}

	if len(files) == 0 {
		u.logger.Info("No files to upload")
		return nil
	}

	u.logger.Info("Found files to upload", zap.Int("count", len(files)))

	// Create progress bar
	bar := pb.Full.Start(len(files))

	// Create worker pool
	var wg sync.WaitGroup
	jobs := make(chan string, len(files))
	results := make(chan error, len(files))
	
	// Start workers
	for i := 0; i < u.config.MaxConcurrency; i++ {
		wg.Add(1)
		go u.uploadWorker(ctx, &wg, jobs, results, bar)
	}

	// Send jobs
	for _, file := range files {
		jobs <- file
	}
	close(jobs)

	// Wait for workers to finish
	wg.Wait()
	close(results)

	// Process results
	var failedFiles int
	for err := range results {
		if err != nil {
			failedFiles++
		}
	}

	bar.Finish()

	if failedFiles > 0 {
		u.logger.Warn("Upload completed with errors", zap.Int("failed_files", failedFiles))
		return fmt.Errorf("failed to upload %d files", failedFiles)
	}

	u.logger.Info("Upload completed successfully", zap.Int("total_files", len(files)))
	return nil
}

// findFiles finds all files matching the pattern
func (u *Uploader) findFiles() ([]string, error) {
	var files []string

	err := filepath.Walk(u.config.LocalPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		matched, err := filepath.Match(u.config.Pattern, filepath.Base(path))
		if err != nil {
			return err
		}

		if matched {
			files = append(files, path)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return files, nil
}

// uploadWorker handles file uploads
func (u *Uploader) uploadWorker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan string, results chan<- error, bar *pb.ProgressBar) {
	defer wg.Done()

	for filePath := range jobs {
		start := time.Now()
		err := u.uploadFile(ctx, filePath)
		duration := time.Since(start)

		if err != nil {
			u.logger.Error("Upload failed",
				zap.String("file", filePath),
				zap.Error(err))
			results <- err
		} else {
			// Determine S3 key for logging
			relPath, _ := filepath.Rel(u.config.LocalPath, filePath)
			s3Key := filepath.Join(u.config.S3Prefix, filepath.ToSlash(relPath))
			
			u.logger.Debug("File uploaded",
				zap.String("file", filePath),
				zap.String("s3_key", s3Key),
				zap.Duration("duration", duration))
			results <- nil
		}

		bar.Increment()
	}
}

// uploadFile uploads a single file to S3
func (u *Uploader) uploadFile(ctx context.Context, filePath string) error {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()
	
	// Determine S3 key
	relPath, err := filepath.Rel(u.config.LocalPath, filePath)
	if err != nil {
		return fmt.Errorf("failed to determine relative path: %w", err)
	}
	s3Key := filepath.Join(u.config.S3Prefix, filepath.ToSlash(relPath))
	
	// Upload to S3
	_, err = u.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(u.config.BucketName),
		Key:    aws.String(s3Key),
		Body:   file,
	})
	
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			if strings.Contains(apiErr.Error(), "region") {
				u.logger.Error("Region error detected",
					zap.String("file", filePath),
					zap.String("s3_key", s3Key),
					zap.Error(err))
			}
		}
		return fmt.Errorf("failed to upload file: %w", err)
	}
	
	return nil
}

// createLogger creates a new logger with the specified log level
func createLogger(level string) (*zap.Logger, error) {
	// Logger configuration
	config := zap.NewProductionConfig()
	
	// Set log level
	switch strings.ToLower(level) {
	case "debug":
		config.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	case "info":
		config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	case "warn":
		config.Level = zap.NewAtomicLevelAt(zapcore.WarnLevel)
	case "error":
		config.Level = zap.NewAtomicLevelAt(zapcore.ErrorLevel)
	default:
		config.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}
	
	return config.Build()
}

func main() {
	// Define command line flag for config file path
	configPath := flag.String("config", "config.json", "Path to config.json file")
	flag.Parse()
	
	// Load configuration from JSON file
	config, err := LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}
	
	// Print configuration summary
	fmt.Printf("Configuration loaded from %s:\n", *configPath)
	fmt.Printf("  Bucket: %s\n", config.BucketName)
	fmt.Printf("  Prefix: %s\n", config.S3Prefix)
	fmt.Printf("  Region: %s\n", config.Region)
	fmt.Printf("  Source: %s\n", config.LocalPath)
	fmt.Printf("  Pattern: %s\n", config.Pattern)
	
	// Create uploader
	uploader, err := NewUploader(config)
	if err != nil {
		log.Fatalf("Failed to create uploader: %v", err)
	}
	
	// Start upload
	if err := uploader.Upload(); err != nil {
		log.Fatalf("Upload failed: %v", err)
	}
}
