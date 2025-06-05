package workers

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// WorkerPoolConfig configures worker pool behavior
type WorkerPoolConfig struct {
	SmallFileWorkers  int           // Workers for files ≤50MB
	MediumFileWorkers int           // Workers for files 50-300MB
	LargeFileWorkers  int           // Workers for files >300MB
	QueueSize         int           // Maximum queue size
	WorkerTimeout     time.Duration // Individual worker timeout
	ShutdownTimeout   time.Duration // Graceful shutdown timeout
}

// DefaultWorkerPoolConfig returns sensible defaults
func DefaultWorkerPoolConfig() WorkerPoolConfig {
	return WorkerPoolConfig{
		SmallFileWorkers:  10,
		MediumFileWorkers: 5,
		LargeFileWorkers:  3,
		QueueSize:         1000,
		WorkerTimeout:     30 * time.Second,
		ShutdownTimeout:   60 * time.Second,
	}
}

// SizeCategory represents file size categories for worker allocation
type SizeCategory int

const (
	SmallFile  SizeCategory = iota // ≤50MB
	MediumFile                     // 50-300MB
	LargeFile                      // >300MB
)

// String returns the string representation of a size category
func (sc SizeCategory) String() string {
	switch sc {
	case SmallFile:
		return "small"
	case MediumFile:
		return "medium"
	case LargeFile:
		return "large"
	default:
		return "unknown"
	}
}

// GetSizeCategory determines the size category for a file
func GetSizeCategory(sizeBytes int64) SizeCategory {
	const (
		smallThreshold  = 50 * 1024 * 1024  // 50MB
		mediumThreshold = 300 * 1024 * 1024 // 300MB
	)

	if sizeBytes <= smallThreshold {
		return SmallFile
	} else if sizeBytes <= mediumThreshold {
		return MediumFile
	}
	return LargeFile
}

// Task represents a unit of work to be processed
type Task struct {
	ID          string
	SizeBytes   int64
	Category    SizeCategory
	Payload     interface{}
	ProcessFunc func(ctx context.Context, payload interface{}) error
	ResultChan  chan TaskResult
}

// TaskResult represents the result of task execution
type TaskResult struct {
	TaskID    string
	Success   bool
	Error     error
	Duration  time.Duration
	StartTime time.Time
	EndTime   time.Time
}

// WorkerPool manages size-based worker pools with graceful shutdown
type WorkerPool struct {
	config WorkerPoolConfig
	logger *zap.Logger

	// Worker pools by size category
	smallPool  *categoryPool
	mediumPool *categoryPool
	largePool  *categoryPool

	// Coordination
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Statistics
	tasksSubmitted int64
	tasksCompleted int64
	tasksFailed    int64

	// Shutdown coordination
	shutdown      int32
	shutdownMutex sync.RWMutex
}

// categoryPool manages workers for a specific size category
type categoryPool struct {
	name        string
	workerCount int
	taskQueue   chan *Task
	workers     []*worker

	// Statistics
	activeWorkers int64
	idleWorkers   int64
	queueSize     int64

	logger *zap.Logger
}

// worker represents an individual worker goroutine
type worker struct {
	id       int
	pool     *categoryPool
	taskChan chan *Task
	ctx      context.Context
	logger   *zap.Logger
}

// NewWorkerPool creates a new worker pool with the given configuration
func NewWorkerPool(config WorkerPoolConfig, logger *zap.Logger) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &WorkerPool{
		config: config,
		logger: logger,
		ctx:    ctx,
		cancel: cancel,
	}

	// Create category pools
	wp.smallPool = newCategoryPool("small", config.SmallFileWorkers, config.QueueSize/3, logger)
	wp.mediumPool = newCategoryPool("medium", config.MediumFileWorkers, config.QueueSize/3, logger)
	wp.largePool = newCategoryPool("large", config.LargeFileWorkers, config.QueueSize/3, logger)

	return wp
}

// newCategoryPool creates a new category-specific worker pool
func newCategoryPool(name string, workerCount, queueSize int, logger *zap.Logger) *categoryPool {
	return &categoryPool{
		name:        name,
		workerCount: workerCount,
		taskQueue:   make(chan *Task, queueSize),
		workers:     make([]*worker, 0, workerCount),
		logger:      logger,
	}
}

// Start initializes and starts all worker pools
func (wp *WorkerPool) Start() error {
	wp.shutdownMutex.Lock()
	defer wp.shutdownMutex.Unlock()

	if atomic.LoadInt32(&wp.shutdown) == 1 {
		return fmt.Errorf("worker pool is shutting down")
	}

	wp.logger.Info("Starting worker pool",
		zap.Int("small_workers", wp.config.SmallFileWorkers),
		zap.Int("medium_workers", wp.config.MediumFileWorkers),
		zap.Int("large_workers", wp.config.LargeFileWorkers))

	// Start all category pools
	if err := wp.startCategoryPool(wp.smallPool); err != nil {
		return fmt.Errorf("failed to start small file pool: %w", err)
	}

	if err := wp.startCategoryPool(wp.mediumPool); err != nil {
		return fmt.Errorf("failed to start medium file pool: %w", err)
	}

	if err := wp.startCategoryPool(wp.largePool); err != nil {
		return fmt.Errorf("failed to start large file pool: %w", err)
	}

	wp.logger.Info("Worker pool started successfully")
	return nil
}

// startCategoryPool starts workers for a specific category
func (wp *WorkerPool) startCategoryPool(pool *categoryPool) error {
	for i := 0; i < pool.workerCount; i++ {
		w := &worker{
			id:       i,
			pool:     pool,
			taskChan: pool.taskQueue,
			ctx:      wp.ctx,
			logger:   pool.logger.With(zap.String("worker", fmt.Sprintf("%s-%d", pool.name, i))),
		}

		pool.workers = append(pool.workers, w)

		wp.wg.Add(1)
		go wp.runWorker(w)
	}

	atomic.StoreInt64(&pool.idleWorkers, int64(pool.workerCount))
	return nil
}

// runWorker runs the main worker loop
func (wp *WorkerPool) runWorker(w *worker) {
	defer wp.wg.Done()

	w.logger.Debug("Worker started")
	defer w.logger.Debug("Worker stopped")

	for {
		select {
		case <-w.ctx.Done():
			return

		case task := <-w.taskChan:
			if task == nil {
				return // Channel closed
			}

			// Update worker statistics
			atomic.AddInt64(&w.pool.activeWorkers, 1)
			atomic.AddInt64(&w.pool.idleWorkers, -1)
			atomic.AddInt64(&w.pool.queueSize, -1)

			// Process the task
			result := wp.processTask(w, task)

			// Send result if channel is available
			select {
			case task.ResultChan <- result:
			case <-w.ctx.Done():
				return
			default:
				// Don't block if no one is listening for results
			}

			// Update statistics
			atomic.AddInt64(&wp.tasksCompleted, 1)
			if !result.Success {
				atomic.AddInt64(&wp.tasksFailed, 1)
			}

			// Update worker statistics
			atomic.AddInt64(&w.pool.activeWorkers, -1)
			atomic.AddInt64(&w.pool.idleWorkers, 1)
		}
	}
}

// processTask executes a single task with timeout handling
func (wp *WorkerPool) processTask(w *worker, task *Task) TaskResult {
	startTime := time.Now()

	// Create timeout context for the task
	taskCtx, taskCancel := context.WithTimeout(w.ctx, wp.config.WorkerTimeout)
	defer taskCancel()

	w.logger.Debug("Processing task",
		zap.String("task_id", task.ID),
		zap.String("category", task.Category.String()),
		zap.Int64("size_bytes", task.SizeBytes))

	// Execute the task
	var err error
	done := make(chan error, 1) // Buffered to prevent goroutine leak

	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("task panicked: %v", r)
			}
		}()

		// Check if context is already cancelled before starting
		select {
		case <-taskCtx.Done():
			done <- taskCtx.Err()
			return
		default:
		}

		// Execute the task function
		taskErr := task.ProcessFunc(taskCtx, task.Payload)
		done <- taskErr
	}()

	// Wait for completion, timeout, or worker shutdown
	select {
	case err = <-done:
		// Task completed (err may be nil or an actual error)
	case <-taskCtx.Done():
		// Task timed out or context cancelled
		err = fmt.Errorf("task timed out after %v: %w", wp.config.WorkerTimeout, taskCtx.Err())
	case <-w.ctx.Done():
		// Worker pool is shutting down
		err = fmt.Errorf("worker pool shutting down: %w", w.ctx.Err())
	}

	endTime := time.Now()
	duration := endTime.Sub(startTime)

	result := TaskResult{
		TaskID:    task.ID,
		Success:   err == nil,
		Error:     err,
		Duration:  duration,
		StartTime: startTime,
		EndTime:   endTime,
	}

	if err != nil {
		w.logger.Error("Task failed",
			zap.String("task_id", task.ID),
			zap.Error(err),
			zap.Duration("duration", duration))
	} else {
		w.logger.Debug("Task completed",
			zap.String("task_id", task.ID),
			zap.Duration("duration", duration))
	}

	return result
}

// SubmitTask submits a task to the appropriate worker pool based on size
func (wp *WorkerPool) SubmitTask(task *Task) error {
	wp.shutdownMutex.RLock()
	defer wp.shutdownMutex.RUnlock()

	if atomic.LoadInt32(&wp.shutdown) == 1 {
		return fmt.Errorf("worker pool is shutting down")
	}

	// Determine the appropriate pool
	var pool *categoryPool
	switch task.Category {
	case SmallFile:
		pool = wp.smallPool
	case MediumFile:
		pool = wp.mediumPool
	case LargeFile:
		pool = wp.largePool
	default:
		return fmt.Errorf("unknown task category: %v", task.Category)
	}

	// Submit to the appropriate queue with timeout
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	select {
	case pool.taskQueue <- task:
		atomic.AddInt64(&wp.tasksSubmitted, 1)
		atomic.AddInt64(&pool.queueSize, 1)
		return nil
	case <-wp.ctx.Done():
		return fmt.Errorf("worker pool is shutting down")
	case <-timeout.C:
		// Instead of immediate error, wait briefly and retry
		wp.logger.Debug("Queue busy, retrying", zap.String("category", pool.name))
		select {
		case pool.taskQueue <- task:
			atomic.AddInt64(&wp.tasksSubmitted, 1)
			atomic.AddInt64(&pool.queueSize, 1)
			return nil
		case <-wp.ctx.Done():
			return fmt.Errorf("worker pool is shutting down")
		default:
			return fmt.Errorf("queue full for %s file category after retry", pool.name)
		}
	}
}

// Shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) Shutdown() error {
	wp.shutdownMutex.Lock()
	defer wp.shutdownMutex.Unlock()

	if atomic.SwapInt32(&wp.shutdown, 1) == 1 {
		return nil // Already shutting down
	}

	wp.logger.Info("Shutting down worker pool")

	// First signal shutdown by setting atomic flag and canceling context
	wp.cancel()

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		wp.logger.Info("All workers stopped gracefully")
		// Now safe to close channels since workers are finished
		wp.closeChannelsSafely()
	case <-time.After(wp.config.ShutdownTimeout):
		wp.logger.Warn("Shutdown timeout reached, forcing shutdown")
		// Close channels even if workers haven't finished
		wp.closeChannelsSafely()
	}

	wp.logger.Info("Worker pool shutdown complete")
	return nil
}

// closeChannelsSafely closes all task queue channels safely
func (wp *WorkerPool) closeChannelsSafely() {
	// Use recover to handle potential double-close panics
	defer func() {
		if r := recover(); r != nil {
			wp.logger.Warn("Recovered from panic while closing channels", zap.Any("panic", r))
		}
	}()

	// Close channels if not already closed
	select {
	case <-wp.smallPool.taskQueue:
		// Channel already closed
	default:
		close(wp.smallPool.taskQueue)
	}

	select {
	case <-wp.mediumPool.taskQueue:
		// Channel already closed
	default:
		close(wp.mediumPool.taskQueue)
	}

	select {
	case <-wp.largePool.taskQueue:
		// Channel already closed
	default:
		close(wp.largePool.taskQueue)
	}
}

// Statistics returns current worker pool statistics
func (wp *WorkerPool) Statistics() WorkerPoolStats {
	return WorkerPoolStats{
		TasksSubmitted: atomic.LoadInt64(&wp.tasksSubmitted),
		TasksCompleted: atomic.LoadInt64(&wp.tasksCompleted),
		TasksFailed:    atomic.LoadInt64(&wp.tasksFailed),
		SmallPool:      wp.getCategoryStats(wp.smallPool),
		MediumPool:     wp.getCategoryStats(wp.mediumPool),
		LargePool:      wp.getCategoryStats(wp.largePool),
	}
}

// getCategoryStats returns statistics for a specific category pool
func (wp *WorkerPool) getCategoryStats(pool *categoryPool) CategoryStats {
	return CategoryStats{
		Name:          pool.name,
		WorkerCount:   pool.workerCount,
		ActiveWorkers: atomic.LoadInt64(&pool.activeWorkers),
		IdleWorkers:   atomic.LoadInt64(&pool.idleWorkers),
		QueueSize:     atomic.LoadInt64(&pool.queueSize),
		QueueCapacity: cap(pool.taskQueue),
	}
}

// WorkerPoolStats contains comprehensive worker pool statistics
type WorkerPoolStats struct {
	TasksSubmitted int64         `json:"tasks_submitted"`
	TasksCompleted int64         `json:"tasks_completed"`
	TasksFailed    int64         `json:"tasks_failed"`
	SmallPool      CategoryStats `json:"small_pool"`
	MediumPool     CategoryStats `json:"medium_pool"`
	LargePool      CategoryStats `json:"large_pool"`
}

// CategoryStats contains statistics for a specific worker category
type CategoryStats struct {
	Name          string `json:"name"`
	WorkerCount   int    `json:"worker_count"`
	ActiveWorkers int64  `json:"active_workers"`
	IdleWorkers   int64  `json:"idle_workers"`
	QueueSize     int64  `json:"queue_size"`
	QueueCapacity int    `json:"queue_capacity"`
}
