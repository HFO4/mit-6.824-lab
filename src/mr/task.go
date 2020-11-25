package mr

import (
	"sync"
	"time"
)

// TaskType type of tasks
type TaskType int

const (
	MapTask = iota
	ReduceTask
)

// Task for workers
type Task struct {
	ID       int
	Type     TaskType
	Input    []string
	NReduce  int
	ReduceID int

	// Locks
	modifyLock sync.Mutex

	// Timeout timer
	timer *time.Timer
}
