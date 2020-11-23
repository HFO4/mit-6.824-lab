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
	Type       TaskType
	Input      []string
	LastUpsate time.Time

	// Locks
	modifyLock sync.Mutex
}

// Update the last update time
func (task *Task) Update() {
	task.modifyLock.Lock()
	defer task.modifyLock.Unlock()
	task.LastUpsate = time.Now()
}
