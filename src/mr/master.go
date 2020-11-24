package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Master struct {
	// Tasks wait to be done
	UndoTasks chan *Task
	// Tasks who are being processed
	DoingTasks map[int]*Task
	// ID counter for new tasks
	IDCounter       int
	RemainedMapTask int
	MapOutput       [][]string

	// Locks
	idCounterLock sync.Mutex
	doingTaskLock sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// RequestTask request for a new task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <-m.UndoTasks
	task.Update()
	task.timer = time.AfterFunc(time.Second*10, func() {
		m.handleTimeout(task)
	})
	reply.Task = task

	m.doingTaskLock.Lock()
	m.DoingTasks[task.ID] = task
	m.doingTaskLock.Unlock()
	return nil
}

// TaskDone mark a task as Done
func (m *Master) TaskDone(args *NotifyTaskDoneArgs, reply *NotifyTaskDoneReply) error {
	m.doingTaskLock.Lock()
	if task, ok := m.DoingTasks[args.Task.ID]; ok {
		task.timer.Stop()
		delete(m.DoingTasks, args.Task.ID)
		if task.Type == MapTask {
			m.RemainedMapTask--
			m.MapOutput[task.ID] = args.Output
		} else {

		}
	}
	if m.RemainedMapTask == 0 {
		// Start issuing reduce tasks
		fmt.Println("Start reduce")
		fmt.Println(m.MapOutput)
	}
	m.doingTaskLock.Unlock()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// HandleTimeout handles timeout event of a task
func (m *Master) handleTimeout(task *Task) {
	task.Update()
	m.UndoTasks <- task
	fmt.Println("Timeout event triggered")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		UndoTasks:       make(chan *Task, len(files)+nReduce),
		DoingTasks:      make(map[int]*Task),
		RemainedMapTask: len(files),
		MapOutput:       make([][]string, len(files)),
	}

	m.server()

	time.Sleep(time.Second * 5)

	// Fill init tasks
	for _, file := range files {
		m.idCounterLock.Lock()
		newTask := &Task{
			ID:      m.IDCounter,
			Type:    MapTask,
			Input:   []string{file},
			NReduce: nReduce,
		}
		m.IDCounter++
		m.idCounterLock.Unlock()
		newTask.Update()
		m.UndoTasks <- newTask
	}

	return &m
}
