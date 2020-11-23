package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Tasks wait to be done
	UndoTasks chan *Task

	// Locks
}

// Your code here -- RPC handlers for the worker to call.

// RequestTask request for a new task
func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	task := <-m.UndoTasks
	task.Update()
	reply.Task = task
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

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		UndoTasks: make(chan *Task, len(files)),
	}

	// Fill init tasks
	for _, file := range files {
		newTask := &Task{
			Type:  MapTask,
			Input: []string{file},
		}
		newTask.Update()
		m.UndoTasks <- newTask
	}

	m.server()
	return &m
}
