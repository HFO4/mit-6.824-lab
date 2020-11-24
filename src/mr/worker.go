package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		if task, alive := PullTask(); alive {
			fmt.Println("NewTask:", task.Input)
			err := ProcessTask(task, mapf, reducef)
			if err != nil {
				fmt.Println("Failed task:", err)
				// Skip failed task
				continue
			}
		} else {
			fmt.Println("Worker quit")
			return
		}
	}

}

// ProcessTask process a task
func ProcessTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (err error) {
	// Handle panics
	defer func() {
		if panicErr := recover(); panicErr != nil {
			fmt.Println("Panic caught:", panicErr)
			err = fmt.Errorf("was panic, recovered value: %v", panicErr)
		}
	}()

	switch task.Type {
	case MapTask:
		return processMapTask(task, mapf)
	default:
		return errors.New("Unknown task type")
	}

	return nil
}

func processMapTask(task *Task, mapf func(string, string) []KeyValue) error {
	// init in-memory intermediate store
	intermediate := make([][]KeyValue, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		intermediate[i] = make([]KeyValue, 0)
	}

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}

		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {
			bucketID := ihash(kv.Key) % task.NReduce
			intermediate[bucketID] = append(intermediate[bucketID], kv)
		}
	}

	// Stroe intermediate values to disk
	for i := 0; i < task.NReduce; i++ {
		str, err := json.Marshal(intermediate[i])
		if err != nil {
			return err
		}

		if err := ioutil.WriteFile(fmt.Sprintf("intermediate/mr-%d-%d", task.ID, i), str, 0777); err != nil {
			return err
		}
	}

	return nil
}

// PullTask pull a new task from master
func PullTask() (*Task, bool) {

	// declare an argument structure.
	args := RequestTaskArgs{}

	// declare a reply structure.
	reply := RequestTaskReply{}

	// send the RPC request, wait for the reply.
	finished := call("Master.RequestTask", &args, &reply)

	return reply.Task, finished
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
