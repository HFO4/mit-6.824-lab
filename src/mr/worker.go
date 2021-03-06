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
	"sort"

	"github.com/google/uuid"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
			res, err := ProcessTask(task, mapf, reducef)
			if err != nil {
				fmt.Println("Failed task:", err)
				// Skip failed task
				continue
			}

			// Notify master task complete
			call("Master.TaskDone", res, nil)
		} else {
			fmt.Println("Worker quit")
			return
		}
	}

}

// ProcessTask process a task
func ProcessTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) (reply *NotifyTaskDoneArgs, err error) {
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
	case ReduceTask:
		return processReduceTask(task, reducef)
	default:
		return nil, errors.New("Unknown task type")
	}
}

func processReduceTask(task *Task, reducef func(string, []string) string) (*NotifyTaskDoneArgs, error) {
	// init intermediate values
	intermediate := make([]KeyValue, 0)
	for _, input := range task.Input {
		content, err := ioutil.ReadFile(input)
		if err != nil {
			return nil, err
		}

		var kv []KeyValue
		if err := json.Unmarshal(content, &kv); err != nil {
			return nil, err
		}

		intermediate = append(intermediate, kv...)
	}

	// sort by keys
	sort.Sort(ByKey(intermediate))

	// performe reduce and write to temp file
	oname := "mr_temp_" + uuid.New().String()
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	// close file
	ofile.Close()

	reply := &NotifyTaskDoneArgs{
		Output: []string{oname},
		Task:   *task,
	}
	return reply, nil
}

func processMapTask(task *Task, mapf func(string, string) []KeyValue) (*NotifyTaskDoneArgs, error) {
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
			return nil, err
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return nil, err
		}

		file.Close()
		kva := mapf(filename, string(content))

		for _, kv := range kva {
			bucketID := ihash(kv.Key) % task.NReduce
			intermediate[bucketID] = append(intermediate[bucketID], kv)
		}
	}

	reply := &NotifyTaskDoneArgs{
		Output: make([]string, task.NReduce),
		Task:   *task,
	}

	// Stroe intermediate values to disk
	for i := 0; i < task.NReduce; i++ {
		str, err := json.Marshal(intermediate[i])
		if err != nil {
			return nil, err
		}

		outputPath := fmt.Sprintf("mr-%d-%d", task.ID, i)
		if err := ioutil.WriteFile(outputPath, str, 0777); err != nil {
			return nil, err
		}
		reply.Output[i] = outputPath
	}

	return reply, nil
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
