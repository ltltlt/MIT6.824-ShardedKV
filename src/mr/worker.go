package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	reply := GetTaskReply{}
	for true {
		ok := call("Coordinator.GetTask", &GetTaskArgs{}, &reply)
		if !ok || reply.IsDoneTask {
			return
		}
		if reply.IsMapper {

		}
	}
}

func runMapper(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	pattern := fmt.Sprintf("/tmp/Worker%n", os.Getpid())

	file := reply.FileName
	content, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Run mapper failed because unable to read file %s", file)
		return
	}
	handlers := make([]*os.File, 0, reply.NReduce)
	encoders := make([]*json.Encoder, 0, reply.NReduce)
	success := false
	for i := 0; i < reply.NReduce; i++ {
		handler, err := os.CreateTemp(pattern, strconv.Itoa(i))
		if err != nil {
			log.Println("Run mapper failed because unable to create temp file")
			break
		}
		handlers = append(handlers, handler)
		encoders = append(encoders, json.NewEncoder(handler))
	}
	defer func() {
		for _, handle := range handlers {
			handle.Close()
			os.Remove(handle.Name())
		}
	}()
	if !success {
		return
	}

	results := mapf(file, string(content))

	// write results to file
	for _, result := range results {
		key := result.Key
		hash := ihash(key) % reply.NReduce
		err := encoders[hash].Encode(result)
		if err != nil {
			log.Println("Failed to encode json")
			return
		}
	}
	for i := 0; i < reply.NReduce; i++ {
		// go can close file multiple times
		handlers[i].Close()
		err := os.Rename(handlers[i].Name(), fmt.Sprintf("Mapper-%n-%n", reply.MapTaskId, i))
		if err != nil {
			log.Println("Failed to rename mapper output file")
			return
		}
	}
	callTaskDone(reply)
}

func callTaskDone(reply *GetTaskReply) {
	args := TaskDoneArgs{TaskId: reply.MapTaskId}
	doneReply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &doneReply)
	if ok {
		log.Printf("Success to register task done, id: %n\n", reply.MapTaskId)
	} else {
		log.Printf("Failed to register task done, id: %n\n", reply.MapTaskId)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
