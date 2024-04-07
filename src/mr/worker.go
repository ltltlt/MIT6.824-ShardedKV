package mr

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	log.SetPrefix("Worker ")

	for true {
		// Attention: we cannot reuse struct, because golang won't pass zero value
		// when server reply a:0, the server won't send it back to client,
		// which means if the current value of reply is 1, it won't be overwrite to 0
		reply := GetTaskReply{}
		ok := call("Coordinator.GetTask", &GetTaskArgs{}, &reply)
		if ok {
			log.Printf("Got task assignment: %+v", reply)
		}
		if !ok || (reply.TaskKind != 1 && reply.TaskKind != 2) {
			log.Printf("Receive done task or socket is closed, exit...")
			return
		}
		if reply.TaskKind == 1 {
			runMapper(mapf, &reply)
		} else {
			runReducer(reducef, &reply)
		}
	}
}

func runMapper(mapf func(string, string) []KeyValue, reply *GetTaskReply) {
	file := reply.FileName
	content, err := os.ReadFile(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Run mapper failed because unable to read file %s", file)
		return
	}
	handlers := make([]*os.File, 0, reply.NReduce)
	encoders := make([]*json.Encoder, 0, reply.NReduce)
	success := true
	for i := 0; i < reply.NReduce; i++ {
		handler, err := os.CreateTemp("/tmp", fmt.Sprintf("mapper-%v-%v", os.Getpid(), i))
		if err != nil {
			log.Printf("Run mapper failed because unable to create temp file, err: %v\n", err)
			success = false
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

	sort.Sort(ByKey(results))

	// write results to file
	log.Printf("Write mapper output, records: %v\n", len(results))
	for _, result := range results {
		key := result.Key
		hash := ihash(key) % reply.NReduce
		err := encoders[hash].Encode(result)
		if err != nil {
			log.Printf("Failed to encode json, err: %v", err)
			return
		}
	}
	for i := 0; i < reply.NReduce; i++ {
		// go can close file multiple times
		handlers[i].Close()
		err := os.Rename(handlers[i].Name(), fmt.Sprintf("Mapper-%v-%v", i, reply.TaskId))
		if err != nil {
			// TODO: we might need to do some cleanup here
			log.Println("Failed to rename mapper output file")
			return
		}
	}
	callTaskDone(reply)
}

func runReducer(reducef func(string, []string) string, task *GetTaskReply) {
	pattern := fmt.Sprintf("Mapper-%v-", task.ReduceTaskNo)

	records := []KeyValue{}
	err := filepath.Walk("./", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Got error %v while walk directory for mapper output\n", err)
			return err
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), pattern) {
			kvp, err := readRecordsFromFile(info, reducef)
			if err != nil {
				log.Printf("failed to run reduce for each mapper file: %s, err: %v\n", info.Name(), err)
				return err
			}
			records = append(records, kvp...)
		}
		return nil
	})
	if err != nil {
		log.Printf("failed to walk directory to read mapper output, err: %v\n", err)
		return
	}

	sort.Sort(ByKey(records))
	err = runReduce(records, reducef, task)
	if err != nil {
		log.Printf("failed to reduce on records, err: %v", err)
		return
	}

	callTaskDone(task)
}

func readRecordsFromFile(info os.FileInfo, reducef func(string, []string) string) ([]KeyValue, error) {
	filename := info.Name()
	file, err := os.OpenFile(filename, os.O_RDONLY, 0644)
	if err != nil {
		log.Printf("failed to read mapper output file %v, err: %v\n", filename, err)
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	kv := KeyValue{}
	records := []KeyValue{}
	for decoder.More() {
		err := decoder.Decode(&kv)
		if err != nil {
			log.Printf("failed to decode file %v, err: %v\n", filename, err)
			return nil, err
		}
		records = append(records, kv)
	}
	return records, nil
}

func runReduce(records []KeyValue, reducef func(string, []string) string, task *GetTaskReply) error {
	temp, err := os.CreateTemp("/tmp", fmt.Sprintf("reducer-%v-%v", os.Getpid(), task.ReduceTaskNo))
	if err != nil {
		log.Printf("Failed to create temp file for reducer, err: %v", err)
		return err
	}
	for i := 0; i < len(records); {
		key := records[i].Key
		end := i + 1
		values := []string{records[i].Value}
		for end < len(records) && key == records[end].Key {
			values = append(values, records[end].Value)
			end++
		}
		result := reducef(key, values)
		fmt.Fprintf(temp, "%v %v\n", key, result)
		i = end
	}

	err = temp.Close()
	if err != nil {
		log.Printf("failed to close temp file, err: %v\n", err)
		return err
	}
	err = os.Rename(temp.Name(), fmt.Sprintf("mr-out-%v", task.ReduceTaskNo))
	if err != nil {
		log.Printf("failed to rename reducer output file, err: %v\n", err)
		return err
	}
	return nil
}

func callTaskDone(task *GetTaskReply) {
	args := TaskDoneArgs{TaskId: task.TaskId, TaskKind: task.TaskKind}
	doneReply := TaskDoneReply{}
	ok := call("Coordinator.TaskDone", &args, &doneReply)
	id := task.TaskId
	if task.TaskKind == 2 {
		id = task.ReduceTaskNo
	}
	if ok {
		log.Printf("Success to register task done, taskKind: %v, id: %v\n", task.TaskKind, id)
	} else {
		log.Printf("Failed to register task done, taskKind: %v, id: %v\n", task.TaskKind, id)
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
