package mr

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	WorkingMapper  int32
	WorkingReducer int32
	TaskChan       chan *GetTaskReply
	DoneTasks      []bool
	mu             *sync.RWMutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return atomic.LoadInt32(&c.WorkingMapper) == 0 && atomic.LoadInt32(&c.WorkingReducer) == 0
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TaskChan:  make(chan *GetTaskReply),
		DoneTasks: make([]bool, len(files)+nReduce),
		mu:        &sync.RWMutex{},
	}

	// Your code here.

	atomic.StoreInt32(&c.WorkingMapper, int32(len(files)))
	atomic.StoreInt32(&c.WorkingReducer, int32(nReduce))

	c.server()
	return &c
}

func (c *Coordinator) createTasks(files []string, nReduce int) {
	for i, file := range files {
		c.TaskChan <- newMapperTask(i, file, nReduce)
	}
	// wait for all mapper done
	for atomic.LoadInt32(&c.WorkingMapper) > 0 {
		time.Sleep(1 * time.Second)
	}

	for i := 0; i < nReduce; i++ {
		c.TaskChan <- newReducerTask(i + len(files))
	}
	for true {
		c.TaskChan <- doneTask
	}
}
