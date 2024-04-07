package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"sync/atomic"
	"time"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

const (
	Mapper = iota + 1
	Reducer
	Complete
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskKind int
	FileName string
	NMapper  int
	NReduce  int
	TaskId   int

	ReduceTaskNo int
}

func newMapperTask(taskId int, fileName string, nReduce int) *GetTaskReply {
	return &GetTaskReply{
		TaskKind: Mapper,
		FileName: fileName,
		NReduce:  nReduce,
		TaskId:   taskId,
	}
}

func newReducerTask(taskId int, reduceTaskNo int) *GetTaskReply {
	return &GetTaskReply{
		TaskKind:     Reducer,
		TaskId:       taskId,
		ReduceTaskNo: reduceTaskNo,
	}
}

var doneTaskReply = GetTaskReply{
	TaskKind: Complete,
}

// Add your RPC definitions here.
func (c *Coordinator) GetTask(dummy *GetTaskArgs, reply *GetTaskReply) error {
	var task = <-c.TaskChan
	*reply = *task
	time.AfterFunc(time.Second*10, func() {
		c.RLock()
		isDone := c.DoneTasks[task.TaskId]
		c.RUnlock()
		if !isDone {
			log.Printf("Task is lost, regenerate, kind: %v, task id: %v, reducerid: %v\n",
				task.TaskKind, task.TaskId, task.ReduceTaskNo)
			c.TaskChan <- task
		}
	})
	return nil
}

type TaskDoneArgs struct {
	TaskKind int
	TaskId   int
}

type TaskDoneReply struct {
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	log.Printf("Receive task done, %+v\n", args)
	c.Lock()
	old := c.DoneTasks[args.TaskId]
	c.DoneTasks[args.TaskId] = true
	c.Unlock()
	if !old {
		if args.TaskKind == Mapper {
			atomic.AddInt32(&c.WorkingMapper, -1)
		} else if args.TaskKind == Reducer {
			atomic.AddInt32(&c.WorkingReducer, -1)
		}
	} else {
		log.Printf("Receive duplicate task done request, ignore...")
	}
	return nil
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
