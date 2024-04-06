package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
)
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	IsMapper bool
	FileName string
	NReduce  int
	TaskId   int

	IsDoneTask bool
}

func newMapperTask(mapTaskId int, fileName string, nReduce int) *GetTaskReply {
	return &GetTaskReply{
		IsMapper: true,
		FileName: fileName,
		NReduce:  nReduce,
		TaskId:   mapTaskId,
	}
}

func newReducerTask(reduceTaskId int) *GetTaskReply {
	return &GetTaskReply{
		IsMapper: false,
		TaskId:   reduceTaskId,
	}
}

var doneTask *GetTaskReply = &GetTaskReply{
	IsDoneTask: true,
}

type Dummy struct{}

// Add your RPC definitions here.
func (c *Coordinator) GetTask(dummy *GetTaskArgs, task *GetTaskReply) error {
	var reply = <-c.TaskChan
	*task = *reply
	return nil
}

type TaskDoneArgs struct {
	IsMapper bool
	TaskId   int
}

type TaskDoneReply struct {
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {

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
