package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type Mapf func(filename string, contents string) []KeyValue
type Reducef func(key string, values []string) string

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

// Add your RPC definitions here.
type TaskType string

const (
	MapType    TaskType = "map"
	ReduceType TaskType = "reduce"
	WaitType   TaskType = "wait" // if all available tasks are dispatched but not yet done
	DoneType   TaskType = "done" // all tasks done
)

type MapTask struct {
	Id       int
	NReduce  int
	Filename string
}

type ReduceTask struct {
	Id   int
	NMap int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	Type       TaskType
	MapTask    *MapTask
	ReduceTask *ReduceTask
}

type DoneTaskArgs struct {
	Type TaskType
	Id   int
}

type DoneTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
