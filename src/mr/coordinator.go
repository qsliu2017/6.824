package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	m sync.Mutex

	Files         []string
	mapDispatched []bool
	mapDone       []bool

	NReduce          int
	reduceDispatched []bool
	reduceDone       []bool
}

func Fisrt[T any](array []T, pred func(T) bool) (int, bool) {
	for i, v := range array {
		if pred(v) {
			return i, true
		}
	}
	return 0, false
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	_, someMapUndone := Fisrt(c.mapDone, func(done bool) bool { return !done })

	if someMapUndone {
		id, someUndispatched := Fisrt(c.mapDispatched, func(dispatched bool) bool { return !dispatched })

		if someUndispatched {
			c.mapDispatched[id] = true
			reply.Type = MapType
			reply.MapTask = &MapTask{
				Id:       id,
				NReduce:  c.NReduce,
				Filename: c.Files[id],
			}
			return nil
		}

		// all map tasks dispatched, wait
		reply.Type = WaitType
		return nil
	}

	id, someReduceDispatched := Fisrt(c.reduceDispatched, func(dispatched bool) bool { return !dispatched })
	if someReduceDispatched {
		c.reduceDispatched[id] = true
		reply.Type = ReduceType
		reply.ReduceTask = &ReduceTask{
			Id:   id,
			NMap: len(c.Files),
		}
		return nil
	}

	reply.Type = DoneType
	return nil
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	switch args.Type {
	case MapType:
		c.mapDone[args.Id] = true
	case ReduceType:
		c.reduceDone[args.Id] = true
	default:
		return fmt.Errorf("unexpected done task type: %v", args.Type)
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.m.Lock()
	defer c.m.Unlock()

	_, someReduceUndone := Fisrt(c.reduceDone, func(done bool) bool { return !done })
	return !someReduceUndone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:            files,
		mapDispatched:    make([]bool, len(files)),
		mapDone:          make([]bool, len(files)),
		NReduce:          nReduce,
		reduceDispatched: make([]bool, nReduce),
		reduceDone:       make([]bool, nReduce),
	}

	// Your code here.

	c.server()
	return &c
}
