package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	m sync.Mutex

	mt []mapTask

	rt []reduceTask
}

func (c *Coordinator) NMap() int    { return len(c.mt) }
func (c *Coordinator) NReduce() int { return len(c.rt) }

type mapTask struct {
	task
	file string
}

type reduceTask struct {
	task
}

type task struct {
	ping   chan struct{}
	done   chan struct{}
	state  taskState
	worker int
}

type taskState int

const (
	IDLE taskState = iota
	IN_PROGRESS
	COMPLETED
)

func (s taskState) String() string {
	switch s {
	case IDLE:
		return "idle"
	case IN_PROGRESS:
		return "in-progress"
	case COMPLETED:
		return "completed"
	default:
		return "unknown"
	}
}

func Fisrt[T any](array []T, pred func(T) bool) (int, *T) {
	for i, v := range array {
		if pred(v) {
			return i, &array[i]
		}
	}
	return 0, nil
}

func Timeout[T any](ch <-chan T, d time.Duration, timeoutF func()) {
	select {
	case <-ch:
	case <-time.After(d):
		timeoutF()
	}
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	if _, mIncompleted := Fisrt(c.mt, func(m mapTask) bool { return m.state != COMPLETED }); mIncompleted != nil {

		if id, m := Fisrt(c.mt, func(mt mapTask) bool { return mt.state == IDLE }); m != nil {
			m.state = IN_PROGRESS
			m.worker = args.WorkerId

			reply.Type = MapType
			reply.MapTask = &MapTask{
				Id:       id,
				NReduce:  c.NReduce(),
				Filename: m.file,
			}
			go c.acceptHeartbeat(&m.task)
			return nil
		}

		// all map tasks in progress, wait
		reply.Type = WaitType
		return nil
	}

	if _, rIncompleted := Fisrt(c.rt, func(r reduceTask) bool { return r.state != COMPLETED }); rIncompleted != nil {

		if id, r := Fisrt(c.rt, func(r reduceTask) bool { return r.state == IDLE }); r != nil {
			r.state = IN_PROGRESS
			r.worker = args.WorkerId

			reply.Type = ReduceType
			reply.ReduceTask = &ReduceTask{
				Id:   id,
				NMap: c.NMap(),
			}
			go c.acceptHeartbeat(&r.task)
			return nil
		}

		reply.Type = WaitType
		return nil
	}

	reply.Type = DoneType
	return nil
}

func (c *Coordinator) acceptHeartbeat(t *task) {
	for {
		select {
		case <-time.After(3 * time.Second):
			// log.Printf("map task has not receive response for 3s")
			c.m.Lock()
			t.state = IDLE
			c.m.Unlock()
		case <-t.done:
			return
		case <-t.ping:
			continue
		}
	}
}

func (c *Coordinator) DoneTask(args *DoneTaskArgs, reply *DoneTaskReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	switch args.Type {
	case MapType:
		m := &c.mt[args.Id]
		m.state = COMPLETED
		m.done <- struct{}{}
	case ReduceType:
		r := &c.rt[args.Id]
		r.state = COMPLETED
		r.done <- struct{}{}
	default:
		return fmt.Errorf("unexpected done task type: %v", args.Type)
	}

	return nil
}

func (c *Coordinator) Ping(args *PingArgs, reply *PingReply) error {
	c.m.Lock()
	defer c.m.Unlock()

	switch args.Type {
	case MapType:
		m := &c.mt[args.Id]
		m.ping <- struct{}{}
	case ReduceType:
		r := &c.rt[args.Id]
		r.ping <- struct{}{}
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

	_, rIncompleted := Fisrt(c.rt, func(r reduceTask) bool { return r.state != COMPLETED })
	return rIncompleted == nil
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mt: make([]mapTask, len(files)),
		rt: make([]reduceTask, nReduce),
	}

	for i := range c.mt {
		m := &c.mt[i]
		m.file = files[i]
		m.state = IDLE
		m.done = make(chan struct{})
		m.ping = make(chan struct{})
	}

	for i := range c.rt {
		r := &c.rt[i]
		r.done = make(chan struct{})
		r.ping = make(chan struct{})
	}

	c.server()
	return &c
}
