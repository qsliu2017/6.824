package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"time"
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
func Worker(mapf Mapf, reducef Reducef) {
loop:
	for {
		t, m, r := getTask()
		switch t {
		case MapType:
			doMap(mapf, *m)
			doneTask(MapType, m.Id)
		case ReduceType:
			doReduce(reducef, *r)
			doneTask(ReduceType, r.Id)
		case WaitType:
			time.Sleep(time.Microsecond)
		case DoneType:
			break loop
		default:
			log.Fatalf("unknown task type: %v\n", t)
		}
	}
}

func getTask() (TaskType, *MapTask, *ReduceTask) {
	args := GetTaskArgs{}

	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Fatalln("get task failed!")
	}

	return reply.Type, reply.MapTask, reply.ReduceTask
}

func doMap(mapf Mapf, t MapTask) {
	contents, err := ioutil.ReadFile(t.Filename)
	if err != nil {
		log.Fatal(err)
	}
	kvs := mapf(t.Filename, string(contents))

	buckets := make([][]KeyValue, t.NReduce)
	for _, kv := range kvs {
		i := ihash(kv.Key) % t.NReduce
		if buckets[i] == nil {
			buckets[i] = []KeyValue{kv}
		} else {
			buckets[i] = append(buckets[i], kv)
		}
	}

	for i, kvs := range buckets {
		f, err := os.Create(fmt.Sprintf("mr-%v-%v", t.Id, i))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		if kvs == nil {
			continue
		}

		encoder := json.NewEncoder(f)
		for _, kv := range kvs {
			if err := encoder.Encode(kv); err != nil {
				log.Fatal(err)
			}
		}
	}
}

func doReduce(reducef Reducef, t ReduceTask) {
	kvs := make(map[string][]string)
	for i := 0; i < t.NMap; i++ {
		f, err := os.Open(fmt.Sprintf(`mr-%v-%v`, i, t.Id))
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()

		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, has := kvs[kv.Key]; !has {
				kvs[kv.Key] = []string{kv.Value}
			} else {
				kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
			}
		}
	}

	f, _ := os.Create(fmt.Sprintf("mr-out-%v", t.Id))
	defer f.Close()
	for k, vs := range kvs {
		fmt.Fprintf(f, "%v %v\n", k, reducef(k, vs))
	}
}

func doneTask(type_ TaskType, id int) {
	args := DoneTaskArgs{
		Type: type_,
		Id:   id,
	}
	reply := DoneTaskReply{}
	ok := call("Coordinator.DoneTask", &args, &reply)
	if !ok {
		log.Fatalln("done task failed!")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
