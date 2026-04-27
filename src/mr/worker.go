package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// implement original sort interface.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

var coordSockName string // socket for coordinator

func reportTaskDone(taskType int, taskID int) {
	args := ReportTaskArgs{TaskType: taskType, TaskID: taskID}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTaskDoneHandler", &args, &reply)
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// global variable
	coordSockName = sockname

	// loop:as a worker, we always try to get task from the master node.
	for {
		args := &AskTaskArgs{}
		reply := &AskTaskReply{}

		ok := call("Coordinator.AskForTasksHandler", args, reply)
		if !ok {
			return
		}

		switch reply.TaskType {
		case MapTask:
			doMapTask(reply, mapf)
			reportTaskDone(MapTask, reply.TaskID)
		case ReduceTask:
			doReduceTask(reply, reducef)
			reportTaskDone(ReduceTask, reply.TaskID)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}

	}
}

// What map and reduce specificly needs to do
func doMapTask(reply *AskTaskReply, mapf func(string, string) []KeyValue) {
	fileContent, _ := os.ReadFile(reply.FileName)
	// get a lot of key value pairs.
	kvs := mapf(reply.FileName, string(fileContent))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)
	for index := 0; index < reply.NReduce; index++ {
		tempFiles[index], _ = os.CreateTemp(".", "mr-map-*")
		encoders[index] = json.NewEncoder(tempFiles[index])
	}

	for _, kv := range kvs {
		bucket := ihash(kv.Key) % reply.NReduce
		encoders[bucket].Encode(&kv)
	}

	// atomic rename files
	for index := 0; index < reply.NReduce; index++ {
		tempFiles[index].Close()
		finalFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, index)
		os.Rename(tempFiles[index].Name(), finalFileName)
	}
}

func doReduceTask(reply *AskTaskReply, reducef func(string, []string) string) {
	var intermediate []KeyValue

	// get all files that belongs to this reduce task
	for index := 0; index < reply.NMap; index++ {
		filename := fmt.Sprintf("mr-%d-%d", index, reply.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			// constantly get new key value pair from the file.
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))
	// When you create files it should in this directory.
	tempFile, _ := os.CreateTemp(".", "mr-reduce-*")
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
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	finalName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	os.Rename(tempFile.Name(), finalName)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Printf("dialing: %v", err)
		return false
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
