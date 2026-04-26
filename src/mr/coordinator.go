package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// The Master Node in the mapreduce.

type Coordinator struct {
	mu sync.Mutex // Protect states of shared variables.

	mapFiles []string
	nReduce  int // finally, we generate nReduce files.

	mapTasks   []TaskMeta // record the state of a task.
	reduceTask []int

	mapTaskDoneNumber    int // how many tasks are done.
	reduceTaskDoneNumber int

	// two phase of tasks
	isMapDone    bool
	isReduceDone bool
}

// the metadata of a task.
type TaskMeta struct {
	TaskStatus int
	StartTime  time.Time // if in inprogress more than 10s. re idle.
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// a idle worker invoke this RPC to get a task.
// think: what kind of task we need to give right now.
func (c *Coordinator) AskForTasksHandler(args *AskTaskArgs, reply *AskTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// traverse the map tasks.

	return nil
}

func (c *Coordinator) ReportTaskDoneHandler(args *ReportTaskArgs, reply *ReportTaskReply) error {

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// Init Master Node
	c := Coordinator{
		// force init a new area of memory here.
		mapFiles:             append([]string(nil), files...),
		nReduce:              nReduce,
		mapTasks:             make([]TaskMeta, len(files)),
		reduceTask:           make([]int, nReduce),
		mapTaskDoneNumber:    0,
		reduceTaskDoneNumber: 0,
		isMapDone:            false,
		isReduceDone:         false,
	}

	c.server(sockname)
	return &c
}
