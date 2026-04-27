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

	mapTasks    []TaskMeta // record the state of a task.
	reduceTasks []TaskMeta

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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.isMapDone && c.isReduceDone
}

// a idle worker invoke this RPC to get a task.
// think: what kind of task we need to give right now.
func (c *Coordinator) AskForTasksHandler(args *AskTaskArgs, reply *AskTaskReply) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	// traverse the map tasks.
	// mapping phase
	if !c.isMapDone {
		for index := 0; index < len(c.mapTasks); index++ {
			if (c.mapTasks[index].TaskStatus == Idle) ||
				(c.mapTasks[index].TaskStatus == InProgress && time.Since(c.mapTasks[index].StartTime) > 10*time.Second) {
				reply.TaskType = MapTask
				reply.TaskID = index
				reply.FileName = c.mapFiles[index]
				reply.NMap = len(c.mapTasks)
				reply.NReduce = c.nReduce

				c.mapTasks[index].TaskStatus = InProgress
				c.mapTasks[index].StartTime = time.Now()
				return nil
			}
		}

		// all map tasks are inprogress but not expired or just completed.
		reply.TaskType = WaitTask
		return nil
	}

	// currently, all map tasks are done.
	if !c.isReduceDone {
		for index := 0; index < c.nReduce; index++ {
			if (c.reduceTasks[index].TaskStatus == Idle) ||
				(c.reduceTasks[index].TaskStatus == InProgress && time.Since(c.reduceTasks[index].StartTime) > 10*time.Second) {
				reply.TaskType = ReduceTask
				reply.TaskID = index
				// Reduce reads mr-*-<TaskID>; nReduce is unrelated to len(mapFiles).
				reply.NMap = len(c.mapTasks)
				reply.NReduce = c.nReduce
				c.reduceTasks[index].TaskStatus = InProgress
				c.reduceTasks[index].StartTime = time.Now()
				return nil
			}
		}

		reply.TaskType = WaitTask
		return nil
	}

	// all map tasks and reduce tasks are completed.
	reply.TaskType = ExitTask
	return nil
}

// when a task is done, the worker will invoke this.
func (c *Coordinator) ReportTaskDoneHandler(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		// avoid a task is executed several times.
		if c.mapTasks[args.TaskID].TaskStatus == Completed {
			return nil
		}
		c.mapTasks[args.TaskID].TaskStatus = Completed
		c.mapTaskDoneNumber++
		if c.mapTaskDoneNumber == len(c.mapTasks) {
			c.isMapDone = true
		}
	case ReduceTask:
		if c.reduceTasks[args.TaskID].TaskStatus == Completed {
			return nil
		}
		c.reduceTasks[args.TaskID].TaskStatus = Completed
		c.reduceTaskDoneNumber++
		if c.reduceTaskDoneNumber == c.nReduce {
			c.isReduceDone = true
		}
	}

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
		reduceTasks:          make([]TaskMeta, nReduce),
		mapTaskDoneNumber:    0,
		reduceTaskDoneNumber: 0,
		isMapDone:            false,
		isReduceDone:         false,
	}

	c.server(sockname)
	return &c
}
