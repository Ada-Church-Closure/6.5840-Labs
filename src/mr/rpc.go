package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

// TaskType
const (
	MapTask = iota
	ReduceTask
	WaitTask
	ExitTask
)

// TaskStatus(in thesis)
const (
	Idle = iota
	InProgress
	Completed
)

// When a worker is idle, it asks the master node to get some tasks.
// It could be null, but we should define it.
type AskTaskArgs struct{}

// Master Node give some tasks to a Worker.
type AskTaskReply struct {
	TaskType int
	TaskID   int
	FileName string
	NMap     int
	NReduce  int // A Map worker needs to generate NReduce files.
}

// After worker ends, reports the task.
type ReportTaskArgs struct {
	TaskType int
	TaskID   int
}

type ReportTaskReply struct{}
