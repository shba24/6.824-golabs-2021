package mr

/*
RPC definitions.
*/

import "os"
import "strconv"

type TaskType int
type Status int

const (
	MAP TaskType = iota
	REDUCE
)

const (
	SUCCESS Status = iota
	FAILED
	RUNNING
	PENDING
)

type TaskInfo struct {
	TaskType  TaskType
	Id        int
	InputPath string
}

type AllocateTaskReq struct {
	WorkerId string
}

type AllocateTaskResp struct {
	TaskInfo *TaskInfo
	NReduce int
	NMap int
}

type FinishTaskReq struct {
	TaskInfo   *TaskInfo
}

type FinishTaskResp struct {
	// None
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
