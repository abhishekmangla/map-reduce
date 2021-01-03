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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type GetTaskArgs struct {
	Pid          int
	Finish       bool
	TaskName     string
	TaskNo       int
	ReduceFinish bool
	ReducePhase  bool
}

type TaskResponse struct {
	Task             string
	TaskNo           int
	TotalNumMapTasks int
	NumPartitions    int
	MapDirectory     string
	ReduceDirectory  string
	Done             bool
	Sleep            int
	Phase            string
}

type NotifyTaskCompleteArgs struct {
	Pid        int
	Phase      string
	Task       string
	TaskNo     int
	Partitions []int
}

type NotifyTaskCompleteResponse struct {
	Ack bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
