package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

type Master struct {
	// Your definitions here.
	taskMap         map[string]int // map a filename to the pid processing it
	numPartitions   int            // number of partitions of map K-V pairs for later reduction
	taskTaskNoMap   map[string]int // map task to task number
	partitionStatus map[int]int    // status of operation on partition 0=idle, 1=inprogress, 2=done
	doneMap         bool           // Done with map work
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) RequestTask(args *ExampleArgs, reply *ExampleReply) error {
	// Check args for finish task information
	if args.Finish {
		fmt.Printf("Process Id: %d finished task %s\n", args.Pid, args.TaskName)
		m.taskMap[args.TaskName] = -2
	}
	if args.ReduceFinish {
		fmt.Printf("Process Id: %d finished reduce task %s\n", args.Pid, args.TaskName)
		taskNo, _ := strconv.Atoi(args.TaskName)
		m.partitionStatus[taskNo] = 2
	}
	if !m.doneMap {
		// Try sending a response back with a new task to the worker
		fmt.Printf("Process Id: %d requesting a map task...\n", args.Pid)
		for key, element := range m.taskMap {
			if element == -1 {
				fmt.Printf("Map Task %s is unassigned. Assigning to %d\n", key, args.Pid)
				m.taskMap[key] = args.Pid
				reply.Task = key
				reply.TaskNo = m.taskTaskNoMap[key]
				reply.NumPartitions = m.numPartitions
				return nil
			}
		}

		// Check if all map processes are finished
		for _, element := range m.taskMap {
			if element != -2 {
				return nil
			}
		}
		m.doneMap = true
	}

	// Try to assign reduce tasks

	for idx := 0; idx < m.numPartitions; idx++ {
		status := m.partitionStatus[idx]
		if status == 0 {
			fmt.Printf("Reduce Task %d is unassigned. Assigning to %d\n", idx, args.Pid)
			m.partitionStatus[idx] = 1
			fmt.Printf("%v\n", m.partitionStatus)
			reply.TaskNo = idx
			// reply.NumPartitions = m.numPartitions
			return nil
		}

	}
	fmt.Println("No reduce tasks are unassigned...")
	// Check if all reduce processes are finished
	for _, element := range m.partitionStatus {
		if element != 2 {
			return nil
		}
	}
	fmt.Println("All reduce tasks are completed...")

	reply.TaskNo = -1

	// reply.Task should be nil when all tasks in the map are -2.
	return nil
}

// CheckMapDone
func (m *Master) CheckMapDone(args *ExampleArgs, reply *ExampleReply) error {
	reply.Done = true
	for _, elem := range m.taskMap {
		if elem != -2 {
			reply.Done = false
			break
		}
	}
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	for _, element := range m.partitionStatus {
		if element != 2 {
			return false
		}
	}
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	fmt.Printf("Num tasks: %d\n", len(files))
	// Your code here.
	taskMap := make(map[string]int)
	taskTaskNoMap := make(map[string]int)
	partitionStatus := make(map[int]int)
	for i := 0; i < len(files); i++ {
		fmt.Printf("Task name: %d %s\n", i, files[i])
		taskMap[files[i]] = -1
		taskTaskNoMap[files[i]] = i
	}
	for i := 0; i < nReduce; i++ {
		partitionStatus[i] = 0
	}
	m := Master{taskMap, nReduce, taskTaskNoMap, partitionStatus, false}

	m.server()
	return &m
}
