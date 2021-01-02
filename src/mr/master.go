package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Master struct {
	// Your definitions here.
	taskMap         map[string]Status // map a filename to the pid processing it, 0=unassigned, 1=inprogress, 2=done
	numPartitions   int               // number of partitions of map K-V pairs for later reduction
	taskTaskNoMap   map[string]int    // map task to task number
	partitionStatus map[int]Status    // status of operation on partition 0=idle, 1=inprogress, 2=done
	doneMap         bool              // Done with map work
	mapDirectory    string
	reduceDirectory string
	mapMutex        sync.Mutex
	reduceMutex     sync.Mutex
}

type Status struct {
	status       int
	timeAssigned int64
	pid          int
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
		m.mapMutex.Lock()
		log.Printf("Process Id: %d finished task %s\n", args.Pid, args.TaskName)
		oldStruct := m.taskMap[args.TaskName]
		m.taskMap[args.TaskName] = Status{2, oldStruct.timeAssigned, oldStruct.pid}
		m.mapMutex.Unlock()
	}
	if args.ReduceFinish {
		m.reduceMutex.Lock()
		log.Printf("Process Id: %d finished reduce task %s\n", args.Pid, args.TaskName)
		taskNo, _ := strconv.Atoi(args.TaskName)
		oldStruct := m.partitionStatus[taskNo]
		m.partitionStatus[taskNo] = Status{2, oldStruct.timeAssigned, oldStruct.pid}
		// m.partitionStatus[taskNo] = 2
		m.reduceMutex.Unlock()
	}
	m.mapMutex.Lock()
	defer m.mapMutex.Unlock()
	if !m.doneMap {
		// Try sending a response back with a new task to the worker
		log.Printf("Process Id: %d requesting a map task...\n", args.Pid)

		for key, element := range m.taskMap {
			if element.status == 0 {
				now := time.Now()
				log.Printf("Map Task %s is unassigned. Assigning to %d\n", key, args.Pid)
				m.taskMap[key] = Status{1, now.Unix(), args.Pid}
				reply.Task = key
				reply.TaskNo = m.taskTaskNoMap[key]
				reply.NumPartitions = m.numPartitions
				reply.MapDirectory = m.mapDirectory
				return nil
			}
		}

		// Check if all map processes are finished
		for _, element := range m.taskMap {
			if element.status != 2 {
				return nil
			}
		}
		m.doneMap = true
	}

	m.reduceMutex.Lock()
	defer m.reduceMutex.Unlock()
	// Try to assign reduce tasks
	for idx := 0; idx < m.numPartitions; idx++ {
		status := m.partitionStatus[idx]
		if status.status == 0 {
			log.Printf("Reduce Task %d is unassigned. Assigning to %d\n", idx, args.Pid)
			// oldStruct := m.partitionStatus[idx]
			m.partitionStatus[idx] = Status{1, time.Now().Unix(), args.Pid}
			// m.partitionStatus[idx] = 1
			// log.Printf("%v\n", m.partitionStatus)
			reply.TaskNo = idx
			reply.MapDirectory = m.mapDirectory
			reply.TotalNumMapTasks = len(m.taskMap)
			reply.ReduceDirectory = m.reduceDirectory
			return nil
		}

	}
	log.Println("No reduce tasks are unassigned...")
	// Check if all reduce processes are finished
	for _, element := range m.partitionStatus {
		if element.status != 2 {
			return nil
		}
	}
	log.Println("All reduce tasks are completed...")

	reply.TaskNo = -1

	// reply.Task should be nil when all tasks in the map are -2.
	return nil
}

// CheckMapDone
func (m *Master) CheckMapDone(args *ExampleArgs, reply *MapStatusReply) error {
	reply.Done = true
	for _, elem := range m.taskMap {
		if elem.status != 2 {
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
	// Check map status and partition status init_epoch_time, if current time >
	// init_epoch_time + 10 seconds, open that task up for re-tasking.
	m.mapMutex.Lock()
	m.reduceMutex.Lock()

	defer m.mapMutex.Unlock()
	defer m.reduceMutex.Unlock()
	for key, element := range m.taskMap {
		now := time.Now()
		if element.status == 1 && (now.Unix()-element.timeAssigned) > 10 {
			m.taskMap[key] = Status{0, 0, 0}
		}
	}
	for key, element := range m.partitionStatus {
		now := time.Now()
		if element.status == 1 && (now.Unix()-element.timeAssigned) > 10 {
			m.partitionStatus[key] = Status{0, 0, 0}
		}
	}
	for _, element := range m.partitionStatus {
		if element.status != 2 {
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
	// log.Printf("Num tasks: %d\n", len(files))
	taskMap := make(map[string]Status)
	taskTaskNoMap := make(map[string]int)
	partitionStatus := make(map[int]Status)
	cwd, _ := os.Getwd()
	mapDirectory := cwd + "/mapOutput"
	reduceDirectory := cwd + "/reduceOutput"
	_ = os.Mkdir(mapDirectory, 0777)
	_ = os.Mkdir(reduceDirectory, 0777)
	for i := 0; i < len(files); i++ {
		taskMap[files[i]] = Status{0, 0, 0}
		taskTaskNoMap[files[i]] = i
	}
	for i := 0; i < nReduce; i++ {
		partitionStatus[i] = Status{0, 0, 0}
	}
	m := Master{}
	m.taskMap = taskMap
	m.numPartitions = nReduce
	m.taskTaskNoMap = taskTaskNoMap
	m.partitionStatus = partitionStatus
	m.doneMap = false
	m.mapDirectory = mapDirectory
	m.reduceDirectory = reduceDirectory
	m.server()
	return &m
}
