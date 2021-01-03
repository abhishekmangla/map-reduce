package mr

import (
	"log"
	"math"
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
	mapTaskStatus    map[string]Status // map a filename to the pid processing it, 0=unassigned, 1=inprogress, 2=done
	numPartitions    int               // number of partitions of map K-V pairs for later reduction
	taskTaskNoMap    map[string]int    // map task to task number
	reduceTaskStatus map[string]Status // status of operation on partition 0=idle, 1=inprogress, 2=done
	mapDirectory     string
	reduceDirectory  string
	mutex            sync.Mutex
	phase            string
}

type Status struct {
	status       int
	timeAssigned int64
	pid          int
}

// Your code here -- RPC handlers for the worker to call.

func shouldSleep(statusMap map[string]Status) int {
	var minSleepTime float64 = 10
	for _, element := range statusMap {
		if element.status == 0 {
			return 0
		} else if element.status == 1 {
			minSleepTime = math.Min(minSleepTime, float64(10-(time.Now().Unix()-element.timeAssigned)))
		}
	}
	return int(minSleepTime)
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) RequestTask(args *GetTaskArgs, reply *TaskResponse) error {
	// Check args for finish task information
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.Phase = m.phase

	if m.phase == "map" {
		reply.Sleep = shouldSleep(m.mapTaskStatus)
		reply.NumPartitions = m.numPartitions
		reply.MapDirectory = m.mapDirectory
		for key, element := range m.mapTaskStatus {
			if element.status == 0 {
				log.Printf("Map Task %s is unassigned. Assigning to %d\n", key, args.Pid)
				m.mapTaskStatus[key] = Status{1, time.Now().Unix(), args.Pid}
				reply.Task = key
				reply.TaskNo = m.taskTaskNoMap[key]
				return nil
			}
		}
	} else if m.phase == "reduce" {
		reply.Sleep = shouldSleep(m.reduceTaskStatus)
		reply.ReduceDirectory = m.reduceDirectory
		for key, element := range m.reduceTaskStatus {
			if element.status == 0 {
				log.Printf("Reduce Task %s is unassigned. Assigning to %d\n", key, args.Pid)
				m.reduceTaskStatus[key] = Status{1, time.Now().Unix(), args.Pid}
				// var err error
				reply.Task = key
				// if err != nil {
				// 	log.Fatalln(err)
				// }
				reply.MapDirectory = m.mapDirectory
				reply.TotalNumMapTasks = len(m.mapTaskStatus)
				reply.ReduceDirectory = m.reduceDirectory
				return nil
			}

		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) NotifyTaskComplete(args *NotifyTaskCompleteArgs, reply *NotifyTaskCompleteResponse) error {
	// Check args for finish task information
	m.mutex.Lock()
	defer m.mutex.Unlock()
	reply.Ack = false

	if args.Phase == "map" {
		oldStruct := m.mapTaskStatus[args.Task]
		m.mapTaskStatus[args.Task] = Status{2, oldStruct.timeAssigned, oldStruct.pid}
		for _, el := range args.Partitions {
			m.reduceTaskStatus[strconv.Itoa(el)] = Status{0, 0, 0}
		}
		log.Printf("Map Task %s finished by %d\n", args.Task, args.Pid)
		reply.Ack = true
	} else if args.Phase == "reduce" {
		task := args.Task
		oldStruct := m.reduceTaskStatus[task]
		m.reduceTaskStatus[task] = Status{2, oldStruct.timeAssigned, oldStruct.pid}
		log.Printf("Reduce Task %s finished by %d\n", task, args.Pid)
		reply.Ack = true
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
	m.mutex.Lock()
	defer m.mutex.Unlock()
	for key, element := range m.mapTaskStatus {
		if element.status == 1 && (time.Now().Unix()-element.timeAssigned) > 10 {
			m.mapTaskStatus[key] = Status{0, 0, 0}
		}
	}
	for key, element := range m.reduceTaskStatus {
		if element.status == 1 && (time.Now().Unix()-element.timeAssigned) > 10 {
			m.reduceTaskStatus[key] = Status{0, 0, 0}
		}
	}
	for _, element := range m.mapTaskStatus {
		if element.status != 2 {
			return false
		}
	}
	m.phase = "reduce"
	log.Printf("%+v", m.reduceTaskStatus)
	for _, element := range m.reduceTaskStatus {
		if element.status != 2 {
			return false
		}
	}
	// os.RemoveAll(m.mapDirectory)
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	// log.Printf("Num tasks: %d\n", len(files))
	mapTaskStatus := make(map[string]Status)
	taskTaskNoMap := make(map[string]int)
	reduceTaskStatus := make(map[string]Status)
	cwd, _ := os.Getwd()
	mapDirectory := cwd + "/mapOutput"
	reduceDirectory := cwd
	_ = os.Mkdir(mapDirectory, 0777)
	_ = os.Mkdir(reduceDirectory, 0777)
	for i := 0; i < len(files); i++ {
		mapTaskStatus[files[i]] = Status{0, 0, 0}
		taskTaskNoMap[files[i]] = i
	}
	// for i := 0; i < nReduce; i++ {
	// 	reduceTaskStatus[strconv.Itoa(i)] = Status{0, 0, 0}
	// }
	m := Master{}
	m.mapTaskStatus = mapTaskStatus
	m.numPartitions = nReduce
	m.taskTaskNoMap = taskTaskNoMap
	m.reduceTaskStatus = reduceTaskStatus
	m.phase = "map"
	m.mapDirectory = mapDirectory
	m.reduceDirectory = reduceDirectory
	m.server()
	return &m
}
