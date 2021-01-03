package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var response TaskResponse = CallGetTask()
	for !response.Done {
		if response.Sleep > 0 {
			sleepTime := time.Duration(response.Sleep) * time.Second
			log.Printf("Sleeping for %+v seconds...\n", sleepTime)
			time.Sleep(sleepTime)
		} else {
			if response.Phase == "map" {
				partitions := performMap(mapf, response)
				CallNotifyTaskComplete(response.Phase, response.Task, response.TaskNo, partitions)
			} else if response.Phase == "reduce" {
				performReduce(reducef, response)
				CallNotifyTaskComplete(response.Phase, response.Task, response.TaskNo, nil)
				log.Printf("Notified master I completed reduce task %s\n", response.Task)
			} else {
				break
			}
		}
		response = CallGetTask()
	}
}

func CallGetTask() TaskResponse {
	args := GetTaskArgs{}
	args.Pid = os.Getpid()
	args.ReducePhase = false
	response := TaskResponse{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &response)
	log.Printf("Request Task response from master: %+v\n", response)
	return response
}

func CallNotifyTaskComplete(phase string, task string, taskNo int, partitions []int) NotifyTaskCompleteResponse {
	args := NotifyTaskCompleteArgs{}
	args.Pid = os.Getpid()
	args.Phase = phase
	args.Task = task
	args.TaskNo = taskNo
	args.Partitions = partitions
	response := NotifyTaskCompleteResponse{}

	// send the RPC request, wait for the reply.
	call("Master.NotifyTaskComplete", &args, &response)
	// log.Printf("NotifyTaskComplete response from master: %+v\n", response)
	return response
}

func performMap(mapf func(string, string) []KeyValue, response TaskResponse) []int {
	filename := response.Task
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalln(err)
	}

	// Apply map function
	kva := mapf(filename, string(content))

	var partitions = make(map[int][]KeyValue)
	for _, element := range kva {
		partition := ihash(element.Key) % response.NumPartitions
		partitions[partition] = append(partitions[partition], element)
	}
	for partition, kvList := range partitions {
		oname := "mr-out-" + fmt.Sprint(response.TaskNo) + "-" + fmt.Sprint(partition)
		ofile, err := os.Create(response.MapDirectory + "/" + oname)
		defer ofile.Close()
		if err != nil {
			log.Fatalln(err)
		}
		var totalDataWritten int = 0
		for _, kv := range kvList {
			jsonKv, err := json.Marshal(kv)
			if err != nil {
				log.Fatalln(err)
			}
			n, err := ofile.WriteString(string(jsonKv) + "\n")
			if err != nil {
				log.Fatalln(err)
			}
			totalDataWritten += n
		}
	}
	keys := make([]int, 0, len(partitions))
	for k := range partitions {
		keys = append(keys, k)
	}
	return keys
}

func performReduce(reducef func(string, []string) string, response TaskResponse) {
	// taskName := strconv.Itoa(response.TaskNo)
	var files []string
	for i := 0; i < response.TotalNumMapTasks; i++ {
		filename := response.MapDirectory + "/mr-out-" + strconv.Itoa(i) + "-" + response.Task
		files = append(files, filename)
	}
	// log.Println(files)
	ReductionStep(files, reducef, response.ReduceDirectory, "mr-out-"+response.Task)
	log.Println("Performed reduce step!")
}

// ReductionStep
func ReductionStep(ret []string, reducef func(string, []string) string, reduceDir string, oname string) {
	intermediate := []KeyValue{}
	ofile, err := ioutil.TempFile(reduceDir, oname)
	if err != nil {
		log.Fatalln(err)
	}
	log.Println(ret, len(ret))
	for _, file := range ret {
		file, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			text := scanner.Text()
			data := KeyValue{}
			json.Unmarshal([]byte(text), &data)
			intermediate = append(intermediate, data)
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

	}
	sort.Sort(ByKey(intermediate))

	// call Reduce on each distinct key in intermediate[],
	// and print the result to ofile.
	//
	i := 0
	total := 0
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

		// this is the correct format for each line of Reduce output.
		n, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalln(err)
		}
		i = j
		total += n
	}
	log.Printf("Wrote %d bytes of data to %s\n", total, ofile.Name())

	absPath, err := filepath.Abs(ofile.Name())
	if err != nil {
		log.Fatalln(err)
	}
	newPath, err := filepath.Abs(reduceDir + "/" + oname)
	if err != nil {
		log.Fatalln(err)
	}
	// log.Println(absPath, newPath)
	err = os.Rename(absPath, newPath)
	if err != nil {
		log.Fatalln(err)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println(err)
	return false
}
