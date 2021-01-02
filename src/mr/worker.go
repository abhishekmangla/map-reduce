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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var reply ExampleReply
	var finish = false
	var finishedTask = ""

	for reply = CallExample(finish, finishedTask); reply.Task != ""; reply = CallExample(finish, finishedTask) {
		// Process input file
		filename := reply.Task
		file, _ := os.Open(filename)
		content, _ := ioutil.ReadAll(file)
		file.Close()

		// Apply map function
		kva := mapf(filename, string(content))

		var partitions = make(map[int][]KeyValue)
		for _, element := range kva {
			partition := ihash(element.Key) % reply.NumPartitions
			partitions[partition] = append(partitions[partition], element)
		}
		for partition, kvList := range partitions {
			oname := "mr-out-" + fmt.Sprint(reply.TaskNo) + "-" + fmt.Sprint(partition)
			ofile, err := os.Create(reply.MapDirectory + "/" + oname)
			if err != nil {
				log.Fatalln(err)
			}
			var totalDataWritten int = 0
			for _, kv := range kvList {
				jsonKv, _ := json.Marshal(kv)
				n, _ := ofile.WriteString(string(jsonKv) + "\n")
				totalDataWritten += n
			}
			// log.Printf("Wrote %d data to %s\n", totalDataWritten, ofile.Name())
			ofile.Close()
		}
		finish = true
		finishedTask = filename
	}

	var newReply = MapStatusReply{}
	log.Println("Check if all maps are done...")
	for !newReply.Done {
		time.Sleep(time.Second * 2)
		newReply = CallCheckMapDone()
		log.Printf("%+v\n", newReply)
	}
	log.Println("Done waiting for all map ops to be done!")

	var reduceReply = ExampleReply{}
	finish = false
	finishedTask = ""
	log.Println("Request reduce tasks...")
	for reduceReply = RequestReduceTask(finish, finishedTask); reduceReply.TaskNo != -1; reduceReply = RequestReduceTask(finish, finishedTask) {
		// finish = false
		// finishedTask = ""
		taskName := strconv.Itoa(reduceReply.TaskNo)
		ret := GetAllFiles(reply.MapDirectory, reply.TotalNumMapTasks, taskName)
		if len(ret) < reply.TotalNumMapTasks {
			log.Fatalln("Did not retrieve all Map files...")
		}
		fmt.Println(ret)
		ReductionStep(ret, reducef, reply.ReduceDirectory, "mr-out-"+taskName)
		finish = true
		finishedTask = taskName
		log.Println("Finished with " + finishedTask)
	}
}

// ReductionStep
func ReductionStep(ret []string, reducef func(string, []string) string, reduceDir string, oname string) {
	intermediate := []KeyValue{}

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
		sort.Sort(ByKey(intermediate))
		ofile, err := os.Create(reduceDir + "/" + oname)
		if err != nil {
			log.Fatalln(err)
		}

		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
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

			// this is the correct format for each line of Reduce output.
			_, err = fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
			if err != nil {
				log.Fatalln(err)
			}
			i = j
		}

		ofile.Close()
	}
}

// GetAllFiles: gets all files for a particular reduction bucket based on last number in filename
func GetAllFiles(root string, numMapTasks int, reduceNo string) []string {
	var files []string
	for i := 0; i < numMapTasks; i++ {
		filename := root + "/mr-out-" + strconv.Itoa(i) + "-" + reduceNo
		files = append(files, filename)
	}
	fmt.Println(files)
	return files
}

// RequestReduceTask
func RequestReduceTask(finish bool, finishedTaskName string) ExampleReply {
	args := ExampleArgs{}
	args.Pid = os.Getpid()
	args.ReduceFinish = finish
	args.TaskName = finishedTaskName

	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)
	log.Printf("Reduce Task assigned: %d\n", reply.TaskNo)
	// log.Printf("%+v\n", reply)

	return reply
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallCheckMapDone() MapStatusReply {
	args := ExampleArgs{}
	reply := MapStatusReply{}
	call("Master.CheckMapDone", &args, &reply)
	return reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(finish bool, finishedTaskName string) ExampleReply {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.Pid = os.Getpid()

	args.Finish = finish
	args.TaskName = finishedTaskName
	// log.Printf("Hi I am %d\n", args.Pid)
	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.RequestTask", &args, &reply)
	log.Printf("Task assigned: %s\n", reply.Task)
	return reply
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
