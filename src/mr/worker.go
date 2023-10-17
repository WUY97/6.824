package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		ok := call("Coordinator.AskTask", &args, &reply)
		if !ok {
			log.Printf("Failed to get a task from the coordinator.")
			time.Sleep(1 * time.Second) // avoid aggressive retries
			continue
		}

		if reply.Task == nil {
			// log.Printf("No tasks available at the moment, let's wait a bit.")
			time.Sleep(1 * time.Second)
			continue
		}

		var status TaskStatus
		switch reply.Task.Type {
		case MapTask:
			// Read input file
			content, err := os.ReadFile(reply.Task.File)
			if err != nil {
				log.Printf("Failed to read file %s", reply.Task.File)
				status = Failed
				break
			}

			// Call map function
			kvs := mapf(reply.Task.File, string(content))

			// Partition k-v pairs into NReduce files
			intermediateFiles := make([][]KeyValue, reply.NReduce)
			for _, kv := range kvs {
				partition := ihash(kv.Key) % reply.NReduce
				intermediateFiles[partition] = append(intermediateFiles[partition], kv)
			}

			for i, kvs := range intermediateFiles {
				filename := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
				file, err := os.Create(filename)
				if err != nil {
					log.Printf("Failed to create file %s", filename)
					status = Failed
					break
				}

				enc := json.NewEncoder(file)
				for _, kv := range kvs {
					err := enc.Encode(&kv)
					if err != nil {
						log.Printf("Failed to encode k-v pair %v", kv)
						status = Failed
						break
					}
				}

				file.Close()
				if status == Failed {
					break
				}
			}

			if status != Failed {
				status = Completed
			}
		case ReduceTask:
			// Read intermediate files
			intermediateKVs := []KeyValue{}
			for i := 0; i < len(reply.Files); i++ {
				filename := fmt.Sprintf("mr-%d-%d", i, reply.Task.Partition)
				file, err := os.Open(filename)
				if err != nil {
					log.Printf("Failed to open file %s", filename)
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						if err != io.EOF {
							log.Printf("Error decoding JSON from file %s: %v", filename, err)
						}
						break
					}
					intermediateKVs = append(intermediateKVs, kv)
				}
				file.Close()
			}

			// Sort k-v pairs by key
			sort.Sort(ByKey(intermediateKVs))

			// Call reducef for each unique key
			oname := fmt.Sprintf("mr-out-%d", reply.Task.Partition)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Printf("Failed to create file %s", oname)
				status = Failed
			} else {
				i := 0
				for i < len(intermediateKVs) {
					j := i + 1
					for j < len(intermediateKVs) && intermediateKVs[j].Key == intermediateKVs[i].Key {
						j++
					}
					values := []string{}
					for k := i; k < j; k++ {
						values = append(values, intermediateKVs[k].Value)
					}
					output := reducef(intermediateKVs[i].Key, values)
					_, err := fmt.Fprintf(ofile, "%s %s\n", intermediateKVs[i].Key, output)
					if err != nil {
						log.Printf("Failed to encode k-v pair %v", err)
						break
					}
					i = j
				}
				ofile.Close()
				if status != Failed {
					status = Completed
				}
			}
		default:
			log.Printf("Unknown task type: %s", reply.Task.Type)
			continue
		}

		reportArgs := ReportTaskArgs{
			TaskID:   reply.TaskID,
			TaskType: reply.Task.Type,
			Status:   status,
		}

		reportReply := ReportTaskReply{}

		ok = call("Coordinator.ReportTask", &reportArgs, &reportReply)
		if ok && reportReply.Success {
			if reply.Task.Type == ReduceTask {
				for i := 0; i < len(reply.Files); i++ {
					filename := fmt.Sprintf("mr-%d-%d", i, reply.Task.Partition)
					err := os.Remove(filename)
					if err != nil {
						log.Printf("Failed to delete file %s", filename)
					}
				}
			}
		} else {
			log.Printf("Failed to report task %d as completed. Error: %s", reply.TaskID, reportReply.Err)
			time.Sleep(1 * time.Second)
			continue
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
