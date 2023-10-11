package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"

import "os"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the coordinator.
	//CallExample()
	for {

		task := TaskInfo{}
		call("Coordinator.Requesttask", &task, &task)

		switch task.TaskType {
		case "map":
			fmt.Printf("get a map task :%d\n", task.ID)
			list := mapf(task.Filename, task.Content)
			var mr = MapResult{
				Inner: list,
			}
			call("Coordinator.Reqortmapresult", &mr, &mr)

		case "reduce":
			fmt.Printf("get a reduce task :%d\n", task.ID)
			filename := fmt.Sprintf("mr-out-%d", task.ID)
			openFile, e := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 777)
			if e != nil {
				fmt.Println("open file error")
				continue
			}
			var result ReduceResult
			for index, key := range task.Keys {
				re := reducef(key, task.Values[index])
				openFile.WriteString(fmt.Sprintf("%s %s\n", key, re))
				result.Key = append(result.Key, key)
				result.Result = append(result.Result, re)
			}
			call("Coordinator.Reportreduce", &result, &result)
		case "none":
			fmt.Print("none task from coordinator\n")
			break
		default:
			fmt.Print("can't receive task !\n")
		}
	}

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
