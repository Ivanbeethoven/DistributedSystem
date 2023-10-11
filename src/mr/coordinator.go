package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	_map           int
	_map_report    int
	_reduce        int
	_reduce_report int
	numberLock     sync.Mutex
	stateLock      sync.Mutex
	state          string
	inner          sync.Map
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Requesttask(args *TaskInfo, reply *TaskInfo) error {
	fmt.Printf("task request ! state:%s\n", c.state)
	c.stateLock.Lock()

	if c.state == "map" {
		c.numberLock.Lock()
		println("numberLock.Lock()")
		reply.ID = c._map
		reply.TaskType = "map"
		c._map++
		if c._map >= len(c.files) {
			c.state = "map_waiting"
		}
		c.numberLock.Unlock()
		c.stateLock.Unlock()
		println("unlock ...")
		filename := c.files[reply.ID]
		reply.Filename = filename
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		buf := make([]byte, 1024)
		for {
			len, _ := file.Read(buf)
			if len == 0 {
				break
			}
			reply.Content += string(buf)
		}
		fmt.Printf("reply:%s\n", reply.TaskType)
		return nil
	} else if c.state == "reduce" {
		c.numberLock.Lock()
		reply.ID = c._reduce
		reply.TaskType = "reduce"
		c._reduce++
		if c._reduce >= c.nReduce {
			c.state = "reduce_wating"
		}
		c.numberLock.Unlock()
		c.stateLock.Unlock()

		c.inner.Range(func(key, value interface{}) bool {
			_key := key.(string)
			_value := value.([]string)
			if ihash(_key)%c.nReduce == reply.ID {
				reply.Keys = append(reply.Keys, _key)
				reply.Values = append(reply.Values, _value)
			}
			return true
		})
	} else {
		c.stateLock.Unlock()
		reply.ID = -1
		reply.TaskType = "none"

	}
	return nil
}

func (c *Coordinator) Reqortmapresult(args *MapResult, reply *MapResult) error {
	c.stateLock.Lock()
	c.numberLock.Lock()
	c._map_report++
	if c._map_report >= len(c.files) {
		c.state = "reduce"
	}
	c.numberLock.Unlock()
	c.stateLock.Unlock()
	var list []string
	for _, kv := range args.Inner {
		_list, ok := c.inner.Load(kv.Key)
		if ok {
			list = _list.([]string)

		} else {
			list = make([]string, 0)
		}
		list = append(list, kv.Value)
		c.inner.Store(kv.Key, list)
	}
	return nil
}

func (c *Coordinator) Reportreduce(args *ReduceResult, reply *ReduceResult) error {
	println("reduce report")
	c.stateLock.Lock()
	c.numberLock.Lock()
	c._reduce_report++
	if c._reduce_report >= c.nReduce {
		c.state = "Done"
	}
	c.numberLock.Unlock()
	c.stateLock.Unlock()

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	println("is Done?")
	c.stateLock.Lock()
	// Your code here.
	if c.state == "Done" {
		c.stateLock.Unlock()
		return true
	} else {
		c.stateLock.Unlock()
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:          files,
		nReduce:        nReduce,
		state:          "map",
		_map:           0,
		_reduce:        0,
		_map_report:    0,
		_reduce_report: 0,
	}

	// Your code here.

	c.server()
	return &c
}
