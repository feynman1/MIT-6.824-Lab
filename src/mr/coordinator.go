package mr

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

const (
	PROCESSING = iota
	FREE
	DONE
	TIMEOUT
)

type Task struct {
	WorkType   int
	WorkStatus int
	WorkIndex  int
	TaskNum    int
}
type Coordinator struct {
	// Your definitions here.
	Sign           bool
	MapInputFiles  []string
	MapOutputFiles map[int][]string
	ReduceDone     []int
	MapDone        []int
	AllMapDone     bool
	ReduceNum      int
	TaskNum        int
	AllTask        map[string]*Task
}

var (
	lock sync.Mutex
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false
	// Your code here.
	lock.Lock()
	ret = c.Sign
	lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	lock.Lock()
	c := Coordinator{
		Sign:           false,
		ReduceNum:      nReduce,
		MapDone:        make([]int, len(files)),
		ReduceDone:     make([]int, nReduce),
		MapInputFiles:  files,
		MapOutputFiles: make(map[int][]string, nReduce),
		AllMapDone:     false,
		TaskNum:        0,
		AllTask:        make(map[string]*Task),
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceDone[i] = FREE
	}
	for i := 0; i < len(files); i++ {
		c.MapDone[i] = FREE
	}
	lock.Unlock()
	// Your code here.
	fmt.Println("master start")
	fmt.Println(files)
	c.server()
	return &c
}

//rpc call:allocate work to a worker
func (c *Coordinator) AllocateWork(args *ExampleArgs, reply *RpcStructure) error {
	lock.Lock()
	defer lock.Unlock()
	//if all work is done,quit
	if c.Sign {
		reply.WorkType = WAIT
		return nil
	}
	//allocate map work
	flag := len(c.MapDone)
	for index, b := range c.MapDone {
		if b == FREE {
			flag = index
			break
		}
	}
	if flag != len(c.MapDone) {
		c.TaskNum++
		c.MapDone[flag] = PROCESSING
		a := Task{WorkIndex: flag, WorkStatus: PROCESSING, WorkType: MAP, TaskNum: c.TaskNum}
		c.AllTask[strconv.Itoa(c.TaskNum)] = &a
		reply.TaskNum = c.TaskNum
		reply.WorkType = MAP
		reply.MapIndex = flag
		reply.ReduceNum = c.ReduceNum
		reply.MapInputFile = c.MapInputFiles[flag]
		reply.MyWorkIsDone = false
		reply.MapNum = len(c.MapInputFiles)
		go c.WaitAndSee(c.TaskNum)
		return nil
	}
	//allocate reduce work if all map works were done
	if c.AllMapDone {
		flag2 := c.ReduceNum
		for index, b := range c.ReduceDone {
			if b == FREE {
				flag2 = index
				break
			}
		}
		if flag2 != c.ReduceNum {
			c.TaskNum++
			c.ReduceDone[flag2] = PROCESSING
			a := Task{WorkIndex: flag2, WorkStatus: PROCESSING, WorkType: REDUCE, TaskNum: c.TaskNum}
			c.AllTask[strconv.Itoa(c.TaskNum)] = &a
			reply.TaskNum = c.TaskNum
			reply.WorkType = REDUCE
			reply.ReduceNum = c.ReduceNum
			reply.ReduceIndex = flag2
			reply.ReduceInputFile = c.MapOutputFiles[flag2]
			reply.MyWorkIsDone = false
			reply.MapNum = len(c.MapInputFiles)
			go c.WaitAndSee(c.TaskNum)
			return nil
		}
	}
	reply.WorkType = WAIT
	return nil
}

//rpc call:worker tells master that his worker is done
func (c *Coordinator) WorkIsDone(args *RpcStructure, reply *ExampleReply) error {
	lock.Lock()
	defer lock.Unlock()
	a := c.AllTask[strconv.Itoa(args.TaskNum)]
	if a.WorkStatus == TIMEOUT {
		delete(c.AllTask, strconv.Itoa(args.TaskNum))
		return nil
	} else {
		if a.WorkType == MAP {
			c.MapDone[a.WorkIndex] = DONE
			a.WorkStatus = DONE
			for i := 0; i < len(args.MapOutputFile); i++ {
				newPath := "mr-" + strconv.Itoa(args.MapIndex) + "-" + strconv.Itoa(i)
				os.Rename(args.MapOutputFile[i], newPath)
				c.MapOutputFiles[i] = append(c.MapOutputFiles[i], newPath)
			}
			flag := false
			for _, b := range c.MapDone {
				if b != DONE {
					flag = true
					break
				}
			}
			if !flag {
				c.AllMapDone = true
			}
		}
		if a.WorkType == REDUCE {
			c.ReduceDone[a.WorkIndex] = DONE
			a.WorkStatus = DONE
			newPath := "mr-out-" + strconv.Itoa(args.ReduceIndex)
			os.Rename(args.ReduceOutputFile, newPath)
			flag := false
			for _, b := range c.ReduceDone {
				if b != DONE {
					flag = true
					break
				}
			}
			if !flag {
				c.Sign = true
			}
		}
	}
	return nil
}

//if worker doesn't finish his work in ten seconds,reallocate the work
func (c *Coordinator) WaitAndSee(taskNUM int) {
	time.Sleep(10 * time.Second)
	lock.Lock()
	defer lock.Unlock()
	a := c.AllTask[strconv.Itoa(taskNUM)]
	if a.WorkType == MAP {
		if c.MapDone[a.WorkIndex] == PROCESSING {
			c.MapDone[a.WorkIndex] = FREE
			a.WorkStatus = TIMEOUT
		} else {
			delete(c.AllTask, strconv.Itoa(a.TaskNum))
		}
	} else {
		if c.ReduceDone[a.WorkIndex] == PROCESSING {
			c.ReduceDone[a.WorkIndex] = FREE
			a.WorkStatus = TIMEOUT
		} else {
			delete(c.AllTask, strconv.Itoa(a.TaskNum))
		}
	}
	return
}
