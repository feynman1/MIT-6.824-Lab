package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
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

	// Your worker implementation here.
	for {
		//1.call and work(map/reduce)
		CallAndWork(mapf, reducef)
		//2.wait for ten seconds
		time.Sleep(10 * time.Second)
		//3.use CallExample check whether master is alive
		args := ExampleArgs{}
		args.X = 99
		reply := ExampleReply{}
		ok := call("Coordinator.Example", &args, &reply)
		if !ok {
			break
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	return
}

func CallAndWork(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	//declare an argument structure
	args := ExampleArgs{}
	//fill in the arguments
	//declare a reply structure
	reply := RpcStructure{}
	//call master asking for work
	b := call("Coordinator.AllocateWork", &args, &reply)
	fmt.Println(reply)
	//work(map/reduce)
	if (!b) || reply.WorkType == WAIT {
		return
	} else if reply.WorkType == MAP {
		file, err := os.Open(reply.MapInputFile)
		if err != nil {
			log.Fatalf("cannot open %v", reply.MapInputFile)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.MapInputFile)
		}
		file.Close()
		intermediate := mapf(reply.MapInputFile, string(content))
		//write the result into file
		//put key-value into intermediate file according to ihash
		files := []*os.File{}
		for i := 0; i < reply.ReduceNum; i++ {
			ofile := "mr-map-" + strconv.Itoa(reply.MapIndex) + "-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.TaskNum)
			create, err := os.Create(ofile)
			if err != nil {
				log.Fatalf("cannot create %v", ofile)
			}
			files = append(files, create)
			reply.MapOutputFile = append(reply.MapOutputFile, ofile)
		}
		for i := 0; i < len(intermediate); i++ {
			key := ihash(intermediate[i].Key) % reply.ReduceNum
			enc := json.NewEncoder(files[key])
			err := enc.Encode(&intermediate[i])
			if err != nil {
				log.Fatalf("cannot write into file%v", key)
			}
		}
		//3.close intermediate files
		for i := 0; i < reply.ReduceNum; i++ {
			files[i].Close()
		}
	} else if reply.WorkType == REDUCE {
		//1.open all reduce files
		rfiles := []*os.File{}
		for i := 0; i < reply.MapNum; i++ {
			open, err := os.Open(reply.ReduceInputFile[i])
			if err != nil {
				log.Fatalf("cannot open %v", reply.MapOutputFile[i])
			}
			rfiles = append(rfiles, open)
		}
		//2.read all files and put the same key together
		kva := []KeyValue{}
		for i := 0; i < reply.MapNum; i++ {
			decoder := json.NewDecoder(rfiles[i])
			for {
				var kv KeyValue
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
		//3.pull kvs into reduce and write into file
		o := "mr-out-" + strconv.Itoa(reply.ReduceIndex) + "-" + strconv.Itoa(reply.TaskNum)
		create, err := os.Create(o)
		if err != nil {
			log.Fatalf("fail to create %v", o)
		}
		reply.ReduceOutputFile = o
		//sort kva
		sort.Sort(ByKey(kva))
		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)
			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(create, "%v %v\n", kva[i].Key, output)
			i = j
		}
		create.Close()
	}
	//call master that my work is done
	args = ExampleArgs{}
	reply.MyWorkIsDone = true
	call("Coordinator.WorkIsDone", &reply, &args)
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//sockname := coordinatorSock()
	//c, err := rpc.DialHTTP("unix", sockname)
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
