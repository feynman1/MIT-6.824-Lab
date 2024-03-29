package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MAP = iota
	REDUCE
	WAIT
)

type RpcStructure struct {
	MapIndex         int
	ReduceIndex      int
	MapInputFile     string
	MapOutputFile    []string
	ReduceInputFile  []string
	ReduceOutputFile string
	MyWorkIsDone     bool
	WorkType         int
	ReduceNum        int
	TaskNum          int
	MapNum           int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
