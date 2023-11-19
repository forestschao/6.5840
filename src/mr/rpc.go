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

type TaskType int

const (
  Map TaskType = iota
  Reduce
  Wait
  Exit
)

// Add your RPC definitions here.
type TaskArgs struct {
  // Intentionally blank
}

type TaskReply struct {
  TaskId    int
  TaskType  TaskType
  Files     []string
  ReduceNum int
}

type NotificationArgs struct {
  TaskType  TaskType
  TaskId    int
}

type NotificationReply struct {
  // Intentionally blank
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
