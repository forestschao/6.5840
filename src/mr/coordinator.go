package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Completed
)

type Task struct {
	id     int
	file   string
	status TaskStatus
}

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapTasks []Task
	reduceTasks []Task

  remainMapTask    int
  remainReduceTask int

	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
  c.mu.Lock()
  defer c.mu.Unlock()

	return c.remainMapTask == 0 && c.remainReduceTask == 0
}

// Caller should hold the mutex.
func (c *Coordinator) scheduleMapTask(taskReply *TaskReply) error {
	for id, task := range c.mapTasks {
		fmt.Printf("Check task: %v\n", task)
		if task.status == Idle {
      taskReply.TaskType = Map
			taskReply.TaskId = task.id
			taskReply.Files = append(taskReply.Files, task.file)
			taskReply.ReduceNum = c.nReduce

			c.mapTasks[id].status = InProgress
			break
		}
	}
  return nil
}

func (c *Coordinator) getIntermediateFiles(reduceId int) []string {
  files := make([]string, 0)
  for i := 0; i < len(c.files); i++ {
    file := fmt.Sprintf("mr-%d-%d", i, reduceId)
    files = append(files, file)
  }
  return files
}

// Caller should hold the mutex.
func (c *Coordinator) scheduleReduceTask(taskReply *TaskReply) error {
	for id, task := range c.reduceTasks {
		fmt.Printf("Check task: %v\n", task)
		if task.status == Idle {
      taskReply.TaskType = Reduce
			taskReply.TaskId = task.id
			taskReply.Files = c.getIntermediateFiles(id)

			c.reduceTasks[id].status = InProgress
			break
		}
	}
  return nil
}

// Caller should hold the mutex.
func (c *Coordinator) scheduleExit(taskReply *TaskReply) error {
  taskReply.TaskType = Exit
  return nil
}

func (c *Coordinator) Schedule(taskArgs *TaskArgs, taskReply *TaskReply) error {
	fmt.Printf("Start to scedule\n")
	c.mu.Lock()
	defer c.mu.Unlock()

  if c.remainMapTask > 0 {
    c.scheduleMapTask(taskReply)
  } else if c.remainReduceTask > 0 {
    c.scheduleReduceTask(taskReply)
  } else {
    c.scheduleExit(taskReply)
  }

	return nil
}

func (c *Coordinator) Notify(notificationArgs *NotificationArgs, notificationReply *NotificationReply) error {
	fmt.Printf("Start to update status\n")
	c.mu.Lock()
	defer c.mu.Unlock()

  switch notificationArgs.TaskType {
  case Map:
    if c.mapTasks[notificationArgs.TaskId].status != Completed {
      c.mapTasks[notificationArgs.TaskId].status= Completed
      c.remainMapTask--
    }
  case Reduce:
    if c.reduceTasks[notificationArgs.TaskId].status != Completed {
      c.reduceTasks[notificationArgs.TaskId].status= Completed
      c.remainReduceTask--
    }
  }

	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
  c.remainMapTask = len(files)
  c.remainReduceTask = nReduce

	for id, file := range files {
		fmt.Printf("Add %d, file: %v\n", id, file)
		task := Task{}
		task.id = id
		task.file = file
		task.status = Idle
		c.mapTasks = append(c.mapTasks, task)
	}
	fmt.Printf("mapTasks: %v\n", c.mapTasks)

  for i := 0; i < nReduce; i++ {
    reduceTask := Task{}
    reduceTask.id = i
    reduceTask.status = Idle

    c.reduceTasks = append(c.reduceTasks, reduceTask)
  }
	c.server()
	return &c
}
