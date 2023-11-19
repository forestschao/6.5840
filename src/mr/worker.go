package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "sort"
import "os"
import "io/ioutil"
import "encoding/json"
import "time"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
  for {
    fmt.Println("Start to ask for task.")
    taskArgs := TaskArgs{}
    taskReply := TaskReply{}

    ok := call("Coordinator.Schedule", &taskArgs, &taskReply)
    if !ok {
      return
    }

		switch taskReply.TaskType {
		case Map:
			runMap(taskReply, mapf)
		case Reduce:
			runReduce(taskReply, reducef)
		case Exit:
			return
		}

    time.Sleep(time.Second)
  }
}

func runMap(task TaskReply, mapf func(string, string) []KeyValue) {
  fmt.Printf("Get Task: %v\n", task)
  // Map values
	intermediates := make([][]KeyValue, task.ReduceNum)
	for _, filename := range(task.Files) {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))

    for _, kv := range kva { 
      reduceId := ihash(kv.Key) % task.ReduceNum
      intermediates[reduceId] = append(intermediates[reduceId], kv)
    }
	}

  // Save intermediate files
  for i, intermediate := range intermediates {
    fmt.Println("Start to write file")

    tempFile, err := ioutil.TempFile("", "test-")
    if err != nil {
      fmt.Println("Error creating temporary file:", err)
      return
    }

    encoder := json.NewEncoder(tempFile)
    for _, kv := range intermediate {
      err := encoder.Encode(kv)
      if err != nil {
        fmt.Println("Error: ", err)
        return
      }
    }

    outputName := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
    if err := os.Rename(tempFile.Name(), outputName); err != nil {
      fmt.Println("Error renaming temporary file:", err)
      return
    }
  }

  // Notifies coordinator
  notify(task)
}

func writeReduceResult(taskId int, kva []KeyValue, reducef func(string, []string) string) {
  fmt.Println("Start to save file")

  tempFile, err := ioutil.TempFile("", "test-")
  if err != nil {
    fmt.Println("Error creating temporary file:", err)
    return
  }

  //
  // call Reduce on each distinct key in intermediate[],
  // and print the result to mr-out-0.
  //
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
    fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

    i = j
  }

  outputName := fmt.Sprintf("mr-out-%d", taskId)
  if err := os.Rename(tempFile.Name(), outputName); err != nil {
    fmt.Println("Error renaming temporary file:", err)
    return
  }
}

func runReduce(task TaskReply, reducef func(string, []string) string) {
  fmt.Printf("Get Reduce Task: %v\n", task)
  // Read intermediate files
	kva := []KeyValue{}
	for _, filename := range(task.Files) {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		defer file.Close()

    dec := json.NewDecoder(file)
    for {
      var kv KeyValue
      if err := dec.Decode(&kv); err != nil {
        break
      }
      kva = append(kva, kv)
    }
	}

  sort.Sort(ByKey(kva))

  // Save to output
  writeReduceResult(task.TaskId, kva, reducef)


  // Notifies coordinator
  notify(task)
}

func notify(task TaskReply) {
  notificationArgs := NotificationArgs{}
  notificationArgs.TaskType = task.TaskType 
  notificationArgs.TaskId = task.TaskId

  notificationReply := NotificationReply{}
  ok := call("Coordinator.Notify", &notificationArgs, &notificationReply)
  if !ok {
    return
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
