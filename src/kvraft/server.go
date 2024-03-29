package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
  "bytes"
  "compress/gzip"
  "fmt"
  "io/ioutil"
  // "log"
	"sync"
	"sync/atomic"
  "time"
)

const DebugMode = false

func PrintDebug(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("%s %s\n", curr, fmt.Sprintf(format, a...))
}

func PrintDebugGreen(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("\033[32m%s %s\033[0m\n", curr, fmt.Sprintf(format, a...))
}

func PrintDebugYellow(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("\033[33m%s %s\033[0m\n", curr, fmt.Sprintf(format, a...))
}

const (
  OpGet = "Get"
  OpPutAppend = "PutAppend"
)

const opTimeout = 100 // Milliseconds

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
  From         int
  Type         string
  ClerkId      int64
  CmdId        int64
  GetArg       GetArgs
  PutAppendArg PutAppendArgs
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
  data     map[string]string
  history  map[int64]int64
  handlers map[int]*chan raft.ApplyMsg

  persister *raft.Persister
}

func (kv *KVServer) updateState() {
  for msg := range kv.applyCh {
    if msg.SnapshotValid {
      kv.ingestSnap(msg.Snapshot)
    } else if msg.CommandValid {
      kv.executeCmd(msg)
    }
  }
}

func (kv *KVServer) ingestSnap(snapshot []byte) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // Decompress
  compressedBuffer := bytes.NewReader(snapshot)
  gzipReader, err := gzip.NewReader(compressedBuffer)
  if err != nil {
    PrintDebug("Error creating gzip rader: %s", err)
    return
  }
  defer gzipReader.Close()

  decompressedData, err := ioutil.ReadAll(gzipReader)
  if err != nil {
    PrintDebug("Error reading decompressed data: %s", err)
    return
  }

  // Decode
  r := bytes.NewBuffer(decompressedData)
  d := labgob.NewDecoder(r)

  if d.Decode(&kv.data) != nil ||
    d.Decode(&kv.history) != nil {
    PrintDebug("Snapshot decode error")
    return
  }

  for index, handler := range kv.handlers {
    close(*handler)
    delete(kv.handlers, index)
  }
}

func (kv *KVServer) executeCmd(msg raft.ApplyMsg) {
  op, ok := msg.Command.(Op)
  if !ok {
    PrintDebug("Cannot instantiate the command")
    return
  }

  PrintDebugGreen("%v: Receive     %v from: %v, index: %v",
    kv.me, op.CmdId, op.From, msg.CommandIndex)

  kv.mu.Lock()
  defer kv.mu.Unlock()

  kv.processOp(op)

  if kv.needSnapshot() {
    kv.sendSnapshot(msg.CommandIndex)
  }

  handler, exists := kv.handlers[msg.CommandIndex]

  if exists {
    *handler <- msg
  }
  PrintDebugGreen("%v: UpdateState %v Done", kv.me, op.CmdId)
}

func (kv *KVServer) needSnapshot() bool {
  return kv.maxraftstate > 0 && 
    kv.persister.RaftStateSize() > kv.maxraftstate - 100
}

func (kv *KVServer) sendSnapshot(index int) {
  // Encode
  w := new(bytes.Buffer)
  e := labgob.NewEncoder(w)
  e.Encode(kv.data)
  e.Encode(kv.history)

  // Compress
  var compressedData bytes.Buffer
  gz := gzip.NewWriter(&compressedData)
  _, err := gz.Write(w.Bytes())
  if err != nil {
    PrintDebug("Error compressing: %s", err)
    return
  }
  gz.Close()

  kv.rf.Snapshot(index, compressedData.Bytes())
}

func (kv *KVServer) processOp(op Op) {
  prevCmdId, _ := kv.history[op.ClerkId]
  if prevCmdId >= op.CmdId {
    return
  }

  kv.history[op.ClerkId] = op.CmdId

  if op.Type == OpPutAppend {
    kv.processPutAppend(&op.PutAppendArg)
  }
}

// Caller must hold the mutex.
func (kv *KVServer) processPutAppend(args *PutAppendArgs) {
  if args.Op == "Put" {
    kv.data[args.Key] = args.Value
  } else if args.Op == "Append" {
    oldValue, _ := kv.data[args.Key]
    kv.data[args.Key] = oldValue + args.Value
  }
}

// Caller mustn't hold the mutex.
func (kv *KVServer) isCommitted(clerkId int64, cmdId int64) bool {
  kv.mu.Lock()
  defer kv.mu.Unlock()


  prevCmdId, _ := kv.history[clerkId]
  return prevCmdId >= cmdId
}

func (kv *KVServer) setHandler(index int, handler *chan raft.ApplyMsg) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  oldHandler, exists := kv.handlers[index]
  if exists {
    close(*oldHandler)
  }

  kv.handlers[index] = handler
}

func (kv *KVServer) deleteHandler(index int) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  PrintDebugYellow(
    "%v: start to delete handler: %v", kv.me, index)
  oldHandler, exists := kv.handlers[index]
  if exists {
    close(*oldHandler)
  }

  delete(kv.handlers, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  reply.Err = OK
  if !kv.isCommitted(args.ClerkId, args.CmdId) {
    op := Op {
      From:    kv.me,
      Type:    OpGet,
      ClerkId: args.ClerkId,
      CmdId:   args.CmdId,
      GetArg:  *args,
    }
    index, _, isLeader := kv.rf.Start(op)
    if !isLeader {
      reply.Err = ErrWrongLeader
      return 
    }

    reply.Err = kv.waitForRaft(index, op.CmdId)
  }

  if reply.Err != OK {
    return
  }

  PrintDebugGreen("%v: %s %v: {%v}", kv.me, "Get", args.CmdId, args.Key)

  kv.mu.Lock()
  defer kv.mu.Unlock()

  value, exists := kv.data[args.Key]
  reply.Value = value
  if !exists {
    reply.Err = ErrNoKey
  } else {
    reply.Err = OK
  }
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
  if kv.isCommitted(args.ClerkId, args.CmdId) {
    reply.Err = OK
    return
  }

  op := Op {
    From:         kv.me,
    Type:         OpPutAppend,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    PutAppendArg: *args,
  }

  index, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    reply.Err = ErrWrongLeader
    return 
  }

  PrintDebugGreen("%v: %s %v: %v -> {%v}",
    kv.me, args.Op, args.CmdId, args.Key, args.Value)

  reply.Err = kv.waitForRaft(index, op.CmdId)
}

func (kv *KVServer) waitForRaft(index int, cmdId int64) Err {
  wait := make(chan raft.ApplyMsg)

  kv.setHandler(index, &wait)
  defer kv.deleteHandler(index)

  PrintDebugYellow("%v: wait for raft %v, index: %v", kv.me, cmdId, index)

  var resultMsg raft.ApplyMsg

  select {
  case <-time.After(opTimeout * time.Millisecond):
    return ErrTimeout
  case resultMsg = <-wait:
  }

  resultOp, ok := resultMsg.Command.(Op)
  if !ok {
    return ErrParse
  }

  if resultOp.CmdId != cmdId {
    return ErrLoseLeader
  } else {
    return OK
  }
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(
  servers []*labrpc.ClientEnd,
  me int,
  persister *raft.Persister,
  maxraftstate int,
) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
  kv.data = make(map[string]string)
  kv.history = make(map[int64]int64)
  kv.handlers = make(map[int]*chan raft.ApplyMsg)

  kv.persister = persister
  snapshot := persister.ReadSnapshot()
  if snapshot != nil && len(snapshot) > 0 {
    kv.ingestSnap(snapshot)
  }

  go kv.updateState()

	return kv
}
