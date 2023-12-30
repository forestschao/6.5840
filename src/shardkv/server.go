package shardkv


import "6.5840/labgob"
import "6.5840/labrpc"
import "6.5840/raft"
import "6.5840/shardctrler"
import "bytes"
import "compress/gzip"
import "fmt"
import "io/ioutil"
import "sync"
import "time"

const DebugMode = false

func PrintDebug(format string, a ...interface{}) {
  PrintDebugInternal("\033[0m", format, a...)
}

func PrintDebugRed(format string, a ...interface{}) {
  PrintDebugInternal("\033[31m", format, a...)
}

func PrintDebugGreen(format string, a ...interface{}) {
  PrintDebugInternal("\033[32m", format, a...)
}

func PrintDebugYellow(format string, a ...interface{}) {
  PrintDebugInternal("\033[33m", format, a...)
}

func PrintDebugInternal(color string,format string, a ...interface{}) {
	if !DebugMode {
		return
	}
  reset := "\033[0m"
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("%s%s %s%s\n",
    color, curr, fmt.Sprintf(format, a...), reset)
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

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
  data     map[string]string           // Key -> Value
  history  map[int64]int64             // Clerk Id -> Latest CmdId
  handlers map[int]*chan raft.ApplyMsg // Log index -> handler

  persister *raft.Persister

  sm       *shardctrler.Clerk
  config   shardctrler.Config
}

func (kv *ShardKV) receiveMsg() {
  for msg := range kv.applyCh {
    if msg.SnapshotValid {
      kv.ingestSnap(msg.Snapshot)
    } else if msg.CommandValid {
      kv.executeCmd(msg)
    }
  }
}

func (kv *ShardKV) ingestSnap(snapshot []byte) {
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

  // Clear handlers
  for index, handler := range kv.handlers {
    close(*handler)
    delete(kv.handlers, index)
  }
}

func (kv *ShardKV) executeCmd(msg raft.ApplyMsg) {
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

func (kv *ShardKV) needSnapshot() bool {
  return kv.maxraftstate > 0 &&
    kv.persister.RaftStateSize() > kv.maxraftstate - 100
}

func (kv *ShardKV) sendSnapshot(index int) {
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

func (kv *ShardKV) processOp(op Op) {
  // TODO: Probably we don't need to check it.
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
func (kv *ShardKV) processPutAppend(args *PutAppendArgs) {
  if args.Op == "Put" {
    kv.data[args.Key] = args.Value
  } else if args.Op == "Append" {
    oldValue, _ := kv.data[args.Key]
    kv.data[args.Key] = oldValue + args.Value
  }
}

// Caller mustn't hold the mutex.
func (kv *ShardKV) isCommitted(clerkId int64, cmdId int64) bool {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  prevCmdId, _ := kv.history[clerkId]
  return prevCmdId >= cmdId
}

func (kv *ShardKV) setHandler(index int, handler *chan raft.ApplyMsg) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  oldHandler, exists := kv.handlers[index]
  if exists {
    close(*oldHandler)
  }

  kv.handlers[index] = handler
}

func (kv *ShardKV) deleteHandler(index int) {
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

func (kv *ShardKV) commitOp(op *Op) Err {
  if kv.isCommitted(op.ClerkId, op.CmdId) {
    return OK
  }

  index, term, isLeader := kv.rf.Start(*op)
  if !isLeader {
    return ErrWrongLeader
  }
  PrintDebug("commit op: type: %v, clerkId: %v, cmdId: %v",
    op.Type, op.ClerkId, op.CmdId)

  return kv.waitForRaft(index, term)
}

func (kv *ShardKV) waitForRaft(index int, term int) Err {
  wait := make(chan raft.ApplyMsg)

  kv.setHandler(index, &wait)
  defer kv.deleteHandler(index)

  PrintDebugYellow(
    "%v: wait for raft index: %v, term: %v", kv.me, index, term)

  select {
  case <-time.After(opTimeout * time.Millisecond):
    return ErrTimeout
  case <-wait:
  }

  currTerm, isLeader := kv.rf.GetState()
  if !isLeader || currTerm != term {
    PrintDebugYellow("Lose leader.")
    return ErrLoseLeader
  }

  PrintDebugYellow(
    "%v: receive raft index: %v, term: %v", kv.me, index, term)
  return OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  PrintDebugGreen("%v: %s %v: {%v}", kv.me, "Get", args.CmdId, args.Key)

  op := Op {
    From:    kv.me,
    Type:    OpGet,
    ClerkId: args.ClerkId,
    CmdId:   args.CmdId,
    GetArg:  *args,
  }
  reply.Err = kv.commitOp(&op)

  if reply.Err != OK {
    return
  }

  kv.getValue(args.Key, reply)
}

// Caller should not hold the mutex.
func (kv *ShardKV) getValue(key string, reply *GetReply) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  value, exists := kv.data[key]
  reply.Value = value
  if !exists {
    reply.Err = ErrNoKey
  } else {
    reply.Err = OK
  }
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
  PrintDebugGreen("%v: %s %v: %v -> {%v}",
    kv.me, args.Op, args.CmdId, args.Key, args.Value)

  op := Op {
    From:         kv.me,
    Type:         OpPutAppend,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    PutAppendArg: *args,
  }

  reply.Err = kv.commitOp(&op)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(
  servers []*labrpc.ClientEnd,
  me int,
  persister *raft.Persister,
  maxraftstate int,
  gid int,
  ctrlers []*labrpc.ClientEnd,
  make_end func(string) *labrpc.ClientEnd,
) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)

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

  kv.sm = shardctrler.MakeClerk(ctrlers)

  go kv.receiveMsg()

	return kv
}
