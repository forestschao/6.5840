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
  OpShards = "Shards"
  OpConfig = "Config"

  ShardReady = "ShardReady"
  ShardHandoff = "ShardHandoff"
  ShardWaiting = "ShardWaiting"
  ShardWrongGroup = "ShardWrongGroup"
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
  ConfigArg    ConfigArgs
  ShardsArg    ShardsArgs
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

  shardState [shardctrler.NShards]string

  persister *raft.Persister

  sm        *shardctrler.Clerk
  config    shardctrler.Config
  prevShard ShardsArgs
  receivers []int  // GIDs of the receiver groups.
}

func (kv *ShardKV) receiveConfig() {
  for {
    newConfig := kv.sm.Query(-1)

    needReconfig := false

    kv.mu.Lock()

    _, isLeader := kv.rf.GetState()
    needReconfig = isLeader && newConfig.Num >= kv.config.Num

    kv.mu.Unlock()

    if needReconfig {
      kv.reconfig(&newConfig)
    }

    time.Sleep(100 * time.Millisecond)
  }
}

func (kv *ShardKV) reconfig(newConfig *shardctrler.Config) {
  PrintDebug("%v Update shards: %v", kv.me, newConfig.Shards)

  // Send config args
  args := ConfigArgs {
    Num:     newConfig.Num,
    Shards:  newConfig.Shards[:],
    Groups:  newConfig.Groups,
  }

  op := Op {
    From:      kv.me,
    Type:      OpConfig,
    ClerkId:   int64(kv.gid),
    CmdId:     int64(args.Num),
    ConfigArg: args,
  }

  kv.commitOp(&op)

  // for _, receiver := range receivers {
  //   if servers, ok := kv.config.Groups[receiver]; ok {
  //     go kv.sendShards(&args, servers)
  //   }
  // }
}

func (kv *ShardKV) sendShards(args *ShardsArgs, servers []string) {
  // try each server for the shard.
  for si := 0; si < len(servers); si++ {
    srv := kv.make_end(servers[si])
    var reply ShardsReply
    ok := srv.Call("ShardKV.ReceiveShards", args, &reply)
    if ok && reply.Err != ErrWrongLeader {
      return
    }
  }
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

// Caller must hold the mutex.
func (kv *ShardKV) processOp(op Op) {
  // TODO: Probably we don't need to check it.
  prevCmdId, _ := kv.history[op.ClerkId]
  if prevCmdId >= op.CmdId {
    return
  }

  kv.history[op.ClerkId] = op.CmdId

  switch op.Type {
  case OpPutAppend:
    kv.processPutAppend(&op.PutAppendArg)
  case OpShards:
    kv.processShards(&op.ShardsArg)
  case OpConfig:
    kv.processConfig(&op.ConfigArg)
  }
}

// Caller must hold the mutex.
func (kv *ShardKV) correctShard(key string) bool {
  shard := key2shard(key)
  gid := kv.config.Shards[shard]
  if gid != kv.gid {
    PrintDebugRed("Wrong group: expect: %v, get: %v", kv.gid, gid)
  }
  return gid == kv.gid
}

func (kv *ShardKV) shardReady(key string) bool {
  shard := key2shard(key)
  if kv.shardState[shard] != ShardReady {
    PrintDebugRed("Shard %v is not ready: %v", shard, kv.shardState[shard])
  }
  return true
  // return kv.shardState[shard] == ShardReady
}

// Caller must hold the mutex.
func (kv *ShardKV) processPutAppend(args *PutAppendArgs) {
  if !kv.correctShard(args.Key) || !kv.shardReady(args.Key) {
    return
  }

  if args.Op == "Put" {
    kv.data[args.Key] = args.Value
  } else if args.Op == "Append" {
    oldValue, _ := kv.data[args.Key]
    kv.data[args.Key] = oldValue + args.Value
  }
}

// Caller must hold the mutex.
func (kv *ShardKV) processConfig(args *ConfigArgs) {
  PrintDebug("processConfig: shard: %v", args.Shards)

  kv.saveShards(args)
  kv.updateShardState(args.Shards[:])
  kv.setShardsReceivers(args.Shards[:])

  // Update config.
  kv.config.Num = args.Num

  for shard, gid := range args.Shards {
    kv.config.Shards[shard] = gid
  }

  kv.config.Groups = make(map[int][]string)
  for gid, servers := range args.Groups {
    kv.config.Groups[gid] = servers
  }
}

func (kv *ShardKV) saveShards(newConfig *ConfigArgs) {
  // Copy all the data of the original shards.
  data := make(map[string]string)
  for key, value := range kv.data {
    if kv.correctShard(key) {
      data[key] = value
    }
  }

  kv.prevShard = ShardsArgs {
    ClerkId: int64(kv.gid),
    CmdId:   int64(newConfig.Num),
    Num:     newConfig.Num,
    Data:    data,
  }
}

func (kv *ShardKV) updateShardState(newShards []int) {
  PrintDebug("updateShardState: %v", newShards)
  shards := kv.config.Shards[:]
  for i, newGid := range newShards {
    oldGid := shards[i]

    if oldGid == 0 || newGid == 0 || oldGid == newGid {
      kv.shardState[i] = ShardReady
    } else if oldGid == kv.gid && newGid != kv.gid {
      kv.shardState[i] = ShardHandoff
    } else if oldGid != kv.gid && newGid == kv.gid {
      kv.shardState[i] = ShardWaiting
    }
  }
}

func (kv *ShardKV) setShardsReceivers(shards []int) {
  receivers := []int{}
  for i, newGid := range shards {
    if kv.config.Shards[i] == kv.gid && newGid != kv.gid {
      receivers = append(receivers, newGid)
    }
  }

  kv.receivers = receivers
}

// Caller must hold the mutex.
func (kv *ShardKV) processShards(args *ShardsArgs) {
  if args.Num < kv.config.Num {
    return
  }

  for key, value := range args.Data {
    if kv.correctShard(key) {
      kv.data[key] = value
    }
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
    PrintDebug(
      "%v: op (clerkId %v, cmdId %v) is committed",
      kv.me, op.ClerkId, op.CmdId)
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

  kv.mu.Lock()
  defer kv.mu.Unlock()

  // if !kv.correctShard(key) {
  //   return ErrWrongGroup
  // }

  // if !kv.shardReady(key) {
  //   return ErrShardNotReady
  // }

  PrintDebugYellow(
    "%v: receive raft index: %v, term: %v", kv.me, index, term)
  return OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  PrintDebugGreen(
    "%v: %s %v: {%v} (shard: %v)",
    kv.me, "Get", args.CmdId, args.Key, key2shard(args.Key))

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
  PrintDebugGreen("%v: %s %v: %v (shard: %v) -> {%v}",
    kv.me, args.Op, args.CmdId, args.Key, key2shard(args.Key), args.Value)

  op := Op {
    From:         kv.me,
    Type:         OpPutAppend,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    PutAppendArg: *args,
  }

  reply.Err = kv.commitOp(&op)
}

func (kv *ShardKV) ReceiveShards(args *ShardsArgs, reply *ShardsReply) {
  PrintDebugGreen("%v: receive shards. config num: %v", kv.me, args.Num)

  op := Op {
    From:         kv.me,
    Type:         OpShards,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    ShardsArg:    *args,
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

  for i := 0; i < len(kv.shardState); i++ {
    kv.shardState[i] = ShardWrongGroup
  }

  go kv.receiveMsg()
  go kv.receiveConfig()

	return kv
}
