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

const DebugMode = true

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
  OpReceiveShards = "ReceiveShards"
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

type Reply struct {
  GetReply       GetReply
  PutAppendReply PutAppendReply
  ConfigReply    ConfigReply
  ShardsReply    ShardsReply
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
  handlers map[int]*chan Reply // Log index -> handler

  shardState [shardctrler.NShards]string

  persister *raft.Persister

  sm         *shardctrler.Clerk
  config     shardctrler.Config
  prevConfig shardctrler.Config
  prevShard  ShardsArgs
  receivers  map[int]int  // Shard Id -> Receiver GID
}

func (kv *ShardKV) receiveConfig() {
  for {
    newConfig := kv.sm.Query(-1)

    kv.mu.Lock()
    needReconfig := newConfig.Num > kv.config.Num
    kv.mu.Unlock()

    if needReconfig {
      kv.reconfig(&newConfig)
    }

    time.Sleep(100 * time.Millisecond)
  }
}

func (kv *ShardKV) reconfig(newConfig *shardctrler.Config) {
  PrintDebug("%v: Update shards: %v", kv.gid, newConfig.Shards)

  // Send config args
  args := ConfigArgs {
    Num:     newConfig.Num,
    Shards:  newConfig.Shards[:],
    Groups:  newConfig.Groups,
  }

  op := Op {
    From:      kv.gid,
    Type:      OpConfig,
    ClerkId:   int64(kv.gid),
    CmdId:     int64(args.Num),
    ConfigArg: args,
  }

  kv.commitOp(&op)
}

func (kv *ShardKV) pushShards() {
  for {
    receiverGid := kv.getReceivers()

    for gid, _ := range receiverGid {
      go kv.sendShards(&kv.prevShard, gid)
    }

    time.Sleep(100 * time.Millisecond)
  }
}

// Caller must not hold mutex.
// Deduplicate receiver gid. Returns empty if not leader.
func (kv *ShardKV) getReceivers() map[int]bool {
  receiverGid := make(map[int]bool)

  _, isLeader := kv.rf.GetState()
  if !isLeader {
    return receiverGid
  }

  kv.mu.Lock()
  defer kv.mu.Unlock()

  for shard, state := range kv.shardState {
    if state == ShardHandoff {
      gid := kv.receivers[shard]
      receiverGid[gid] = true
    }
  }
  return receiverGid
}

func (kv *ShardKV) sendShards(args *ShardsArgs, gid int) {
  for {
    kv.mu.Lock()
    servers, exists := kv.config.Groups[gid]
    kv.mu.Unlock()

    PrintDebug("sendShards: %v -> %v, data: %v",
      kv.gid, gid, args.Data)

    if exists {
      for si := 0; si < len(servers); si++ {
        var reply ShardsReply

        srv := kv.make_end(servers[si])
        ok := srv.Call("ShardKV.ReceiveShards", args, &reply)
        if ok && reply.Err == OK {
          kv.completePushShards(reply.Shards)
          return
        }
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
}

// Caller must not hold mutex.
func (kv *ShardKV) completePushShards(shards []int) {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  // TODO: We may need to use raft to save the state.
  for _, shard := range shards {
    kv.shardState[shard] = ShardReady
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
    d.Decode(&kv.history) != nil ||
    d.Decode(&kv.shardState) != nil ||
    d.Decode(&kv.prevConfig) != nil ||
    d.Decode(&kv.prevShard) != nil ||
    d.Decode(&kv.receivers) != nil {
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

  // PrintDebugGreen("%v: Receive     %v from: %v, index: %v",
  //   kv.gid, op.CmdId, op.From, msg.CommandIndex)

  kv.mu.Lock()
  defer kv.mu.Unlock()

  reply := kv.processOp(op)

  if kv.needSnapshot() {
    kv.sendSnapshot(msg.CommandIndex)
  }


  handler, exists := kv.handlers[msg.CommandIndex]
  if exists {
    *handler <- reply
  }
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
  e.Encode(&kv.shardState)
  e.Encode(&kv.prevConfig)
  e.Encode(&kv.prevShard)
  e.Encode(&kv.receivers)

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

func (kv* ShardKV) emptyReply() Reply {
  reply := Reply {
    GetReply:       GetReply       { Err: OK },
    PutAppendReply: PutAppendReply { Err: OK },
    ConfigReply:    ConfigReply    { Err: OK },
    ShardsReply:    ShardsReply    { Err: OK },
  }
  return reply
}

func (kv* ShardKV) setErr(reply *Reply, err Err) {
  reply.GetReply.Err       = err
  reply.PutAppendReply.Err = err
  reply.ConfigReply.Err    = err
  reply.ShardsReply.Err    = err
}

// Caller must hold the mutex.
func (kv *ShardKV) processOp(op Op) Reply {
  reply := kv.emptyReply()

  // TODO: Probably we don't need to check it.
  prevCmdId, _ := kv.history[op.ClerkId]
  if op.Type != OpReceiveShards && prevCmdId >= op.CmdId {
    PrintDebugYellow(
      "%v: %v is committed, clerkId: %v, cmdId: %v",
      kv.gid, op.Type, op.ClerkId, op.CmdId)
    return reply
  }

  PrintDebug(
    "%v: start to process: %v, clerkId: %v, cmdId: %v",
    kv.gid, op.Type, op.ClerkId, op.CmdId)

  kv.history[op.ClerkId] = op.CmdId

  switch op.Type {
  case OpGet:
    kv.processGet(&op.GetArg, &reply.GetReply)
  case OpPutAppend:
    kv.processPutAppend(&op.PutAppendArg, &reply.PutAppendReply)
  case OpReceiveShards:
    kv.processShards(&op.ShardsArg, &reply.ShardsReply)
  case OpConfig:
    kv.processConfig(&op.ConfigArg, &reply.ConfigReply)
  }
  return reply
}

func (kv *ShardKV) processGet(args *GetArgs, reply *GetReply) {
  if !kv.correctShard(args.Key) {
    shard := key2shard(args.Key)
    gid := kv.config.Shards[shard]
    PrintDebugRed(
      "%v: Get Wrong group: me: %v, target: %v",
      kv.gid, kv.gid, gid)
    reply.Err = ErrWrongGroup
  }

  kv.getValue(args.Key, reply)
}

// Caller must hold the mutex.
func (kv *ShardKV) correctShard(key string) bool {
  shard := key2shard(key)
  gid := kv.config.Shards[shard]
  return gid == kv.gid
}

func (kv *ShardKV) shardReady(key string) bool {
  shard := key2shard(key)
  if kv.shardState[shard] != ShardReady {
    PrintDebugRed(
      "%v: Shard %v is not ready: %v",
      kv.gid, shard, kv.shardState[shard])
  }
  return kv.shardState[shard] == ShardReady
}

// Caller must hold the mutex.
func (kv *ShardKV) processPutAppend(
  args *PutAppendArgs,
  reply *PutAppendReply,
) {
  PrintDebug(
    "%v: PutAppend: key: %v (shard: %v) -> value: %v cmdId: %v",
    kv.gid, args.Key, key2shard(args.Key), args.Value, 
    args.CmdId)

  if !kv.correctShard(args.Key) || !kv.shardReady(args.Key) {
    if !kv.correctShard(args.Key) {
      shard := key2shard(args.Key)
      gid := kv.config.Shards[shard]
      PrintDebugRed(
        "%v: PutAppend Wrong group: me: %v, target: %v, cmdId: %v",
        kv.gid, kv.gid, gid, args.CmdId)
    }
    reply.Err = ErrWrongGroup
    return
  }

  if args.Op == "Put" {
    kv.data[args.Key] = args.Value
  } else if args.Op == "Append" {
    oldValue, _ := kv.data[args.Key]
    kv.data[args.Key] = oldValue + args.Value
  }
  reply.Err = OK
}

// Caller must hold the mutex.
func (kv *ShardKV) processConfig(args *ConfigArgs, reply *ConfigReply) {
  PrintDebug("%v: processConfig: shard: %v", kv.gid, args.Shards)

  kv.saveShards(args)
  PrintDebugYellow("%v: processConfig: updateState: %v", kv.gid, args.Shards)
  kv.updateShardState(args.Shards[:])
  PrintDebugYellow("%v: processConfig: setReceivers: %v", kv.gid, args.Shards)
  kv.setShardsReceivers(args.Shards[:])
  PrintDebugYellow("%v: processConfig: prev: %v", kv.gid, args.Shards)
  kv.prevConfig = kv.config

  // Update config.
  kv.config.Num = args.Num

  for shard, gid := range args.Shards {
    kv.config.Shards[shard] = gid
  }

  kv.config.Groups = make(map[int][]string)
  for gid, servers := range args.Groups {
    kv.config.Groups[gid] = servers
  }

  reply.Err = OK
  PrintDebugYellow("%v: processConfig: done: %v", kv.gid, args.Shards)
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
  receivers := make(map[int]int)
  for i, newGid := range shards {
    if kv.config.Shards[i] == kv.gid && newGid != kv.gid {
      receivers[i] = newGid
    }
  }

  kv.receivers = receivers
}

// Caller must hold the mutex.
func (kv *ShardKV) isNeeded(key string, from int64) bool {
  shard := key2shard(key)
  prevShard := kv.prevConfig.Shards[shard]
  return (prevShard == int(from) || prevShard == 0) &&
    kv.config.Shards[shard] == kv.gid
}

// Caller must hold the mutex.
func (kv *ShardKV) processShards(
  args *ShardsArgs,
  reply *ShardsReply,
) {
  reply.Err = OK

  // Update shard data
  if args.Num >= kv.config.Num {
    for key, value := range args.Data {
      if kv.isNeeded(key, args.ClerkId) {
        kv.data[key] = value
        PrintDebug(
          "             %v: receive shards: key: %v, value: %v",
          kv.gid, key, value)
      }
    }
  }

  PrintDebugYellow("%v: processShards prev shards %v", kv.gid, kv.prevConfig.Shards)
  PrintDebugYellow("%v: processShards new  shards %v", kv.gid, kv.config.Shards)
  receiveShards := []int{}
  for shard, gid := range kv.prevConfig.Shards {
    if (gid == int(args.ClerkId) || gid == 0) &&
       kv.config.Shards[shard] == kv.gid {
      kv.shardState[shard] = ShardReady
      receiveShards = append(receiveShards, shard)
    }
  }
  PrintDebugYellow("%v: processShards done receive shards %v", kv.gid, receiveShards)

  reply.Shards = receiveShards
}

// Caller mustn't hold the mutex.
func (kv *ShardKV) isCommitted(clerkId int64, cmdId int64) bool {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  prevCmdId, _ := kv.history[clerkId]
  return prevCmdId >= cmdId
}

func (kv *ShardKV) setHandler(index int, handler *chan Reply) {
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

  PrintDebug(
    "%v: start to delete handler: %v", kv.gid, index)
  oldHandler, exists := kv.handlers[index]
  if exists {
    close(*oldHandler)
  }

  delete(kv.handlers, index)
}

func (kv *ShardKV) commitOp(op *Op) Reply {
  reply := kv.emptyReply()

  if op.Type != OpReceiveShards && kv.isCommitted(op.ClerkId, op.CmdId) {
    PrintDebugYellow(
      "%v: op (clerkId %v, cmdId %v) is committed",
      kv.gid, op.ClerkId, op.CmdId)
    return reply
  }

  index, term, isLeader := kv.rf.Start(*op)
  if !isLeader {
    PrintDebugYellow(
      "%v: op (clerkId %v, cmdId %v) wrong leader",
      kv.gid, op.ClerkId, op.CmdId)
    kv.setErr(&reply, ErrWrongLeader)
    return reply
  }

  PrintDebug("%v: commit op: type: %v, clerkId: %v, cmdId: %v, index %v",
    kv.gid, op.Type, op.ClerkId, op.CmdId, index)

  return kv.waitForRaft(index, term)
}

func (kv *ShardKV) waitForRaft(index int, term int) Reply {
  wait := make(chan Reply)

  kv.setHandler(index, &wait)
  defer kv.deleteHandler(index)

  reply := kv.emptyReply()

  select {
  case <-time.After(opTimeout * time.Millisecond):
    kv.setErr(&reply, ErrTimeout)
    return reply
  case reply = <-wait:
  }

  currTerm, isLeader := kv.rf.GetState()
  if !isLeader || currTerm != term {
    PrintDebugYellow("Lose leader.")
    kv.setErr(&reply, ErrLoseLeader)
  }

  return reply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  PrintDebug("%v: %v %v: {%v} (shard: %v) cmdId: %v",
    kv.gid, args.CmdId, "Get", args.Key, key2shard(args.Key), args.CmdId)

  // kv.mu.Lock()
  // if !kv.correctShard(args.Key) {
  //   reply.Err = ErrWrongLeader
  //   kv.mu.Unlock()
  //   return
  // }
  // kv.mu.Unlock()

  op := Op {
    From:    kv.gid,
    Type:    OpGet,
    ClerkId: args.ClerkId,
    CmdId:   args.CmdId,
    GetArg:  *args,
  }

  *reply = kv.commitOp(&op).GetReply
}

// Caller should not hold the mutex.
func (kv *ShardKV) getValue(key string, reply *GetReply) {
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
  PrintDebug("%v: %v %s: %v (shard: %v) -> {%v}",
    kv.gid, args.CmdId, args.Op, args.Key, key2shard(args.Key), args.Value)

  op := Op {
    From:         kv.gid,
    Type:         OpPutAppend,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    PutAppendArg: *args,
  }

  *reply = kv.commitOp(&op).PutAppendReply
}

func (kv *ShardKV) ReceiveShards(args *ShardsArgs, reply *ShardsReply) {
  PrintDebugYellow("%v: receive shards. config num: %v", kv.gid, args.Num)

  op := Op {
    From:         kv.gid,
    Type:         OpReceiveShards,
    ClerkId:      args.ClerkId,
    CmdId:        args.CmdId,
    ShardsArg:    *args,
  }

  *reply = kv.commitOp(&op).ShardsReply
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
	kv.gid = me
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
  kv.handlers = make(map[int]*chan Reply)

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
  go kv.pushShards()

	return kv
}
