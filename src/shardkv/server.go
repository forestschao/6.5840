package shardkv

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
)

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

func PrintDebugInternal(color string, format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	reset := "\033[0m"
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("%s%s %s%s\n",
		color, curr, fmt.Sprintf(format, a...), reset)
}

const (
	OpGet           = "Get"
	OpPutAppend     = "PutAppend"
	OpReceiveShards = "ReceiveShards"
	OpConfig        = "Config"
	OpShardState    = "ShardState"

	ShardReady      = "ShardReady"
	ShardHandoff    = "ShardHandoff"
	ShardWaiting    = "ShardWaiting"
	ShardWrongGroup = "ShardWrongGroup"
)

const opTimeout = 100 // Milliseconds

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	From          int
	Type          string
	ClerkId       int64
	CmdId         int64
	GetArg        GetArgs
	PutAppendArg  PutAppendArgs
	ConfigArg     ConfigArgs
	ShardsArg     ShardsArgs
	ShardStateArg ShardStateArgs
}

type ShardStateArgs struct {
  Gid    int
  Num    int
	Shards []int
}

type ShardStateReply struct {
	Err
}

type Reply struct {
	GetReply        GetReply
	PutAppendReply  PutAppendReply
	ConfigReply     ConfigReply
	ShardsReply     ShardsReply
	ShardStateReply ShardStateReply
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
	data     map[string]string   // Key -> Value
	history  map[int64]int64     // Clerk Id -> Latest CmdId
	handlers map[int]*chan Reply // Log index -> handler

	shardState [shardctrler.NShards]string

	persister *raft.Persister

	sm        *shardctrler.Clerk
	config    shardctrler.Config
	prevShard ShardsArgs
	receivers map[int]int // Shard Id -> Receiver GID
}

func (kv *ShardKV) receiveConfig() {
	for {
		var newConfig shardctrler.Config

		if kv.getNextConfig(&newConfig) {
			kv.reconfig(&newConfig)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) getNextConfig(
	newConfig *shardctrler.Config,
) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

  // Check whether server is ready to get new config.
	// if it is waiting for shards or sending shards,
	// do not reconfig.
	for shard, state := range kv.shardState {
		if state != ShardReady {
			PrintDebugYellow(
				"%v_%v: skip reconfig: shard %v: %v",
				kv.gid, kv.me, shard, state)
			return false
		}
	}

	nextConfigNum := kv.config.Num + 1
	*newConfig = kv.sm.Query(nextConfigNum)

	return newConfig.Num > kv.config.Num
}

func (kv *ShardKV) reconfig(newConfig *shardctrler.Config) {
	PrintDebug(
    "%v_%v: reconfig shards: %v\n                                  -> %v",
    kv.gid, kv.me, kv.config.Shards, newConfig.Shards)

	// Send config args
	args := ConfigArgs{
		Num:    newConfig.Num,
		Shards: newConfig.Shards[:],
		Groups: newConfig.Groups,
	}

	op := Op{
		From:      kv.gid,
		Type:      OpConfig,
		ClerkId:   int64(kv.gid),
		CmdId:     int64(newConfig.Num),
		ConfigArg: args,
	}

  for {
    reply := kv.commitOp(&op).ConfigReply

    if reply.Err == ErrWrongLeader || reply.Err == OK {
      return
    }
  }
}

func (kv *ShardKV) sendShards(args *ShardsArgs, gid int) {
	for {
		kv.mu.Lock()
		servers, exists := kv.config.Groups[gid]
		kv.mu.Unlock()


		if exists {
			for si := 0; si < len(servers); si++ {
        PrintDebug("sendShards: %v_%v -> %v_%v, (num: %v) data: %v",
          kv.gid, kv.me, gid, si, args.CmdId, args.Data)
				var reply ShardsReply

				srv := kv.make_end(servers[si])
				ok := srv.Call("ShardKV.ReceiveShards", args, &reply)
				if ok && reply.Err == OK {
					kv.completePushShards(reply)
					return
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) pushShards() {
	for {
		time.Sleep(100 * time.Millisecond)
    _, isLeader := kv.rf.GetState()
    if !isLeader {
      continue
    }

		receiverGid := kv.getReceivers()

		for gid, _ := range receiverGid {
			go kv.sendShards(&kv.prevShard, gid)
		}

	}
}

// Caller must not hold mutex.
// Deduplicate receiver gid. Returns empty if not leader.
func (kv *ShardKV) getReceivers() map[int]bool {
	receiverGid := make(map[int]bool)

	// _, isLeader := kv.rf.GetState()
	// if !isLeader {
	// 	return receiverGid
	// }

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

// Caller must not hold mutex.
func (kv *ShardKV) completePushShards(reply ShardsReply) {
	PrintDebugGreen(
    "%v_%v: complete shards: %v, config: %v",
      kv.gid, kv.me, reply.Shards, reply.Num)
	args := ShardStateArgs{
    Gid:    reply.Gid,
    Num:    reply.Num,
		Shards: reply.Shards,
	}

	for {
		op := Op{
			From:          kv.gid,
			Type:          OpShardState,
			ClerkId:       int64(kv.me),
			CmdId:         time.Now().Unix(),
			ShardStateArg: args,
		}

		reply := kv.commitOp(&op).ShardStateReply

		if reply.Err == OK || reply.Err == ErrWrongLeader {
			break
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
	PrintDebugGreen("%v_%v: start to ingest snapshot", kv.gid, kv.me)
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
    d.Decode(&kv.config) != nil ||
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

	kv.mu.Lock()
	defer kv.mu.Unlock()

	reply := kv.processOp(op)

	handler, exists := kv.handlers[msg.CommandIndex]
	if exists {
		*handler <- reply
	}

	if kv.needSnapshot() {
		kv.sendSnapshot(msg.CommandIndex)
	}
}

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate > 0 &&
		kv.persister.RaftStateSize() > kv.maxraftstate-100
}

func (kv *ShardKV) sendSnapshot(index int) {
	// Encode
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.history)
	e.Encode(&kv.shardState)
  e.Encode(&kv.config)
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

func (kv *ShardKV) Reply(err Err) Reply {
	reply := Reply{
		GetReply:        GetReply{Err: err},
		PutAppendReply:  PutAppendReply{Err: err},
		ConfigReply:     ConfigReply{Err: err},
		ShardsReply:     ShardsReply{Err: err},
		ShardStateReply: ShardStateReply{Err: err},
	}
	return reply
}

// Caller must hold the mutex.
func (kv *ShardKV) processOp(op Op) Reply {
	// TODO: Probably we don't need to check it.
	prevCmdId, _ := kv.history[op.ClerkId]
	if op.Type == OpPutAppend && prevCmdId >= op.CmdId {
		PrintDebugYellow(
			"%v_%v: %v is committed, clerkId: %v, cmdId: %v",
			kv.gid, kv.me, op.Type, op.ClerkId, op.CmdId)
		return kv.Reply(OK)
	}

	PrintDebug(
		"%v_%v: start to process: %v, clerkId: %v, cmdId: %v",
		kv.gid, kv.me, op.Type, op.ClerkId, op.CmdId)


  kv.history[op.ClerkId] = op.CmdId

	reply := kv.Reply(OK)
	switch op.Type {
	case OpGet:
		kv.processGet(&op.GetArg, &reply.GetReply)
	case OpPutAppend:
		kv.processPutAppend(&op.PutAppendArg, &reply.PutAppendReply)
	case OpReceiveShards:
		kv.receiveShards(&op.ShardsArg, &reply.ShardsReply)
	case OpConfig:
		kv.processConfig(&op.ConfigArg, &reply.ConfigReply)
	case OpShardState:
		kv.processShardState(&op.ShardStateArg, &reply.ShardStateReply)
	}
	return reply
}

func (kv *ShardKV) processShardState(
  args *ShardStateArgs,
  reply *ShardStateReply,
) {
  PrintDebug(
    "%v_%v: process shard state: config num: %v, shards: %v",
    kv.gid, kv.me, args.Num, args.Shards)

  if args.Num == kv.config.Num {
    PrintDebug(
      "%v_%v: update shard state: %v",
      kv.gid, kv.me, args.Shards)
    for _, shard := range args.Shards {
      kv.shardState[shard] = ShardReady
    }
  }
  reply.Err = OK
}

func (kv *ShardKV) checkKeyStatus(key string) Err {
	if !kv.correctShard(key) {
    return ErrWrongGroup
	}
	if !kv.shardReady(key) {
		return ErrShardNotReady
	}

  return OK
}

func (kv *ShardKV) processGet(args *GetArgs, reply *GetReply) {
  state := kv.checkKeyStatus(args.Key)
  if state != OK {
    reply.Err = state
    return
  }

	kv.getValue(args.Key, reply)
}

// Caller must hold the mutex.
func (kv *ShardKV) correctShard(key string) bool {
	shard := key2shard(key)
	gid := kv.config.Shards[shard]

  if gid != kv.gid {
		PrintDebugRed(
      "%v_%v: Get Wrong group: key: %v(shard: %v) me: %v, target: %v",
			kv.gid, kv.me, key, shard, kv.gid, gid)
  }
	return gid == kv.gid
}

func (kv *ShardKV) shardReady(key string) bool {
	shard := key2shard(key)
	if kv.shardState[shard] != ShardReady {
		PrintDebugRed(
			"%v_%v: Shard %v is not ready: %v",
			kv.gid, kv.me, shard, kv.shardState[shard])
	}
	return kv.shardState[shard] == ShardReady
}

// Caller must hold the mutex.
func (kv *ShardKV) processPutAppend(
	args *PutAppendArgs,
	reply *PutAppendReply,
) {
	PrintDebug(
		"%v_%v: PutAppend: key: %v (shard: %v) -> value: %v cmdId: %v",
		kv.gid, kv.me, args.Key, key2shard(args.Key), args.Value,
		args.CmdId)

  state := kv.checkKeyStatus(args.Key)
  if state != OK {
    reply.Err = state
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
  PrintDebug("%v_%v: processConfig: num: %v shard: %v",
    kv.gid, kv.me, args.Num, args.Shards)

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

	reply.Err = OK
	PrintDebugYellow(
    "%v_%v: processConfig: done: %v", kv.gid, kv.me, args.Shards)
}

func (kv *ShardKV) saveShards(newConfig *ConfigArgs) {
	// Copy all the data of the original shards.
	data := make(map[string]string)
	for key, value := range kv.data {
		if kv.correctShard(key) {
			data[key] = value
		}
	}

	kv.prevShard = ShardsArgs{
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

func (kv *ShardKV) setShardsReceivers(newShards []int) {
	receivers := make(map[int]int)
	for i, newGid := range newShards {
		if kv.config.Shards[i] == kv.gid && newGid != kv.gid {
			receivers[i] = newGid
		}
	}

	kv.receivers = receivers
}

func (kv *ShardKV) needUpdateShards(
  configNum int,
  receiveShards []int,
) bool {
  if configNum != kv.config.Num {
    return false
  }

  for _, shard := range receiveShards {
    if kv.shardState[shard] == ShardReady {
      PrintDebugYellow(
        "%v_%v: skip receiveShards, receive shards %v",
        kv.gid, kv.me, receiveShards)
      return false
    }
  }

  return true
}

// Caller must hold the mutex.
func (kv *ShardKV) receiveShards(
	args *ShardsArgs,
	reply *ShardsReply,
) {
	reply.Err = OK
  reply.Gid = kv.gid
  reply.Num = args.Num

	receiveShards := kv.getReceiveShards(args.Num, args.ClerkId)

	// Update shard data
  if kv.needUpdateShards(args.Num, receiveShards) {
		for key, value := range args.Data {
			if kv.correctShard(key) {
				kv.data[key] = value
        shard := key2shard(key)
				PrintDebug(
          "             %v_%v: receive shards: key: %v (shard: %v), value: %v",
					kv.gid, kv.me, key, shard, value)
			}
		}

		for _, shard := range receiveShards {
			kv.shardState[shard] = ShardReady
		}
	}

	PrintDebugYellow(
		"%v_%v: receiveShards from %v done receive shards %v",
    kv.gid, kv.me, args.ClerkId, receiveShards)

	reply.Shards = receiveShards
}

func (kv *ShardKV) getShards(configNum int) []int {
	if configNum == kv.config.Num {
		return kv.config.Shards[:]
	}

	config := kv.sm.Query(configNum)
	return config.Shards[:]
}

// Caller must hold the mutex.
func (kv *ShardKV) getReceiveShards(currNum int, from int64) []int {
	if currNum > kv.config.Num {
		return make([]int, 0)
	}

	curr := kv.getShards(currNum)
	prev := kv.getShards(currNum - 1)

	receiveShards := []int{}
	for shard, prevGid := range prev {
		if (prevGid == int(from) || prevGid == 0) &&
			curr[shard] == kv.gid {
			receiveShards = append(receiveShards, shard)
		}
	}
	return receiveShards
}

// Caller mustn't hold the mutex.
func (kv *ShardKV) isCommitted(op *Op) bool {
  if op.Type == OpReceiveShards || op.Type == OpShardState {
    return false
  }

	kv.mu.Lock()
	defer kv.mu.Unlock()

	prevCmdId, _ := kv.history[op.ClerkId]
	return prevCmdId >= op.CmdId
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

	oldHandler, exists := kv.handlers[index]
	if exists {
		close(*oldHandler)
	}

	delete(kv.handlers, index)
}

func (kv *ShardKV) commitOp(op *Op) Reply {
	if kv.isCommitted(op) {
		PrintDebugYellow(
			"%v_%v: op (clerkId %v, cmdId %v) is committed",
			kv.gid, kv.me, op.ClerkId, op.CmdId)
		return kv.Reply(OK)
	}

	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		PrintDebugYellow(
			"%v_%v: op (clerkId %v, cmdId %v) wrong leader",
			kv.gid, kv.me, op.ClerkId, op.CmdId)
		return kv.Reply(ErrWrongLeader)
	}

	PrintDebug("%v_%v: commit op: type: %v, clerkId: %v, cmdId: %v, index %v",
		kv.gid, kv.me, op.Type, op.ClerkId, op.CmdId, index)

	return kv.waitForRaft(index, term)
}

func (kv *ShardKV) waitForRaft(index int, term int) Reply {
	wait := make(chan Reply)

	kv.setHandler(index, &wait)
	defer kv.deleteHandler(index)

	var reply Reply
	select {
	case <-time.After(opTimeout * time.Millisecond):
		return kv.Reply(ErrTimeout)
	case reply = <-wait:
	}

	currTerm, isLeader := kv.rf.GetState()
	if !isLeader || currTerm != term {
		PrintDebugYellow("Lose leader.")
		return kv.Reply(ErrLoseLeader)
	}

	return reply
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	PrintDebug("%v_%v: %v %v: {%v} (shard: %v) cmdId: %v",
		kv.gid, kv.me, args.CmdId, "Get", args.Key,
		key2shard(args.Key), args.CmdId)

	op := Op{
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
	PrintDebug("%v_%v: %v %s: %v (shard: %v) -> {%v}",
		kv.gid, kv.me, args.CmdId, args.Op, args.Key,
		key2shard(args.Key), args.Value)

	op := Op{
		From:         kv.gid,
		Type:         OpPutAppend,
		ClerkId:      args.ClerkId,
		CmdId:        args.CmdId,
		PutAppendArg: *args,
	}

	*reply = kv.commitOp(&op).PutAppendReply
}

func (kv *ShardKV) ReceiveShards(args *ShardsArgs, reply *ShardsReply) {
	PrintDebugYellow(
		"%v_%v: receive shards from %v. config num: %v",
    kv.gid, kv.me, args.ClerkId, args.Num)

	op := Op{
		From:      kv.gid,
		Type:      OpReceiveShards,
		ClerkId:   args.ClerkId,
		CmdId:     args.CmdId,
		ShardsArg: *args,
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
	kv.handlers = make(map[int]*chan Reply)

	kv.persister = persister
	snapshot := persister.ReadSnapshot()
	if snapshot != nil && len(snapshot) > 0 {
		kv.ingestSnap(snapshot)
	}

	kv.sm = shardctrler.MakeClerk(ctrlers)

	for i := 0; i < len(kv.shardState); i++ {
		kv.shardState[i] = ShardReady
	}

	go kv.receiveMsg()
	go kv.receiveConfig()
	go kv.pushShards()

	return kv
}
