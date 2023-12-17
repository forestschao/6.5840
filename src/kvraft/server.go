package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
  "fmt"
  // "log"
	"sync"
	"sync/atomic"
  "time"
)

const Debug = false

func PrintDebug(format string, a ...interface{}) {
	if !Debug {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("%s %s\n", curr, fmt.Sprintf(format, a...))
}

const opTimeout = 100 // milliseconds

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
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
}

// Caller must hold the mutex.
func (kv *KVServer) startOp(op *Op) Err {
  _, _, isLeader := kv.rf.Start(op)
  if !isLeader {
    return ErrWrongLeader
  }

  select {
    case <-kv.applyCh:
      return OK
    case <-time.After(opTimeout * time.Millisecond):
      return ErrTimeout
  }
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
  op := Op{}

  kv.mu.Lock()
  defer kv.mu.Unlock()

  status := kv.startOp(&op)
  if status != OK {
    reply.Err = status
    return
  }

  value, exists := kv.data[args.Key]
  if !exists {
    reply.Value = ""
    reply.Err = ErrNoKey
    return
  }

  reply.Value = value
  reply.Err = OK
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
  op := Op{}

  kv.mu.Lock()
  defer kv.mu.Unlock()

  status := kv.startOp(&op)
  if status != OK {
    reply.Err = status
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

	return kv
}
