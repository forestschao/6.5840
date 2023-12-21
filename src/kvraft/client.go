package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "fmt"

const (
  RpcPutAppend = "PutAppend"
  RpcGet       = "Get"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
  leaderId int // Index of the KVServer leader
  me       int64
  cmdId    int64 // Command index
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
  ck.me = nrand()
  ck.leaderId = int(nrand() % int64(len(servers)))
	return ck
}

func (ck *Clerk) updateLeader () {
  ck.leaderId++
  if ck.leaderId >= len(ck.servers) {
    ck.leaderId = 0
  }
}
func (ck *Clerk) getCmdId() int64 {
  ck.cmdId++
  return ck.cmdId
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
  args := GetArgs {
    ClerkId: ck.me,
    CmdId: ck.getCmdId(),
    Key: key,
  }
  reply := GetReply{}

  rpc := fmt.Sprintf("KVServer.%s", RpcGet)
  for {
    PrintDebug(
      "%v: Get leader:%v, %v: {%v}", ck.me, ck.leaderId, args.CmdId, key)
    ok := ck.servers[ck.leaderId].Call(rpc, &args, &reply)
    if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
      break
    }

    PrintDebug(
      "%v: Get leader:%v, %v: error {%v}",
      ck.me, ck.leaderId, args.CmdId, reply.Err)
    ck.updateLeader()
  }

  if reply.Err == OK {
    return reply.Value
  }

  PrintDebug("Get error: %s", reply.Err)
	return ""
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
  args := PutAppendArgs {
    ClerkId: ck.me,
    CmdId: ck.getCmdId(),
    Key: key,
    Value: value,
    Op: op,
  }
  reply := PutAppendReply{}

  rpc := fmt.Sprintf("KVServer.%s", RpcPutAppend)
  for {
    PrintDebug("%v: %v leader: %v, %v: %v -> {%v}",
      ck.me, op, ck.leaderId, args.CmdId, args.Key, args.Value)
    ok := ck.servers[ck.leaderId].Call(rpc, &args, &reply)
    if ok && reply.Err == OK {
      break
    }

    PrintDebug("%v: %v result: %v", ck.me, op, reply.Err)
    ck.updateLeader()
  }

  if reply.Err != OK {
    PrintDebug("%s error: %s", op, reply.Err)
  }
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
