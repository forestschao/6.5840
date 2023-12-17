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
	return ck
}

func (ck *Clerk) updateLeader () {
  ck.leaderId++
  if ck.leaderId >= len(ck.servers) {
    ck.leaderId = 0
  }
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
    Key: key,
  }
  reply := GetReply{}

  rpc := fmt.Sprintf("KVServer.%s", RpcGet)
  for {
    ok := ck.servers[ck.leaderId].Call(rpc, &args, &reply)
    if ok && reply.Err != ErrWrongLeader {
      break
    }

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
    Key: key,
    Value: value,
    Op: op,
  }
  reply := PutAppendReply{}

  rpc := fmt.Sprintf("KVServer.%s", RpcPutAppend)
  for {
    ok := ck.servers[ck.leaderId].Call(rpc, &args, &reply)
    if ok && reply.Err != ErrWrongLeader {
      break
    }

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
