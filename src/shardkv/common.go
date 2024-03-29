package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK               = "OK"
	ErrNoKey         = "ErrNoKey"
	ErrWrongGroup    = "ErrWrongGroup"
	ErrWrongLeader   = "ErrWrongLeader"
	ErrTimeout       = "ErrTimeout"
	ErrLoseLeader    = "ErrLoseLeader"
	ErrShardNotReady = "ErrShardNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
  ClerkId int64
  CmdId   int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
  ClerkId int64
  CmdId   int64
}

type GetReply struct {
	Err   Err
	Value string
}

type ConfigArgs struct {
  Num     int
  Shards  []int
  Groups  map[int][]string
}

type ConfigReply struct {
  Err  Err
}

type ShardsArgs struct {
  ClerkId int64
  CmdId   int64
  Num     int
  Data    map[string]string
  History map[int64]int64
}

type ShardsReply struct {
  Err    Err
  Gid    int
  Num    int
  Shards []int
}

type ShardStateArgs struct {
  Gid    int
  Num    int
	Shards []int
}

type ShardStateReply struct {
	Err
}

