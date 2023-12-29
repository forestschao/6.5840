package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "fmt"
import "time"


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
  OpJoin  = "Join"
  OpLeave = "Leave"
  OpMove  = "Move"
  OpQuery = "Query"
)

const opTimeout = 100 // Milliseconds

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
  cmdNum   int                         // Starts with 0
	configs  []Config                    // indexed by config num
  history  map[int64]int64             // Clerk Id -> Latest Cmd Id
  handlers map[int]*chan raft.ApplyMsg // Log index -> handler
}


type Op struct {
	// Your data here.
  Type      string
  ClerkId   int64
  CmdId     int64
  JoinArgs  JoinArgs
  LeaveArgs LeaveArgs
  MoveArgs  MoveArgs
  QueryArgs QueryArgs
}

func (sc *ShardCtrler) commitOp(op *Op) Err {
  if sc.isCommitted(op.ClerkId, op.CmdId) {
    return OK
  }

  index, term, isLeader := sc.rf.Start(op)
  if !isLeader {
    return ErrWrongLeader
  }

  return sc.waitForRaft(index, term)
}

func (sc *ShardCtrler) waitForRaft(index int, term int) Err {
  wait := make(chan raft.ApplyMsg)

  sc.setHandler(index, &wait)
  defer sc.deleteHandler(index)

  PrintDebugYellow(
    "%v: wait for raft index: %v, term: %v", sc.me, index, term)

  select {
  case <-time.After(opTimeout * time.Millisecond):
    return ErrTimeout
  case <-wait:
  }

  currTerm, isLeader := sc.rf.GetState()
  if !isLeader || currTerm != term {
    return ErrLoseLeader
  }

  return OK
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
  op := Op {
    Type:     OpJoin,
    ClerkId:  args.ClerkId,
    CmdId:    args.CmdId,
    JoinArgs: *args,
  }

  PrintDebugGreen("%v: %v %v: servers: %v",
    sc.me, op.Type, args.CmdId, args.Servers)

  reply.Err = sc.commitOp(&op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
  op := Op {
    Type:     OpLeave,
    ClerkId:  args.ClerkId,
    CmdId:    args.CmdId,
    LeaveArgs: *args,
  }

  PrintDebugGreen("%v: %v %v: %v",
    sc.me, op.Type, args.CmdId, args.GIDs)

  reply.Err = sc.commitOp(&op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
  op := Op {
    Type:     OpMove,
    ClerkId:  args.ClerkId,
    CmdId:    args.CmdId,
    MoveArgs: *args,
  }

  PrintDebugGreen("%v: %v %v: %v -> %v",
    sc.me, op.Type, args.CmdId, args.Shard, args.GID)

  reply.Err = sc.commitOp(&op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
  reply.Err = OK
  if !sc.isCommitted(args.ClerkId, args.CmdId) {
    op := Op {
      Type:    OpQuery,
      ClerkId: args.ClerkId,
      CmdId:   args.CmdId,
      QueryArgs:  *args,
    }
    reply.Err = sc.commitOp(&op)
  }

  if reply.Err != OK {
    return
  }

  PrintDebugGreen("%v: %s %v", sc.me, OpQuery, args.Num)

  reply.Config = sc.getConfig(args)
}

// Caller should not hold mutex.
func (sc *ShardCtrler) getConfig(args *QueryArgs) Config {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  configNum := args.Num
  if configNum  == -1 || configNum > sc.cmdNum {
    configNum = sc.cmdNum
  }
  return sc.configs[configNum]
}

func (sc *ShardCtrler) receiveCmd() {
  for msg := range sc.applyCh {
    op, ok := msg.Command.(Op)
    if !ok {
      PrintDebug("Cannot instantiate the command")
      return
    }

    PrintDebugGreen("%v: Receive     %v index: %v",
      sc.me, op.CmdId, msg.CommandIndex)

    sc.mu.Lock()
    defer sc.mu.Unlock()

    sc.processOp(op)

    handler, exists := sc.handlers[msg.CommandIndex]

    if exists {
      *handler <- msg
    }
    PrintDebugGreen("%v: UpdateState %v Done", sc.me, op.CmdId)
  }
}

func (sc *ShardCtrler) processOp(op Op) {
  prevCmdId, _ := sc.history[op.ClerkId]
  if prevCmdId >= op.CmdId {
    return
  }

  sc.history[op.ClerkId] = op.CmdId

  switch op.Type {
  case OpJoin:
    sc.processJoin(&op.JoinArgs)
  case OpLeave:
    sc.processLeave(&op.LeaveArgs)
  case OpMove:
    sc.processMove(&op.MoveArgs)
  }
}

// Caller must hold the mutex.
func (sc *ShardCtrler) processJoin(args *JoinArgs) {
}

func (sc *ShardCtrler) processLeave(args *LeaveArgs) {
}

func (sc *ShardCtrler) processMove(args *MoveArgs) {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  config := sc.addConfig()
  config.Shards[args.Shard] = args.GID
}

// Caller must hold the mutex.
// Duplicate the latest config into configs.
func (sc *ShardCtrler) addConfig() Config {
  latestConfig := sc.configs[sc.cmdNum]

  sc.cmdNum++
  config := Config {
    Num: sc.cmdNum,
  }

  for i := 0; i < len(config.Shards); i++ {
    config.Shards[i] = latestConfig.Shards[i]
  }

  for gid, servers := range latestConfig.Groups {
    config.Groups[gid] = make([]string, len(servers))
    copy(config.Groups[gid], servers)
  }

  sc.configs = append(sc.configs, config)

  return config
}

func (sc *ShardCtrler) isCommitted(clerkId int64, cmdId int64) bool {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  prevCmdId, _ := sc.history[clerkId]
  return prevCmdId >= cmdId
}

func (sc *ShardCtrler) setHandler(index int, handler *chan raft.ApplyMsg) {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  oldHandler, exists := sc.handlers[index]
  if exists {
    close(*oldHandler)
  }

  sc.handlers[index] = handler
}

func (sc *ShardCtrler) deleteHandler(index int) {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  PrintDebugYellow(
    "%v: start to delete handler: %v", sc.me, index)
  oldHandler, exists := sc.handlers[index]
  if exists {
    close(*oldHandler)
  }

  delete(sc.handlers, index)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(
  servers []*labrpc.ClientEnd,
  me int,
  persister *raft.Persister,
) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
  sc.cmdNum = 0
  config := Config{}
  sc.configs = append(sc.configs, config)

  go sc.receiveCmd()

	return sc
}
