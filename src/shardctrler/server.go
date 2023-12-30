package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "fmt"
import "time"
import "sort"


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

func PrintDebugRed(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("\033[31m%s %s\033[0m\n", curr, fmt.Sprintf(format, a...))
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

type GroupShard struct {
  gid    int
  has    int
  need int
}


func (sc *ShardCtrler) commitOp(op *Op) Err {
  if sc.isCommitted(op.ClerkId, op.CmdId) {
    return OK
  }

  index, term, isLeader := sc.rf.Start(*op)
  if !isLeader {
    return ErrWrongLeader
  }
  PrintDebug("commit op: type: %v, clerkId: %v, cmdId: %v",
    op.Type, op.ClerkId, op.CmdId)

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
    PrintDebugYellow("Lose leader.")
    return ErrLoseLeader
  }

  PrintDebugYellow(
    "%v: receive raft index: %v, term: %v", sc.me, index, term)
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
  for gid, servers := range reply.Config.Groups {
    PrintDebug("Query: gid: %v servers: %v", gid, servers)
  }
}

// Caller should not hold mutex.
func (sc *ShardCtrler) getConfig(args *QueryArgs) Config {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  configNum := args.Num
  if configNum  == -1 || configNum >= len(sc.configs) {
    configNum = len(sc.configs) - 1
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

    sc.mu.Lock()

    sc.processOp(op)

    handler, exists := sc.handlers[msg.CommandIndex]

    if exists {
      *handler <- msg
    }

    sc.mu.Unlock()

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

// Caller should not hold the mutex.
func (sc *ShardCtrler) processJoin(args *JoinArgs) {
  PrintDebugGreen(
    "Process Join: clerk: %v, cmd: %v", args.ClerkId, args.CmdId)

  sc.addConfig()
  config := &sc.configs[len(sc.configs) - 1]
  for gid, servers := range args.Servers {
    config.Groups[gid] = make([]string, len(servers))
    copy(config.Groups[gid], servers)
  }

  groupShards, groupOrder := sc.getGroupShard(config)
  sc.rebalance(groupShards, groupOrder, config)
}

func (sc *ShardCtrler) processLeave(args *LeaveArgs) {
  PrintDebugGreen(
    "Process Leave: clerk: %v, cmd: %v, GIDs: %v",
    args.ClerkId, args.CmdId, args.GIDs)

  sc.addConfig()
  config := &sc.configs[len(sc.configs) - 1]
  sc.removeGIDs(args.GIDs, config)

  if len(config.Groups) > 0 {
    groupShards, groupOrder := sc.getGroupShard(config)
    groupShards[0] = GroupShard{}

    sc.rebalance(groupShards, groupOrder, config)
  }
}

func (sc *ShardCtrler) removeGIDs(gids []int, config *Config) {
  for _, gid := range gids {
    delete(config.Groups, gid)
  }

  gidMap := map[int]bool{}
  for _, gid := range gids {
    gidMap[gid] = true
  }

  for index, gid := range config.Shards {
    _, isExisted := gidMap[gid]
    if isExisted {
      config.Shards[index] = 0
    }
  }
}

func (sc *ShardCtrler) getGroupShard(
  config *Config,
) (map[int]GroupShard, []GroupShard) {
  targets := sc.createTargetSlice(len(config.Groups))
  for id, value := range targets {
    PrintDebug(
      "target: index: %v, value: %v", id, value)
  }

  groupShards, groupOrder := sc.createGroupShard(config)
  for id, g := range groupOrder {
    PrintDebug(
      "order: index: %v, gid: %v, has: %v",
      id, g.gid, g.has)
  }

  for id, groupShard := range groupOrder {
    g := groupShards[groupShard.gid]
    g.need = targets[id]
    groupShards[groupShard.gid] = g
    PrintDebug(
      "groupShards: gid: %v, need: %v", groupShard.gid, g.need)
  }

  return groupShards, groupOrder
}

func (sc *ShardCtrler) rebalance(
  groupShards map[int]GroupShard,
  groupOrder  []GroupShard,
  config      *Config,
) {
  smallestId := 0
  for shardId, gid := range config.Shards {
    fromGroup := groupShards[gid]
    if fromGroup.need > 0 {
      fromGroup.need--
      groupShards[gid] = fromGroup
    } else {
      to := groupOrder[smallestId].gid
      for groupShards[to].need <= 0 {
        PrintDebugGreen(
          "rebalance: to: %v, need: %v", to, groupShards[to].need)
        smallestId++
        to = groupOrder[smallestId].gid
      }

      config.Shards[shardId] = to

      toGroup := groupShards[to]
      toGroup.need--
      groupShards[to] = toGroup
    }
  }

  for shardId, gid := range config.Shards {
    PrintDebugGreen("rebalance: shardId: %v, gid: %v", shardId, gid)
  }
}

func (sc *ShardCtrler) createGroupShard(
  config *Config,
) (map[int]GroupShard, []GroupShard) {
  groupShards := make(map[int]GroupShard)
  for gid := range config.Groups {
    if gid == 0 {
      continue
    }
    groupShards[gid] = GroupShard{}
  }
  for _, gid := range config.Shards {
    if gid == 0 {
      continue
    }
    g := groupShards[gid]
    g.has++
    groupShards[gid] = g
  }

  groupOrder := make([]GroupShard, len(groupShards))
  i := 0
  for gid, groupShard := range groupShards {
    groupOrder[i] = GroupShard {
      gid: gid,
      has: groupShard.has,
    }
    i++
  }

  sort.Slice(groupOrder, func(i, j int) bool {
    g1 := groupOrder[i]
    g2 := groupOrder[j]
    return (g1.has < g2.has) ||
      (g1.has == g2.has && g1.gid < g2.gid)
  })

  return groupShards, groupOrder
}

func (sc *ShardCtrler) createTargetSlice(groupSize int) []int {
  target := make([]int, groupSize)

  remainShard := NShards
  for i := 0; i < len(target); i++ {
    quota := remainShard / (groupSize - i)
    target[i] = quota
    remainShard -= quota
  }

  return target
}

func (sc *ShardCtrler) processMove(args *MoveArgs) {
  sc.addConfig()
  config := &sc.configs[len(sc.configs) - 1]
  config.Shards[args.Shard] = args.GID
}

// Caller must hold the mutex.
// Duplicate the latest config into configs.
func (sc *ShardCtrler) addConfig() {
  latestConfig := sc.configs[len(sc.configs) - 1]

  config := Config {
    Num:    len(sc.configs),
    Groups: make(map[int][]string),
  }

  for i := 0; i < len(config.Shards); i++ {
    config.Shards[i] = latestConfig.Shards[i]
  }

  for gid, servers := range latestConfig.Groups {
    config.Groups[gid] = make([]string, len(servers))
    copy(config.Groups[gid], servers)
  }

  sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) isCommitted(clerkId int64, cmdId int64) bool {
  sc.mu.Lock()
  defer sc.mu.Unlock()

  prevCmdId, _ := sc.history[clerkId]

  if prevCmdId >= cmdId {
    PrintDebug("cmd: clerkId: %v, cmdId: %v is committed", clerkId, cmdId)
  }
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
  sc.history = make(map[int64]int64)
  sc.handlers = make(map[int]*chan raft.ApplyMsg)

  go sc.receiveCmd()

	return sc
}
