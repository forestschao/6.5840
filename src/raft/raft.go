package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartbeatTimeout = 100
const commitInterval = 10
const DebugMode = false

func PrintDebug(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("%s %s\n", curr, fmt.Sprintf(format, a...))
}

func PrintDebugRed(format string, a ...interface{}) {
	if !DebugMode {
		return
	}
	curr := time.Now().Format("15:04:05.000")
	fmt.Printf("\033[31m%s %s\033[0m\n", curr, fmt.Sprintf(format, a...))
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

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Log struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int // Latest term server has seen
	votedFor    int // candidateId that received vote in current term
	// or -1 if none
	state        State     // current state of server
	voteReceived int       // number of votes for the server received
	lastUpdate   time.Time // true if received heartbeat from leader
	// within the election timeout

	logs        []Log // log entries. First index is 1
	commitIndex int   // index of highest log entry known to be committed
	// (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state
	// machine (intialized to 0, increases monotonically)
	nextIndex []int // for each server, index of the next log entry to
	// send to that server (initialized to leader last
	// log index + 1
	matchIndex []int // for each server, index of highest log entry known
	// to be replicated on server (initialized to 0,
	// increases monotonically)
	applyCh chan ApplyMsg

	startIndex        int // index of the first log entry in the logs.
	                      // Initialized to 1.
	lastIncludedTerm  int // Last included term of the latest snapshot.
	lastIncludedIndex int // Last included index of the latest snapshot.
  snapshot          []byte // Latest snapshot. Initialized to nil.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.startIndex)
	e.Encode(rf.lastIncludedTerm)
	e.Encode(rf.lastIncludedIndex)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
    rf.commitIndex = 0
    rf.lastApplied = 0

		rf.startIndex = 1
		rf.lastIncludedIndex = 0
		rf.lastIncludedTerm = 0
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []Log
	var startIndex, lastIncludedTerm, lastIncludedIndex int

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&startIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
		PrintDebug("Fail to read persist")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.startIndex = startIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastIncludedIndex = lastIncludedIndex
	}

	rf.commitIndex = startIndex - 1
	rf.lastApplied = startIndex - 1

	PrintDebug("%v read persist: logs: %v", rf.me, len(rf.logs))
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
  rf.mu.Lock()
  defer rf.mu.Unlock()

  arrayIndex := rf.getArrayIndex(index)
  if arrayIndex <= 0 {
    return
  }

  rf.lastIncludedIndex = index
  rf.lastIncludedTerm = rf.logs[arrayIndex].Term
  rf.startIndex = rf.lastIncludedIndex + 1

  rf.logs = rf.logs[arrayIndex + 1:]

  rf.snapshot = clone(snapshot)

  PrintDebugYellow(
    "%v: Snapshot: logIndex: %v, arrayIndex: %v " +
    "lastIndex: %v, lastTerm: %v, startIndex: %v, " +
    "log size: %v",
    rf.me, index, arrayIndex, rf.lastIncludedIndex,
    rf.lastIncludedTerm, rf.startIndex, len(rf.logs))

  rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // Candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// Caller must hold the mutex
func (rf *Raft) getLogIndex(arrayIndex int) int {
	return arrayIndex + rf.startIndex
}

// Caller must hold the mutex
func (rf *Raft) getArrayIndex(logIndex int) int {
	return logIndex - rf.startIndex
}

// Caller must hold the mutex
func (rf *Raft) getLastLogIndex() int {
  lastArrayIndex := len(rf.logs) - 1 // Could be -1 if array is empty.

  return rf.getLogIndex(lastArrayIndex)
}

// Caller must hold the mutex
func (rf *Raft) getLastLogTerm() int {
	lastIndex := len(rf.logs) - 1

  var lastLogTerm int
	if lastIndex < 0 {
		// All the logs are saved into the snapshot
		lastLogTerm = rf.lastIncludedTerm
	} else {
		lastLogTerm = rf.logs[lastIndex].Term
	}

  return lastLogTerm
}

// Caller must hold the mutex
func (rf *Raft) isUpToDate(args *RequestVoteArgs) bool {
	lastLogIndex := rf.getLastLogIndex()
  lastLogTerm := rf.getLastLogTerm()

	return args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	PrintDebug(
    "server: %v Get RequestVote: term: %v, candidate: %v",
    rf.me, args.Term, args.CandidateId)

	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term

	isUpdated := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.voteReceived = 0

		isUpdated = true
	}

	if rf.isUpToDate(args) &&
    (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if rf.votedFor == -1 {
			isUpdated = true
		}

		rf.votedFor = args.CandidateId
		rf.lastUpdate = time.Now()

		reply.VoteGranted = true

	}

	if isUpdated {
		rf.persist()
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	PrintDebugRed(
    "server: %v -> %v StartVote. term: %v", rf.me, server, args.Term)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	PrintDebug("%v receive sendRequestVote: %v", rf.me, *reply)

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.voteReceived = 0

		rf.persist()
		return ok
	}

	if reply.Term < rf.currentTerm {
		return ok
	}

	if reply.VoteGranted && rf.state != Leader {
		rf.voteReceived++

		if rf.voteReceived > len(rf.peers)/2 {
			rf.state = Leader

			rf.nextIndex = make([]int, len(rf.peers))
      nextIndex := rf.getLastLogIndex() + 1
			for i := range rf.nextIndex {
				rf.nextIndex[i] = nextIndex
			}

			rf.matchIndex = make([]int, len(rf.peers))
			for i := range rf.matchIndex {
				rf.matchIndex[i] = rf.lastIncludedIndex
			}

			// Send empty AppendEntries
			go rf.sendUpdates()
		}
	}

	return ok
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log // log entries to store (empty for heartbeat)
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex
	             // and prevLogTerm

	XTerm  int // term in the conflicting entry (if any)
	XIndex int // index of first entry with that term (if any)
	XLen   int // log length
}

// Caller must hold mutex.
func (rf *Raft) fillConflictTerm(
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) {
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.getLastLogIndex()

	// Doesn't have enough logs.
	if rf.getLastLogIndex() < args.PrevLogIndex {
		return
	}

	prevLogIndex := args.PrevLogIndex
	prevIndex := rf.getArrayIndex(prevLogIndex)

	if prevIndex < 0 {
		reply.XTerm = rf.lastIncludedTerm
		reply.XIndex = rf.lastIncludedIndex
	} else {
		reply.XTerm = rf.logs[prevIndex].Term

		firstIndex := prevIndex
		for firstIndex >= 0 && rf.logs[firstIndex].Term == reply.XTerm {
			firstIndex--
		}
		reply.XIndex = rf.getLogIndex(firstIndex + 1)
	}
}

// Caller must hold the mutex.
func (rf *Raft) hasPrevLog(args *AppendEntriesArgs) bool {
	if rf.getLastLogIndex() < args.PrevLogIndex {
		return false
	}

	prevIndex := rf.getArrayIndex(args.PrevLogIndex)
	if prevIndex < -1 {
    // Already in the snapshot
		return true
	} else if prevIndex == -1 {
		// Should be true since they are supposed to be committed.
		return rf.lastIncludedIndex == args.PrevLogIndex &&
           rf.lastIncludedTerm == args.PrevLogTerm
	} else {
    return rf.logs[prevIndex].Term == args.PrevLogTerm
  }
}

// Caller must hold the mutex.
func (rf *Raft) updateLogs(args *AppendEntriesArgs) {
	prevIndex := rf.getArrayIndex(args.PrevLogIndex)
	i, j := prevIndex+1, 0
  for i < 0 {
    i++
    j++
  }

	for i < len(rf.logs) && j < len(args.Entries) {
		if rf.logs[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}

	if j < len(args.Entries) {
		if i < len(rf.logs) {
			rf.logs = rf.logs[:i]
		}

		rf.logs = append(rf.logs, args.Entries[j:]...)
	}
	PrintDebug("%v AppendEntries result logs size: %v", rf.me, len(rf.logs))

  rf.persist()
}

// Caller must hold the mutex.
func (rf *Raft) updateCommitId(args *AppendEntriesArgs) {
  candidate := args.LeaderCommit

	lastNewLogIndex := args.PrevLogIndex + len(args.Entries)
  if candidate > lastNewLogIndex {
    candidate = lastNewLogIndex
  }

  if rf.commitIndex < candidate {
    rf.commitIndex = candidate
  }
}

func (rf *Raft) AppendEntries(
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = args.Term

	rf.state = Follower
	rf.lastUpdate = time.Now()

  if !rf.hasPrevLog(args) {
		PrintDebug(
      "%v AppendEntries failed: no prev log found. " +
      "prevIndex: %v, prevTerm: %v, rf.logs: %v",
      rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.logs)
		rf.fillConflictTerm(args, reply)
		return
	}

	reply.Success = true

  rf.updateLogs(args)

  rf.updateCommitId(args)
}

func (rf *Raft) commitLog(log Log, logIndex int) {
	msg := ApplyMsg{}
	msg.CommandValid = true
	msg.Command = log.Command
	msg.CommandIndex = logIndex

	rf.applyCh <- msg
  PrintDebugGreen(
    "    %v: commit log: %v done", rf.me, logIndex)
}

// Caller must hold mutex.
func (rf *Raft) updateNextIndex(
  server int,
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) {
	PrintDebug(
    "%v updateNextIndex: XTerm: %v, XIndex: %v, XLen: %v",
    server, reply.XTerm, reply.XIndex, reply.XLen)

	originIndex := rf.nextIndex[server]
	nextIndex := &rf.nextIndex[server]

	// follower's log is too short.
	if reply.XTerm == -1 {
		*nextIndex = reply.XLen
		PrintDebug(
      "%v [too short] original index: %v, update index: %v",
      server, originIndex, rf.nextIndex[server])
    return
	}


  lastArrayIndex := rf.getArrayIndex(args.PrevLogIndex)
  for lastArrayIndex >= 0 && rf.logs[lastArrayIndex].Term > reply.XTerm {
    lastArrayIndex--
  }

  if lastArrayIndex >= 0 && rf.logs[lastArrayIndex].Term == reply.XTerm {
    // leader has XTerm
    *nextIndex = rf.getLogIndex(lastArrayIndex)
  } else {
    // leader doesn't have XTerm
    *nextIndex = reply.XIndex
  }

  PrintDebug(
    "%v [conflict] original index: %v, update index: %v",
    server, originIndex, rf.nextIndex[server])
}

func (rf *Raft) sendAppendEntries(
  server int, 
  args *AppendEntriesArgs,
  reply *AppendEntriesReply,
) bool {
	PrintDebug(
    "start to sendAppendEntries %v -> %v: " + 
    "[term: %v, prevId: %v, prevTerm: %v, commit: %v, log size: %v]",
    rf.me, server, args.Term, args.PrevLogIndex, 
    args.PrevLogTerm, args.LeaderCommit, len(args.Entries))

	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.voteReceived = 0

		rf.persist()
		return ok
	}

	// Fail to receive reply from followers.
	if reply.Term < rf.currentTerm {
		return ok
	}

	if !reply.Success {
		rf.updateNextIndex(server, args, reply)
	} else {
    matchLogIndex := args.PrevLogIndex + len(args.Entries)

    if matchLogIndex > rf.matchIndex[server] {
      rf.matchIndex[server] = matchLogIndex

      PrintDebug(
        "%v -> %v: succeed to append entries: match: %v",
        rf.me, server, rf.matchIndex[server])
      rf.updateCommit()
    }

    nextLogIndex := matchLogIndex + 1
    if nextLogIndex > rf.nextIndex[server] {
      rf.nextIndex[server] = nextLogIndex
    }
	}
	return ok
}

func (rf *Raft) isMajority(targetLogIndex int) bool {
	count := 0

	if targetLogIndex <= rf.getLastLogIndex() {
		count++
	}

	for i, match := range rf.matchIndex {
		if i == rf.me {
			continue
		}

		if targetLogIndex <= match {
			count++
		}
	}

	return count > len(rf.peers) / 2
}

// Caller must hold the mutex.
func (rf *Raft) updateCommit() {
	minMatchLogIndex := rf.commitIndex + 1

	for rf.isMajority(minMatchLogIndex) {
		minMatchLogIndex++
	}

	minMatchLogIndex--
	PrintDebugGreen(
    "%v: updateCommit: matchIndex: %v, logs size: %v, " +
    "commitIndex: %v",
    rf.me, minMatchLogIndex, len(rf.logs), rf.commitIndex)

	if minMatchLogIndex > rf.commitIndex {
		minMatch := rf.getArrayIndex(minMatchLogIndex)

		if rf.logs[minMatch].Term == rf.currentTerm {
			rf.commitIndex = minMatchLogIndex
		}
	}
}

type InstallSnapshotArgs struct {
  Term              int // leader's term
  LeaderId          int
  LastIncludedIndex int
  LastIncludedTerm  int
  Data              []byte
}

type InstallSnapshotReply struct {
  Term int
}

// Caller must hold the mutex.
func (rf *Raft) makeInstallSnapshotArgs() InstallSnapshotArgs {
  args := InstallSnapshotArgs{}
  args.Term = rf.currentTerm
  args.LeaderId = rf.me
  args.LastIncludedIndex = rf.lastIncludedIndex
  args.LastIncludedTerm = rf.lastIncludedTerm
  args.Data = clone(rf.snapshot)
  return args
}

func (rf *Raft) sendInstallSnapshot(
  server int,
  args *InstallSnapshotArgs,
  reply *InstallSnapshotReply,
) bool {
  PrintDebug(
    "Send installSnapshot: %v -> %v, lastId: %v, lastTerm: %v",
  rf.me, server, args.LastIncludedIndex, args.LastIncludedTerm)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.voteReceived = 0

		rf.persist()
	  return ok
	}

  rf.nextIndex[server] = args.LastIncludedIndex + 1
  rf.matchIndex[server] = args.LastIncludedIndex

	return ok
}

func (rf *Raft) InstallSnapshot(
  args *InstallSnapshotArgs,
  reply *InstallSnapshotReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

  PrintDebug(
    "%v InstallSnapshot: lastId: %v, lastTerm: %v",
    rf.me, args.LastIncludedIndex, args.LastIncludedTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

  rf.state = Follower
  rf.lastUpdate = time.Now()

  // If follower's snapshot contains the leader's snapshot
  if rf.lastIncludedIndex >= args.LastIncludedIndex {
    return
  }

  // If follower's log contains the leader's snapshot
  lastLogIndex := rf.getLastLogIndex()
  if lastLogIndex >= args.LastIncludedIndex {
    arrayIndex := rf.getArrayIndex(args.LastIncludedIndex)
    if rf.logs[arrayIndex].Term == args.LastIncludedTerm {
      return
    }
  }

  // Discard the entire logs.
  rf.logs = rf.logs[:0]
  rf.lastIncludedTerm = args.LastIncludedTerm
  rf.lastIncludedIndex = args.LastIncludedIndex
  rf.startIndex = args.LastIncludedIndex + 1
  rf.snapshot = clone(args.Data)

  rf.persist()

  rf.commitIndex = args.LastIncludedIndex

  rf.commitSnapshot(args)
}

func (rf *Raft) commitSnapshot(args *InstallSnapshotArgs) {
  msg := ApplyMsg{}
  msg.SnapshotValid = true
  msg.Snapshot = clone(args.Data)
  msg.SnapshotTerm = args.LastIncludedTerm
  msg.SnapshotIndex = args.LastIncludedIndex

  rf.applyCh <- msg
  PrintDebugGreen(
    "commitSnapshot: lastId: %v, lastTerm: %v done",
    args.LastIncludedIndex, args.LastIncludedTerm)
}

func (rf *Raft) sendUpdates() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == Leader {
		for id := range rf.peers {
			if id == rf.me {
				continue
			}

      if rf.nextIndex[id] >= rf.startIndex {
        args := rf.makeAppendEntriesArgs(id)
        reply := AppendEntriesReply{}
        go rf.sendAppendEntries(id, &args, &reply)
      } else {
        args := rf.makeInstallSnapshotArgs()
        reply := InstallSnapshotReply{}
        go rf.sendInstallSnapshot(id, &args, &reply)
      }
		}
	}
}

func (rf *Raft) heartbeats() {
	for {
		rf.sendUpdates()

		time.Sleep(heartbeatTimeout * time.Millisecond)
	}
}

func (rf *Raft) commit() {
	for {
    rf.mu.Lock()
    commitIndex := rf.commitIndex
    if rf.lastApplied < rf.lastIncludedIndex {
      rf.lastApplied = rf.lastIncludedIndex
    }
    rf.mu.Unlock()


    for rf.lastApplied < commitIndex {
      rf.lastApplied++

      rf.mu.Lock()

      arrayIndex := rf.getArrayIndex(rf.lastApplied)
      log := rf.logs[arrayIndex]

      rf.mu.Unlock()

      rf.commitLog(log, rf.lastApplied)
    }

		time.Sleep(commitInterval * time.Millisecond)
	}
}

// Caller must hold the mutex.
func (rf *Raft) makeAppendEntriesArgs(peer int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.LeaderId = rf.me

	nextLogId := rf.nextIndex[peer]

  if nextLogId <= rf.startIndex {
	// TODO: Install snapshot if previous logs are deleted.
    args.PrevLogIndex = rf.lastIncludedIndex
    args.PrevLogTerm = rf.lastIncludedTerm
  } else {
    args.PrevLogIndex = nextLogId - 1
    prevIndex := rf.getArrayIndex(args.PrevLogIndex)
    args.PrevLogTerm = rf.logs[prevIndex].Term
  }

	args.Entries = []Log{}
	nextId := rf.getArrayIndex(nextLogId)
	for i := nextId; i < len(rf.logs); i++ {
		args.Entries = append(args.Entries, rf.logs[i])
	}

	args.LeaderCommit = rf.commitIndex

	PrintDebug(
    "MakeAppendEntriesArgs server: %v -> %v:\n             " +
    "nextLogId: %v, preLogIndex: %v, prevLogTerm: %v " +
    "entries size: %v, commit: %v",
    rf.me, peer, nextLogId, args.PrevLogIndex,
    args.PrevLogTerm, len(args.Entries), args.LeaderCommit)

	return args
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, /*isLeader=*/false
	}

	newLog := Log{}
	newLog.Term = rf.currentTerm
	newLog.Command = command
	rf.logs = append(rf.logs, newLog)

	rf.persist()

	PrintDebugRed(
    "%v Start command: [term: %v, total logs size: %v]",
    rf.me, newLog.Term, rf.getLastLogIndex())
	go rf.sendUpdates()

	return /*index=*/ rf.getLogIndex(len(rf.logs) - 1),
		/*term=*/ newLog.Term,
		/*isLeader=*/ true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Caller should hold the mutex
func (rf *Raft) needElection() bool {
	if rf.state == Leader {
		return false
	}

	duration := time.Now().Sub(rf.lastUpdate)

	// election timeout for a random amount of time between 200 and 350
	// milliseconds.
	electionTimeout := 200 + (rand.Int63() % 150)
	return duration > time.Duration(electionTimeout)*time.Millisecond
}

func (rf *Raft) checkLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.needElection() {
		rf.state = Candidate
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.voteReceived = 1

		rf.persist()

		for id, _ := range rf.peers {
			if id == rf.me {
				continue
			}

			args := RequestVoteArgs{}
			args.Term = rf.currentTerm
			args.CandidateId = rf.me

			args.LastLogIndex = rf.getLastLogIndex()
			args.LastLogTerm = rf.getLastLogTerm()

			reply := RequestVoteReply{}
			go rf.sendRequestVote(id, &args, &reply)
		}
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.checkLeader()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 250)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// Avoid start up competition
	rf.lastUpdate = time.Now()

	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
  rf.snapshot = clone(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.heartbeats()
  go rf.commit()

	return rf
}
