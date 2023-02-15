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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

const (
	HeartbeatInterval time.Duration = 200 * time.Millisecond
	TimeoutMin                      = 2 * HeartbeatInterval
	TimeoutMax                      = TimeoutMin + 200*time.Millisecond
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	rwmu      sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    *int // candidateId that received vote in current term (or null if none)
	s           State

	resetTicker    chan struct{}
	pauseTicker    chan struct{}
	continueTicker *sync.Cond

	pauseHeartbeat    chan struct{}
	continueHeartbeat *sync.Cond
}

type State int

const (
	Follower State = 1 << iota
	Candidate
	Leader
)

func (s State) to(t State) int { return int(s | t<<3) }

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

func (rf *Raft) isLeader() bool {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	return rf.s == Leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.rwmu.RLock()
	defer rf.rwmu.RUnlock()
	return rf.currentTerm, rf.isLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

type RequestVoteArgs struct {
	Term        int // candidate requesting vote
	CandidateId int // candidate requesting vote
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.log(dVote, "<- S%d RequestVote(args: %+v, ...)", args.CandidateId, args)
	defer rf.log(dVote, "<- S%d RequestVote(..., reply: %+v)", args.CandidateId, reply)

	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
		rf.convertTo(Follower)
	}

	// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
	if rf.votedFor == nil || *rf.votedFor == args.CandidateId {
		reply.VoteGranted = true
		rf.votedFor = &args.CandidateId
		return
	}
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rf.log(dVote, "sendRequestVote(S%d, args: %+v, ...)", server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	defer rf.log(dVote, "sendRequestVote(S%d, ..., reply: %+v) -> %v", server, reply, ok)
	return ok
}

type AppendEntriesArgs struct {
	Term     int // leader’s term
	LeaderId int // so follower can redirect clients
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.log(dLog, "<- S%d AppendEntries(args: %+v, ...)", args.LeaderId, args)
	defer rf.log(dLog, "<- S%d AppendEntries(..., reply: %+v)", args.LeaderId, reply)

	rf.rwmu.Lock()
	defer rf.rwmu.Unlock()

	reply.Term = rf.currentTerm

	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.convertTo(Follower)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = nil
	}

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm

	reply.Success = true

	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it

	// 4. Append any new entries not already in the log

	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	rf.log(dLeader, "sendAppendEntries(S%d, args: %+v, ...)", server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	defer rf.log(dLeader, "sendAppendEntries(S%d, ..., %+v) -> %v", server, reply, ok)
	return ok
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		select {
		case <-time.After(time.Duration(rand.Int63n(int64(TimeoutMax-TimeoutMin))) + TimeoutMin):
			rf.log(dTimer, "ticker timeout")
			rf.rwmu.Lock()
			rf.convertTo(Candidate)
			rf.rwmu.Unlock()
		case <-rf.resetTicker:
			rf.log(dTimer, "ticker reset")
			continue
		case <-rf.pauseTicker:
			rf.log(dTimer, "ticker pause")
			rf.continueTicker.L.Lock()
			for rf.isLeader() {
				rf.continueTicker.Wait()
			}
			rf.continueTicker.L.Unlock()
			rf.log(dTimer, "ticker continue")
		}
	}
}

// The heartbeat go routine
func (rf *Raft) heartbeat() {
	rf.continueHeartbeat.L.Lock()
	for !rf.isLeader() {
		rf.continueHeartbeat.Wait()
	}
	rf.continueHeartbeat.L.Unlock()
	rf.log(dTimer, "heartbeat start")

	for !rf.killed() {
		rf.heartbeatOnce()
		select {
		case <-time.After(HeartbeatInterval):
		case <-rf.pauseHeartbeat:
			rf.log(dTimer, "heartbeat pause")
			rf.continueHeartbeat.L.Lock()
			for !rf.isLeader() {
				rf.continueHeartbeat.Wait()
			}
			rf.continueHeartbeat.L.Unlock()
			rf.log(dTimer, "heartbeat continue")
		}
	}
}

// heartbeatOnce starts go rountines to send empty AppendEntries to each server
func (rf *Raft) heartbeatOnce() {
	rf.log(dTimer, "heartbeat")
	rf.rwmu.RLock()
	term := rf.currentTerm
	rf.rwmu.RUnlock()
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go rf.sendAppendEntries(server, &AppendEntriesArgs{Term: term, LeaderId: rf.me}, &AppendEntriesReply{})
	}
}

// On conversion to candidate, start election:
//
// - Increment currentTerm
//
// - Vote for self
//
// - Reset election timer
//
// - Send RequestVote RPCs to all other servers
//
// caller must NOT hold the write lock
func (rf *Raft) election() {
	rf.log(dTrace, "start election")

	// 1. Increment currentTerm
	rf.rwmu.Lock()
	rf.currentTerm += 1
	term := rf.currentTerm
	rf.rwmu.Unlock()
	rf.log(dTrace, "increment currentTerm to %d", term)

	// 2. Vote for self
	var nVote uint32 = 1

	// 3. Reset election timer
	rf.resetTicker <- struct{}{}

	// 4. Send RequestVote RPCs to all other servers
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:        term,
				CandidateId: rf.me,
			}
			reply := &RequestVoteReply{}
			if !rf.sendRequestVote(server, args, reply) {
				return
			}
			// TODO: If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
			if reply.VoteGranted {
				rf.log(dVote, "<- S%d vote", server)
				atomic.AddUint32(&nVote, 1)
			}
		}(server)
	}

	time.Sleep(TimeoutMin - 200*time.Millisecond) // disconnected server might be too late

	n := int(atomic.LoadUint32(&nVote))
	rf.log(dVote, "gets %d votes", n)
	if 2*n >= len(rf.peers) {
		rf.rwmu.Lock()
		rf.convertTo(Leader)
		rf.rwmu.Unlock()
	}
}

// caller must hold the writer lock
func (rf *Raft) convertTo(t State) {
	rf.log(dLog, "convert from %v to %v", rf.s, t)

	switch rf.s.to(t) {
	case Follower.to(Follower):
		// NOT in Figure 4, use to reset timer
		rf.resetTicker <- struct{}{}
	case Follower.to(Candidate):
		// (Figure 4) times out, start election
		go rf.election()
	case Candidate.to(Follower):
		// (Figure 4) discovers current leader or new term
		rf.resetTicker <- struct{}{}
	case Candidate.to(Candidate):
		// (Figure 4) times out, new election
		go rf.election()
	case Candidate.to(Leader):
		// (Figure 4) receives votes from majority of servers
		rf.pauseTicker <- struct{}{}
		rf.continueHeartbeat.Broadcast()
	case Leader.to(Follower):
		// (Figure 4) discovers server with higher term
		rf.pauseHeartbeat <- struct{}{}
		rf.continueTicker.Broadcast()
	default:
		rf.log(dError, "unexpected convert from %v to %v", rf.s, t)
	}
	rf.s = t
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.log(dLog, "make and init server")

	rf.currentTerm = 0
	rf.s = Follower
	rf.resetTicker = make(chan struct{})
	rf.pauseTicker = make(chan struct{})
	rf.continueTicker = sync.NewCond(&sync.Mutex{})
	rf.pauseHeartbeat = make(chan struct{})
	rf.continueHeartbeat = sync.NewCond(&sync.Mutex{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeat()

	return rf
}

func (rf *Raft) log(topic logTopic, format string, a ...interface{}) {
	DPrintf(topic, rf.me, format, a...)
}
