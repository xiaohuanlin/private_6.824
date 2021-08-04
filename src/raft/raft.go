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
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Term 	int
	Index 	int
	Message [] byte
}

const (
	Follower int32 = iota + 1
	Candidate
	Leader
)

const (
	InvalidVote = -1
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	currentTerm int
	state		int32

	voteFor 	int
	voteTimer 	Timer
	voteCount 	int32

	appendTimer	Timer

	log			[]Log

	commitIndex int
	lastApplied int

	nextIndex 	[]int
	matchIndex 	[]int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	return rf.currentTerm, atomic.LoadInt32(&rf.state) == Leader
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



/*
=======================================
REQUEST VOTE
=======================================
 */

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

func (ra *RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs(term:%d candidateId:%d lastLogIndex:%d lastLogTerm:%d)", ra.Term, ra.CandidateId, ra.LastLogIndex, ra.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Raft %v(term:%v) receive RequestVote %v", rf.me, rf.currentTerm, args)
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.TurnToFollower()
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		log.Printf("Raft %v(term:%v) receive RequestVote %v", rf.me, rf.currentTerm, args)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	if rf.voteFor == InvalidVote && args.LastLogIndex >= rf.commitIndex {
		rf.voteFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		log.Printf("Raft %v(term:%v) accept RequestVote %v", rf.me, rf.currentTerm, args)
		return
	}
	reply.VoteGranted = false
	log.Printf("Raft %v(term:%v) reject RequestVote %v", rf.me, rf.currentTerm, args)
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
	log.Printf("Raft %v send RequestVote to %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


/*
=======================================
APPEND ENTRIES
=======================================
*/
type AppendEntriesArgs struct {
	Term 			int
	LeaderId 		int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries			[]Log
	LeaderCommit	int
}

func (aa *AppendEntriesArgs) String() string {
	return fmt.Sprintf("AppendEntriesArgs(term:%d leader:%d preLogIndex:%d preLogTerm:%d)", aa.Term, aa.LeaderId, aa.PrevLogIndex, aa.PrevLogTerm)
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	log.Printf("Raft %v(term:%v) receive AppendEntries from %v", rf.me, rf.currentTerm, args)
	rf.voteTimer.Reset()

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
	// If AppendEntries RPC received from new leader: convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.TurnToFollower()
	}

	if atomic.LoadInt32(&rf.state) == Candidate {
		rf.currentTerm = args.Term
		rf.TurnToFollower()
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		log.Printf("Raft %v(term:%v) refuse AppendEntries from %v", rf.me, rf.currentTerm, args)
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[len(rf.log) - 1].Index < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		log.Printf("Raft %v(term:%v, log_term:%v log_index:%v) refuse AppendEntries from %v",
			rf.me, rf.currentTerm, rf.log[args.PrevLogIndex].Term, rf.log[len(rf.log) - 1].Index, args)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	remainEntries := args.Entries
	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		if rf.log[index].Term != args.Entries[i].Term {
			// Remove the wrong log
			rf.log = rf.log[:index]
			// Update remaining entries
			remainEntries = args.Entries[i:]
			break
		}
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, remainEntries...)
	reply.Success = true
	// Reset vote timer
	rf.voteTimer.Reset()

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := rf.log[len(rf.log) - 1].Index
		if args.LeaderCommit < lastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}
	}

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	if rf.commitIndex > rf.lastApplied {
		rf.lastApplied = rf.commitIndex
		// todo: apply it to state machine
	}

	log.Printf("Raft %v(term:%v) accept AppendEntries from %v", rf.me, rf.currentTerm, args)
}


// Send AppendEntry request to other peer
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Upon election: send initial empty AppendEntries RPCs
// (heartbeat) to each server; repeat during idle periods to
// prevent election timeouts
func (rf *Raft) heartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := AppendEntriesArgs {rf.currentTerm, rf.me, 0, 0, make([]Log, 0), rf.commitIndex}
		reply := AppendEntriesReply {}
		rf.sendAppendEntries(i, &args, &reply)
	}
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

func (rf *Raft) startElection() {
	log.Printf("Raft %v start election", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Increase term
	rf.currentTerm++
	// Vote for itself
	rf.voteFor = rf.me
	atomic.AddInt32(&rf.voteCount, 1)
	// Reset election timer
	rf.voteTimer.Reset()

	log.Printf("Raft %v enter loop", rf.me)
	lastLog := rf.log[len(rf.log) - 1]
	args := RequestVoteArgs {rf.currentTerm, rf.me, lastLog.Index, lastLog.Term}

	// Send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func (iter int, term int) {
			reply := RequestVoteReply {}
			rf.sendRequestVote(iter, &args, &reply)

			// If votes received from majority of servers: become leader
			if reply.VoteGranted && reply.Term == term && term == rf.currentTerm {
				// majority peers agree it become a leader
				if atomic.AddInt32(&rf.voteCount, 1)  > int32(len(rf.peers) / 2) {
					rf.TurnToLeader()
				}
			}
		}(i, rf.currentTerm)
	}
	log.Printf("Raft %v send vote done", rf.me)
}

func (rf *Raft) TurnToCandidate() {
	if atomic.LoadInt32(&rf.state) == Candidate {
		return
	}
	atomic.StoreInt32(&rf.state, Candidate)

	log.Printf("Raft %v(term:%v) turn to candidate", rf.me, rf.currentTerm)

	if rf.appendTimer.active {
		rf.appendTimer.Cancel()
	}

	rf.startElection()
}

func (rf *Raft) TurnToLeader() {
	if atomic.LoadInt32(&rf.state) == Leader {
		return
	}
	atomic.StoreInt32(&rf.state, Leader)

	log.Printf("Raft %v(term:%v) turn to leader", rf.me, rf.currentTerm)

	rf.voteTimer.Cancel()
	rf.appendTimer.Start(true)
}

func (rf *Raft) TurnToFollower() {
	log.Printf("Raft %v(term:%v) turn to follower", rf.me, rf.currentTerm)
	atomic.StoreInt32(&rf.state, Follower)

	rf.InitVoter()

	if rf.appendTimer.active {
		rf.appendTimer.Cancel()
	}
}

/*
it is a helper function to init voter timer
 */
func (rf *Raft) InitVoter() {
	rf.voteFor = InvalidVote
	rf.voteCount = 0

	if rf.voteTimer.active {
		rf.voteTimer.Reset()
	} else {
		rf.voteTimer.Start(false)
	}
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

	log.Printf("Raft %v start election", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.state = Follower

	rf.voteTimer = Timer {sync.Mutex{}, make(chan int), time.Duration(rand.Intn(400) + 400) * time.Millisecond, rf.TurnToCandidate, false}
	rf.InitVoter()

	rf.appendTimer = Timer {sync.Mutex{}, make(chan int), time.Duration(100 * time.Millisecond), rf.heartBeat, false}

	rf.log = make([]Log, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
