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

	"../labgob"
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
	Message  interface{}
}

const (
	Follower int32 = iota + 1
	Candidate
	Leader
)

const InvalidVote = -1

const VoterTimeoutBase = 400
const VoterTimeoutDelta = 400
const AppendTimeoutBase = 100

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh		chan ApplyMsg
	currentTerm int
	state		int32

	voteFor 	int
	voteTimer 	Timer

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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.state)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var state int32
	var log []Log
	if 	d.Decode(&currentTerm) != nil ||
	   	d.Decode(&voteFor) != nil ||
		d.Decode(&state) != nil ||
		d.Decode(&log) != nil {
		//error
		DPrintf("decode error")
	} else {
		rf.currentTerm = currentTerm
	  	rf.voteFor = voteFor
	  	rf.state = state
	  	rf.log = log
	}
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
	DPrintf("Raft %v(term:%v) receive RequestVote %v", rf.me, rf.currentTerm, args)
	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
		rf.TurnToFollower()
	}

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("Raft %v(term:%v) reject RequestVote %v", rf.me, rf.currentTerm, args)
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote
	lastLog := rf.log[len(rf.log) - 1]
	if rf.voteFor == InvalidVote && (
			args.LastLogTerm > lastLog.Term ||
			(args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)) {
		rf.voteFor = args.CandidateId
		rf.persist()
		reply.Term = args.Term
		reply.VoteGranted = true
		DPrintf("Raft %v(term:%v) accept RequestVote %v", rf.me, rf.currentTerm, args)
		return
	}
	reply.Term = args.Term
	reply.VoteGranted = false
	DPrintf("Raft %v(term:%v) reject RequestVote %v", rf.me, rf.currentTerm, args)
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
	DPrintf("Raft %v send RequestVote to %v", rf.me, server)
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
	return fmt.Sprintf("AppendEntriesArgs(term:%d leader:%d preLogIndex:%d preLogTerm:%d, entriesLen: %v)", aa.Term, aa.LeaderId, aa.PrevLogIndex, aa.PrevLogTerm, len(aa.Entries))
}

type AppendEntriesReply struct {
	Term int
	Success bool
}


// AppendEntries handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft %v(term:%v) receive AppendEntries from %v", rf.me, rf.currentTerm, args)

	// If RPC request or response contains term T > currentTerm: set currentTerm = T, convert to follower 
	// If AppendEntries RPC received from new leader: convert to follower
	if args.Term > rf.currentTerm || atomic.LoadInt32(&rf.state) == Candidate {
		rf.currentTerm = args.Term
		rf.persist()
		rf.TurnToFollower()
	}

	reply.Term = rf.currentTerm
	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		DPrintf("Raft %v(term:%v) refuse AppendEntries from %v for term mismatch", rf.me, rf.currentTerm, args)
		return
	}

	// Reset vote timer
	// it will avoid becoming candidater when fixing log inconsistence problem
	rf.voteTimer.Reset(time.Duration(rand.Intn(VoterTimeoutBase) + VoterTimeoutDelta) * time.Millisecond)

	// Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm
	if rf.log[len(rf.log) - 1].Index < args.PrevLogIndex ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		DPrintf("Raft %v(term:%v) refuse AppendEntries from %v for log mismatch",
			rf.me, rf.currentTerm, args)
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
	var remainEntries []Log
	for i := 0; i < len(args.Entries); i++ {
		index := args.Entries[i].Index
		if index == len(rf.log) {
			// Update remaining entries
			remainEntries = args.Entries[i:]
			break
		}
		if rf.log[index].Term != args.Entries[i].Term {
			// Remove the wrong log
			DPrintf("Raft %v(term:%v) remove log from %v", rf.me, rf.currentTerm, index)
			rf.log = rf.log[:index]
			// Update remaining entries
			remainEntries = args.Entries[i:]
			break
		}
	}

	// Append any new entries not already in the log
	rf.log = append(rf.log, remainEntries...)
	reply.Success = true

	DPrintf("Raft %v(term:%v) log len %v last index %v", rf.me, rf.currentTerm, len(rf.log), rf.log[len(rf.log) - 1].Index)
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastEntryIndex := rf.log[len(rf.log) - 1].Index
		if args.LeaderCommit < lastEntryIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntryIndex
		}
	}
	DPrintf("Raft %v(term:%v) commit index %v leaderCommit %v", rf.me, rf.currentTerm, rf.commitIndex, args.LeaderCommit)

	// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine
	if rf.commitIndex > rf.lastApplied {
		DPrintf("Raft %v(term:%v) apply AppendEntries from %v to %v", rf.me, rf.currentTerm, rf.lastApplied + 1, rf.commitIndex)
		for _, m := range rf.log[rf.lastApplied + 1:rf.commitIndex + 1] {
			DPrintf("Raft %v(term:%v) send apply msg %v", rf.me, rf.currentTerm, m)
			rf.applyCh <- ApplyMsg {true,m.Message, m.Index}
		}
		rf.lastApplied = rf.commitIndex
	}
	rf.persist()

	DPrintf("Raft %v(term:%v) accept AppendEntries from %v", rf.me, rf.currentTerm, args)
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
	rf.Start(nil)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Raft %v(term:%v) start command %v", rf.me, rf.currentTerm, command)
	isLeader := atomic.LoadInt32(&rf.state) == Leader
	if !isLeader {
		return rf.commitIndex, rf.currentTerm, false
	}

	// If command received from client: append entry to local log
	var commandLog Log
	if command == nil {
		// empty log
		DPrintf("Raft %v(term:%v) send empty log", rf.me, rf.currentTerm)
		commandLog = rf.log[0]
	} else {
		DPrintf("Raft %v(term:%v) append local log: index %v", rf.me, rf.currentTerm, len(rf.log))
		commandLog = Log {rf.currentTerm, len(rf.log), command}
		rf.log = append(rf.log, commandLog)
		rf.persist()
	}
	executed := false
	var commitCount int32 = 1

	// send append entries
	for i := 0; !rf.killed() && i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func (peer int) {
			DPrintf("Raft %v(term:%v) send log to %v", rf.me, rf.currentTerm, peer)
			for ;!rf.killed() && atomic.LoadInt32(&rf.state) == Leader && rf.nextIndex[peer] > 0; {
				prevLog := rf.log[rf.nextIndex[peer] - 1]
				entries := rf.log[rf.nextIndex[peer]:]
				args := AppendEntriesArgs {
					rf.currentTerm,
					rf.me,
					prevLog.Index,
					prevLog.Term,
					entries,
					rf.commitIndex,
				}
				reply := AppendEntriesReply {}
				ok := rf.sendAppendEntries(peer, &args, &reply)
				if !ok {
					continue
				}

				if atomic.LoadInt32(&rf.state) != Leader {
					return
				}

				if reply.Success {
					// If successful: update nextIndex and matchIndex for follower
					DPrintf("Raft %v(term:%v) send log %v to %v success", rf.me, rf.currentTerm, commandLog.Index, peer)
					rf.nextIndex[peer] = prevLog.Index + len(entries) + 1
					rf.matchIndex[peer] = prevLog.Index + len(entries)

					// A log entry is committed once the leader that created the entry
					// has replicated it on a majority of the server
					if atomic.AddInt32(&commitCount, 1)  > int32(len(rf.peers) / 2) && !executed {
						rf.mu.Lock()
						// Raft never commits log entries from previous terms by counting replicas
						if len(entries) > 0 && entries[len(entries) - 1].Term < rf.currentTerm && atomic.LoadInt32(&commitCount) != int32(len(rf.peers)) {
							rf.mu.Unlock()
							return
						}
						DPrintf("Raft %v(term:%v) commit log %v", rf.me, rf.currentTerm, commandLog.Index)
						executed = true
						if commandLog.Index > rf.commitIndex {
							DPrintf("Raft %v(term:%v) change commit id to %v", rf.me, rf.currentTerm, commandLog.Index)
							rf.commitIndex = commandLog.Index
						}

						if rf.commitIndex > rf.lastApplied {
							for _, m := range rf.log[rf.lastApplied + 1: rf.commitIndex + 1] {
								DPrintf("Raft %v(term:%v) send apply msg %v", rf.me, rf.currentTerm, m.Index)
								rf.applyCh <- ApplyMsg {true, m.Message, m.Index}
							}
							rf.lastApplied = rf.commitIndex
							rf.persist()
						}
						rf.mu.Unlock()
					}
					return
				} else if reply.Term > rf.currentTerm {
					// it is not a leader again, so turn it to follower
					DPrintf("Raft %v(term:%v) send log to %v fail", rf.me, rf.currentTerm, peer)
					rf.currentTerm = reply.Term
					rf.TurnToFollower()
					return
				} else {
					//  If AppendEntries fails because of log inconsistency: decrement nextIndex and retry
					DPrintf("Raft %v(term:%v) send log to %v retry", rf.me, rf.currentTerm, peer)
					rf.nextIndex[peer]--
				}
			}
		}(i)
	}
	return commandLog.Index, rf.currentTerm, isLeader
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

	DPrintf("Raft %v has been killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	DPrintf("Raft %v start election", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Increase term
	rf.currentTerm++
	// Vote for itself
	rf.voteFor = rf.me
	rf.persist()
	// Reset election timer
	rf.voteTimer.Reset(time.Duration(rand.Intn(VoterTimeoutBase) + VoterTimeoutDelta) * time.Millisecond)

	DPrintf("Raft %v enter loop", rf.me)
	lastLog := rf.log[len(rf.log) - 1]
	args := RequestVoteArgs {rf.currentTerm, rf.me, lastLog.Index, lastLog.Term}

	var voteCount int32 = 1
	// Send RequestVote RPCs to all other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		go func (iter int, term int) {
			reply := RequestVoteReply {}

			ok := rf.sendRequestVote(iter, &args, &reply)
			if !ok {
				return
			}

			// If votes received from majority of servers: become leader
			if reply.VoteGranted {
				if reply.Term == term && term == rf.currentTerm {
					// majority peers agree it become a leader
					if atomic.AddInt32(&voteCount, 1)  > int32(len(rf.peers) / 2) {
						rf.TurnToLeader()
					}
				}
			} else {
				// update current term
				rf.currentTerm = reply.Term
				rf.persist()
			}

		}(i, rf.currentTerm)
	}
	DPrintf("Raft %v send vote done", rf.me)
}

func (rf *Raft) TurnToCandidate() {
	DPrintf("Raft %v(term:%v) turn to candidate", rf.me, rf.currentTerm)

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

	DPrintf("Raft %v(term:%v) turn to leader", rf.me, rf.currentTerm)

	rf.voteTimer.Cancel()
	rf.appendTimer.Start(true)

	// initial nextIndex and matchIndex
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log) - 1].Index + 1
		DPrintf("Raft %v(term:%v) set %v nextIndex: %v", rf.me, rf.currentTerm, i, rf.nextIndex[i])
		rf.matchIndex[i] = 0
	}
}

func (rf *Raft) TurnToFollower() {
	DPrintf("Raft %v(term:%v) turn to follower", rf.me, rf.currentTerm)
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
	rf.persist()

	if rf.voteTimer.active {
		rf.voteTimer.Reset(time.Duration(rand.Intn(VoterTimeoutBase) + VoterTimeoutDelta) * time.Millisecond)
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

	DPrintf("Raft %v start election", me)
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.log) == 0 {
		// initial state
		rf.log = make([]Log, 1)
	}

	rf.me = me
	rf.applyCh = applyCh

	rf.voteTimer = Timer {sync.Mutex{}, make(chan int), time.Duration(rand.Intn(VoterTimeoutBase) + VoterTimeoutDelta) * time.Millisecond, rf.TurnToCandidate, false}
	rf.voteTimer.Start(false)

	rf.appendTimer = Timer {sync.Mutex{}, make(chan int), time.Duration(AppendTimeoutBase * time.Millisecond), rf.heartBeat, false}
	if atomic.LoadInt32(&rf.state) == Leader {
		rf.appendTimer.Start(false)
	}

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	return rf
}
