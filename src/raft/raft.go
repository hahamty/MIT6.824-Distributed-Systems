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
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// Using GOB needs capitalized
type Log struct {
	Term    int
	Command interface{}
}

type raftStateType uint8

const (
	FOLLWER raftStateType = iota
	CANDIDATE
	LEADER
)

const (
	HeartbeatEvent uint8 = iota
	BecomeLeaderEvent
	BroadcastSentEvent
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	raftState raftStateType

	// Persistent state on all servers
	currentTerm int
	voteFor     int
	logs        []Log

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// Election
	votes     int
	eventChan chan uint8

	// Applying to service
	applyCh chan ApplyMsg
}

func (rf *Raft) Loop() {
	for {
		switch rf.raftState {
		case LEADER:
			select {
			case <-rf.eventChan:
			case <-time.After(50 * time.Millisecond):
				rf.mu.Lock()
				term := 0
				if rf.raftState == LEADER {
					term = rf.currentTerm
				}
				rf.mu.Unlock()
				rf.BroadcastAppendEntries(term)
			}
		case CANDIDATE:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(rand.Int63n(50)+100) * time.Millisecond):
				rf.mu.Lock()
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.votes = 1
				term := 0
				if rf.raftState == CANDIDATE {
					term = rf.currentTerm
				}
				rf.mu.Unlock()
				rf.BroadcastRequestVote(term)
			}
		case FOLLWER:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(rand.Int63n(150)+150) * time.Millisecond):
				rf.mu.Lock()
				rf.raftState = CANDIDATE
				rf.currentTerm++
				rf.voteFor = rf.me
				rf.votes = 1
				term := 0
				if rf.raftState == FOLLWER {
					term = rf.currentTerm
				}
				rf.mu.Unlock()
				rf.BroadcastRequestVote(term)
			}
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.raftState == LEADER
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
// Using GOB requires capitalized
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.raftState = FOLLWER
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	lastIndex := len(rf.logs) - 1
	if args.LastLogTerm < rf.logs[lastIndex].Term {
		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex < rf.commitIndex {
		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex <= lastIndex && rf.logs[lastIndex].Term > args.LastLogTerm {
		reply.Term = args.Term
		reply.VoteGranted = false
		return
	}
	go func() {
		rf.eventChan <- HeartbeatEvent
	}()
	reply.Term = args.Term
	if rf.voteFor == -1 {
		rf.voteFor = args.CandidateID
	}
	if rf.voteFor == args.CandidateID {
		reply.VoteGranted = true
		return
	}
	reply.VoteGranted = false
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.VoteGranted {
			rf.votes++
			if rf.votes*2 > len(rf.peers) && rf.raftState == CANDIDATE {
				rf.raftState = LEADER
				for i := 0; i < len(rf.peers); i++ {
					rf.nextIndex[i] = len(rf.logs)
					rf.matchIndex[i] = 0
				}
				go rf.BroadcastAppendEntries(rf.currentTerm)
				go func() {
					rf.eventChan <- BecomeLeaderEvent
				}()
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.raftState = FOLLWER
				rf.currentTerm = reply.Term
				rf.voteFor = -1
				go func() {
					rf.eventChan <- HeartbeatEvent
				}()
			}
		}
	}
	return ok
}

// Do not use lock, or will be deadlocked
func (rf *Raft) BroadcastRequestVote(term int) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			lastLogIndex := len(rf.logs) - 1
			args := RequestVoteArgs{term, rf.me, lastLogIndex, rf.logs[lastLogIndex].Term}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.raftState = FOLLWER
		rf.currentTerm = args.Term
		rf.voteFor = -1
	}
	go func() {
		rf.eventChan <- HeartbeatEvent
	}()
	reply.Term = args.Term
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	index := 0
	conflictIndex := -1
	for ; index < len(args.Entries); index++ {
		if len(rf.logs) <= args.PrevLogIndex+1+index {
			break
		} else {
			if rf.logs[args.PrevLogIndex+1+index].Term != args.Entries[index].Term {
				conflictIndex = args.PrevLogIndex + 1 + index
				break
			}
		}
	}
	if conflictIndex != -1 {
		rf.logs = rf.logs[:conflictIndex]
	}
	for ; index < len(args.Entries); index++ {
		rf.logs = append(rf.logs, args.Entries[index])
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		go rf.CommitUpdate()
	}
	reply.Success = true
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Success {
			if rf.nextIndex[server] < args.PrevLogIndex+1+len(args.Entries) {
				rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
			}
			if rf.matchIndex[server] < rf.nextIndex[server]-1 {
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			}
			index := rf.matchIndex[server]
			if index > rf.commitIndex {
				count := 1
				for i := 0; i < len(rf.peers); i++ {
					if rf.me != i && rf.matchIndex[i] >= index {
						count++
					}
				}
				if count*2 > len(rf.peers) {
					rf.commitIndex = index
					go rf.CommitUpdate()
				}
			}
		} else {
			if reply.Term > rf.currentTerm {
				rf.raftState = FOLLWER
				rf.currentTerm = reply.Term
				rf.voteFor = -1
			} else {
				if args.PrevLogIndex >= rf.matchIndex[server] && rf.raftState == LEADER {
					rf.nextIndex[server] = args.PrevLogIndex
				}
			}
		}
	}
	return ok
}

// Do not use lock, or will be deadlocked
func (rf *Raft) BroadcastAppendEntries(term int) {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]
			args := AppendEntriesArgs{term, rf.me, nextIndex - 1, rf.logs[nextIndex-1].Term, rf.logs[nextIndex:], rf.commitIndex}
			reply := &AppendEntriesReply{}
			go rf.SendAppendEntries(i, args, reply)
		}
	}
	go func() {
		rf.eventChan <- BroadcastSentEvent
	}()
}

func (rf *Raft) CommitUpdate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.commitIndex {
		applyMessage := ApplyMsg{}
		applyMessage.Index = rf.lastApplied + 1
		applyMessage.Command = rf.logs[rf.lastApplied+1].Command
		rf.applyCh <- applyMessage
		rf.lastApplied++
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isLeader = rf.raftState == LEADER
	if rf.raftState == LEADER {
		index = len(rf.logs)
		rf.logs = append(rf.logs, Log{rf.currentTerm, command})
		go rf.BroadcastAppendEntries(rf.currentTerm)
	}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	// rf.commitIndex = -1
	rf.raftState = FOLLWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.logs = make([]Log, 1)
	rf.logs[0] = Log{0, 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.eventChan = make(chan uint8)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Loop()

	return rf
}
