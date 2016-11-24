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
	"encoding/gob"
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
	FOLLOWER raftStateType = iota
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
	CurrentTerm int
	VoteFor     int
	Logs        []Log

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
				rf.BroadcastAppendEntries()
			}
		case FOLLOWER, CANDIDATE:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(rand.Int63n(150)+150) * time.Millisecond):
				rf.mu.Lock()
				rf.raftState = CANDIDATE
				rf.CurrentTerm++
				rf.VoteFor = rf.me
				rf.votes = 1
				rf.persist()
				rf.mu.Unlock()
				rf.BroadcastRequestVote()
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
	term = rf.CurrentTerm
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
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VoteFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.CurrentTerm)
	d.Decode(&rf.VoteFor)
	d.Decode(&rf.Logs)
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
	reply.VoteGranted = false
	rf.mu.Lock()
	defer rf.persist()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	if args.Term > rf.CurrentTerm {
		rf.raftState = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
	}
	reply.Term = rf.CurrentTerm
	lastIndex := len(rf.Logs) - 1
	if args.LastLogTerm < rf.Logs[lastIndex].Term {
		return
	}
	if args.LastLogTerm == rf.Logs[lastIndex].Term && args.LastLogIndex < lastIndex {
		return
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateID {
		rf.VoteFor = args.CandidateID
		reply.VoteGranted = true
		rf.raftState = FOLLOWER
		go func() {
			rf.eventChan <- HeartbeatEvent
		}()
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
			if rf.CurrentTerm == args.Term && rf.raftState == CANDIDATE {
				rf.votes++
				if rf.votes*2 > len(rf.peers) {
					rf.raftState = LEADER
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = len(rf.Logs)
						rf.matchIndex[i] = 0
					}
					go rf.BroadcastAppendEntries()
				}
			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.raftState = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
				rf.persist()
				go func() {
					rf.eventChan <- HeartbeatEvent
				}()
			}
		}
	}
	return ok
}

// Do not use lock, or will be deadlocked
func (rf *Raft) BroadcastRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftState == CANDIDATE {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				lastLogIndex := len(rf.Logs) - 1
				args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, rf.Logs[lastLogIndex].Term}
				reply := &RequestVoteReply{}
				go rf.sendRequestVote(i, args, reply)
			}
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
	Term      int
	NextIndex int
	Success   bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Success = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		return
	}
	rf.raftState = FOLLOWER
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.persist()
	}
	go func() {
		rf.eventChan <- HeartbeatEvent
	}()
	reply.Term = args.Term
	if len(rf.Logs) <= args.PrevLogIndex {
		reply.NextIndex = len(rf.Logs)
		return
	}
	if rf.Logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.Logs[args.PrevLogIndex].Term != rf.Logs[i].Term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}
	index := 0
	conflictIndex := -1
	for ; index < len(args.Entries); index++ {
		if len(rf.Logs) <= args.PrevLogIndex+1+index {
			break
		} else {
			if rf.Logs[args.PrevLogIndex+1+index].Term != args.Entries[index].Term {
				conflictIndex = args.PrevLogIndex + 1 + index
				break
			}
		}
	}
	if conflictIndex != -1 {
		rf.Logs = rf.Logs[:conflictIndex]
	}
	for ; index < len(args.Entries); index++ {
		rf.Logs = append(rf.Logs, args.Entries[index])
	}
	rf.persist()
	reply.NextIndex = len(rf.Logs)
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
			if index > rf.commitIndex && rf.Logs[index].Term == rf.CurrentTerm {
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
			if reply.Term > rf.CurrentTerm {
				rf.raftState = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
				rf.persist()
			} else {
				if reply.NextIndex <= rf.matchIndex[server] {
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				} else {
					rf.nextIndex[server] = reply.NextIndex
				}
			}
		}
	}
	return ok
}

// Do not use lock, or will be deadlocked
func (rf *Raft) BroadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.raftState == LEADER {
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				nextIndex := rf.nextIndex[i]
				args := AppendEntriesArgs{rf.CurrentTerm, rf.me, nextIndex - 1, rf.Logs[nextIndex-1].Term, rf.Logs[nextIndex:], rf.commitIndex}
				reply := &AppendEntriesReply{}
				go rf.SendAppendEntries(i, args, reply)
			}
		}
		go func() {
			rf.eventChan <- BroadcastSentEvent
		}()
	}
}

func (rf *Raft) CommitUpdate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for rf.lastApplied < rf.commitIndex {
		applyMessage := ApplyMsg{}
		applyMessage.Index = rf.lastApplied + 1
		applyMessage.Command = rf.Logs[rf.lastApplied+1].Command
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
	term = rf.CurrentTerm
	isLeader = rf.raftState == LEADER
	if rf.raftState == LEADER {
		index = len(rf.Logs)
		rf.Logs = append(rf.Logs, Log{rf.CurrentTerm, command})
		rf.persist()
		go rf.BroadcastAppendEntries()
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
	rf.raftState = FOLLOWER
	rf.CurrentTerm = 0
	rf.VoteFor = -1
	rf.Logs = make([]Log, 1)
	rf.Logs[0] = Log{Term: 0}
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
