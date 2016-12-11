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
	Index   int
	Command interface{}
}

type raftStateType uint8

const (
	FOLLOWER raftStateType = iota
	CANDIDATE
	LEADER
)

type eventType uint

const (
	EventHeartbeat eventType = iota
	EventBecomeLeader
	EventBroadcastSent
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
	eventChan chan eventType

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

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

func (rf *Raft) GetLogLastTerm() int {
	return rf.Logs[len(rf.Logs)-1].Term
}

func (rf *Raft) GetLogLastIndex() int {
	return rf.Logs[len(rf.Logs)-1].Index
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
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term > rf.CurrentTerm {
		rf.raftState = FOLLOWER
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
	}
	reply.Term = rf.CurrentTerm
	lastTerm := rf.GetLogLastTerm()
	lastIndex := rf.GetLogLastIndex()
	if args.LastLogTerm < lastTerm {
		return
	}
	if args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex {
		return
	}
	if rf.VoteFor == -1 || rf.VoteFor == args.CandidateID {
		rf.VoteFor = args.CandidateID
		reply.VoteGranted = true
		rf.raftState = FOLLOWER
		go func() {
			rf.eventChan <- EventHeartbeat
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
						rf.nextIndex[i] = rf.GetLogLastIndex() + 1
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
					rf.eventChan <- EventHeartbeat
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
				lastLogTerm := rf.GetLogLastTerm()
				lastLogIndex := rf.GetLogLastIndex()
				args := RequestVoteArgs{rf.CurrentTerm, rf.me, lastLogIndex, lastLogTerm}
				go rf.sendRequestVote(i, args, &RequestVoteReply{})
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
	if args.Term < rf.CurrentTerm {
		reply.NextIndex = args.PrevLogIndex + 1
		reply.Term = rf.CurrentTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.raftState = FOLLOWER
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.persist()
	}
	go func() {
		rf.eventChan <- EventHeartbeat
	}()
	reply.Term = args.Term
	baseIndex := rf.Logs[0].Index
	if rf.GetLogLastIndex() < args.PrevLogIndex || args.PrevLogIndex < baseIndex {
		reply.NextIndex = rf.GetLogLastIndex() + 1
		return
	}
	if rf.Logs[args.PrevLogIndex-baseIndex].Term != args.PrevLogTerm {
		reply.NextIndex = baseIndex + 1
		for i := args.PrevLogIndex - baseIndex - 1; i >= 0; i-- {
			if rf.Logs[args.PrevLogIndex-baseIndex].Term != rf.Logs[i].Term {
				reply.NextIndex = baseIndex + i + 1
				break
			}
		}
		return
	}
	index := 0
	conflictIndex := -1
	for ; index < len(args.Entries); index++ {
		if rf.GetLogLastIndex() < args.PrevLogIndex+1+index {
			break
		} else {
			if rf.Logs[args.PrevLogIndex+1+index-baseIndex].Term != args.Entries[index].Term {
				conflictIndex = args.PrevLogIndex + 1 + index
				break
			}
		}
	}
	if conflictIndex != -1 {
		rf.Logs = rf.Logs[:conflictIndex-baseIndex]
	}
	for ; index < len(args.Entries); index++ {
		rf.Logs = append(rf.Logs, args.Entries[index])
	}
	rf.persist()
	reply.NextIndex = rf.GetLogLastIndex() + 1
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
			if rf.raftState == LEADER {
				if rf.nextIndex[server] < args.PrevLogIndex+1+len(args.Entries) {
					rf.nextIndex[server] = args.PrevLogIndex + 1 + len(args.Entries)
				}
				if rf.matchIndex[server] < rf.nextIndex[server]-1 {
					rf.matchIndex[server] = rf.nextIndex[server] - 1
				}
				index := rf.matchIndex[server]
				baseIndex := rf.Logs[0].Index
				if index > rf.commitIndex && rf.Logs[index-baseIndex].Term == rf.CurrentTerm {
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
			}
		} else {
			if reply.Term > rf.CurrentTerm {
				rf.raftState = FOLLOWER
				rf.CurrentTerm = reply.Term
				rf.VoteFor = -1
				rf.persist()
			} else {
				if rf.raftState == LEADER {
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
				baseIndex := rf.Logs[0].Index
				nextIndex := rf.nextIndex[i]
				if nextIndex <= baseIndex {
					args := InstallSnapshotArgs{Term: rf.CurrentTerm, LastIncludedIndex: rf.Logs[0].Index, LastIncludedTerm: rf.Logs[0].Term, Data: rf.persister.ReadSnapshot()}
					go rf.SendInstallSnapshot(i, args, &InstallSnapshotReply{})
				} else {
					args := AppendEntriesArgs{rf.CurrentTerm, rf.me, nextIndex - 1, rf.Logs[nextIndex-1-baseIndex].Term, rf.Logs[nextIndex-baseIndex:], rf.commitIndex}
					go rf.SendAppendEntries(i, args, &AppendEntriesReply{})
				}
			}
		}
		go func() {
			rf.eventChan <- EventBroadcastSent
		}()
	}
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.raftState = FOLLOWER
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VoteFor = -1
		rf.persist()
	}
	go func() {
		rf.eventChan <- EventHeartbeat
	}()
	if args.LastIncludedIndex < rf.Logs[0].Index {
		reply.Term = rf.CurrentTerm
		return
	}
	baseIndex := rf.Logs[0].Index
	if len(rf.Logs) > args.LastIncludedIndex-baseIndex && rf.Logs[args.LastIncludedIndex-baseIndex].Index == args.LastIncludedIndex && rf.Logs[args.LastIncludedIndex-baseIndex].Term == args.LastIncludedTerm {
		reply.Term = rf.CurrentTerm
		return
	}
	rf.Logs = make([]Log, 1)
	rf.Logs[0] = Log{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}
	rf.persister.SaveSnapshot(args.Data)
	rf.ReadSnapshot(args.Data)
	rf.persist()
	applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: args.Data}
	rf.applyCh <- applyMsg
	reply.Term = rf.CurrentTerm
}

func (rf *Raft) SendInstallSnapshot(server int, args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.CurrentTerm {
			rf.raftState = FOLLOWER
			rf.CurrentTerm = reply.Term
			rf.VoteFor = -1
			rf.persist()
		} else {
			if rf.raftState == LEADER {
				rf.nextIndex[server] = rf.Logs[0].Index + 1
			}
		}
	}
}

func (rf *Raft) StartSnapshot(dbData []byte, LastIncludedIndex int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if LastIncludedIndex > rf.Logs[0].Index {
		baseIndex := rf.Logs[0].Index
		LastIncludedTerm := rf.Logs[LastIncludedIndex-baseIndex].Term
		w := new(bytes.Buffer)
		e := gob.NewEncoder(w)
		e.Encode(LastIncludedIndex)
		e.Encode(LastIncludedTerm)
		data := w.Bytes()
		data = append(data, dbData...)
		rf.persister.SaveSnapshot(data)
		rf.lastApplied = LastIncludedIndex
		rf.TruncateLog(LastIncludedIndex, LastIncludedTerm)
		rf.persist()
	}
}

func (rf *Raft) ReadSnapshot(snapshot []byte) {
	if len(snapshot) <= 0 {
		return
	}
	var LastIncludedIndex int
	var LastIncludedTerm int
	r := bytes.NewBuffer(snapshot)
	d := gob.NewDecoder(r)
	d.Decode(&LastIncludedIndex)
	d.Decode(&LastIncludedTerm)
	rf.lastApplied = LastIncludedIndex
	rf.TruncateLog(LastIncludedIndex, LastIncludedTerm)
}

func (rf *Raft) TruncateLog(index int, term int) {
	baseIndex := rf.Logs[0].Index
	if len(rf.Logs) <= index-baseIndex {
		rf.Logs = make([]Log, 1)
		rf.Logs[0] = Log{Index: index, Term: term}
	} else {
		if rf.Logs[index-baseIndex].Index == index && rf.Logs[index-baseIndex].Term == term {
			rf.Logs = rf.Logs[index-baseIndex:]
		} else {
			rf.Logs = make([]Log, 1)
			rf.Logs[0] = Log{Index: index, Term: term}
		}
	}
}

func (rf *Raft) CommitUpdate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	baseIndex := rf.Logs[0].Index
	for rf.lastApplied < rf.commitIndex {
		applyMessage := ApplyMsg{Index: rf.lastApplied + 1, Command: rf.Logs[rf.lastApplied+1-baseIndex].Command, UseSnapshot: false}
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
		index = rf.GetLogLastIndex() + 1
		rf.Logs = append(rf.Logs, Log{rf.CurrentTerm, index, command})
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
	rf.Logs[0] = Log{Term: 0, Index: 0}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}
	rf.eventChan = make(chan eventType)
	rf.applyCh = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.ReadSnapshot(persister.ReadSnapshot())
	go func() {
		applyMsg := ApplyMsg{UseSnapshot: true, Snapshot: persister.ReadSnapshot()}
		applyCh <- applyMsg
	}()

	go rf.Loop()

	return rf
}
