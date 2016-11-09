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

type Log struct {
	term    int
	command interface{}
}

type raftStateType uint8

const (
	FOLLWER raftStateType = iota
	CANDIDATE
	LEADER
)

const (
	HEARTBEAT_EVENT uint8 = iota
	BECOME_LEADER_EVENT
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
	// logs        []Log

	// // Volatile state on all servers
	// commitIndex int
	// lastApplied int

	// // Volatile state on leaders
	// nextIndex  []int
	// matchIndex []int

	// Election
	votes     int
	eventChan chan uint8
}

func (rf *Raft) Loop() {
	for {
		switch rf.raftState {
		case LEADER:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(50) * time.Millisecond):
				rf.mu.Lock()
				if rf.raftState == LEADER {
					rf.mu.Unlock()
					rf.BroadcastHeartbeat()
				} else {
					rf.mu.Unlock()
				}
			}
		case CANDIDATE:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(rand.Int63n(75)+75) * time.Millisecond):
				rf.mu.Lock()
				if rf.raftState == CANDIDATE {
					rf.currentTerm++
					fmt.Printf("%d remains candidate, term update to %d\n", rf.me, rf.currentTerm)
					rf.voteFor = rf.me
					rf.votes = 1
					rf.mu.Unlock()
					rf.BroadcastRequestVote()
				} else {
					rf.mu.Unlock()
				}
			}
		case FOLLWER:
			select {
			case <-rf.eventChan:
			case <-time.After(time.Duration(rand.Int63n(150)+150) * time.Millisecond):
				rf.mu.Lock()
				if rf.raftState == FOLLWER {
					rf.raftState = CANDIDATE
					rf.currentTerm++
					fmt.Printf("%d becomes candidate, term update to %d\n", rf.me, rf.currentTerm)
					rf.voteFor = rf.me
					rf.votes = 1
					rf.mu.Unlock()
					rf.BroadcastRequestVote()
				} else {
					rf.mu.Unlock()
				}
			}
		}
	}
}

func (rf *Raft) BroadcastRequestVote() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := RequestVoteArgs{currentTerm, rf.me}
			reply := &RequestVoteReply{}
			go rf.sendRequestVote(i, args, reply)
		}
	}
}

func (rf *Raft) BroadcastHeartbeat() {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			args := AppendEntriesArgs{currentTerm}
			reply := &AppendEntriesReply{}
			go rf.SendAppendEntries(i, args, reply)
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
	term = rf.currentTerm
	isLeader = rf.raftState == LEADER
	rf.mu.Unlock()
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
	Term        int
	CandidateID int
	// LastLogIndex int
	// LastLogTerm  int
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
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.raftState = FOLLWER
		rf.currentTerm = args.Term
		fmt.Printf("In RequestVote, %d remains/becomes follower, term update to %d\n", rf.me, rf.currentTerm)
		rf.voteFor = args.CandidateID
	}
	rf.mu.Unlock()
	go func() {
		rf.eventChan <- HEARTBEAT_EVENT
	}()
	reply.Term = args.Term
	rf.mu.Lock()
	if rf.voteFor == -1 || rf.voteFor == args.CandidateID {
		rf.voteFor = args.CandidateID
		reply.VoteGranted = true
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
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
	if ok {
		if reply.VoteGranted {
			rf.mu.Lock()
			rf.votes++
			if rf.votes*2 > len(rf.peers) && rf.raftState == CANDIDATE {
				rf.raftState = LEADER
				fmt.Printf("In sendRequestVote, %d becomes leader\n", rf.me)
				rf.mu.Unlock()
				rf.BroadcastHeartbeat()
				go func() {
					rf.eventChan <- BECOME_LEADER_EVENT
				}()
			} else {
				rf.mu.Unlock()
			}
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
	Term int
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.raftState = FOLLWER
		rf.currentTerm = args.Term
		fmt.Printf("In AppendEntries, %d remains/becomes follower, term update to %d\n", rf.me, rf.currentTerm)
		rf.voteFor = -1
		rf.votes = 0
	}
	rf.mu.Unlock()
	go func() {
		rf.eventChan <- HEARTBEAT_EVENT
	}()
	reply.Term = args.Term
}

func (rf *Raft) SendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.raftState = FOLLWER
			rf.currentTerm = reply.Term
			fmt.Printf("In SendAppendEntries, %d becomes follower, term update to %d\n", rf.me, rf.currentTerm)
			rf.voteFor = -1
			rf.votes = 0
		}
		rf.mu.Unlock()
	}
	return ok
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
	rf.eventChan = make(chan uint8)
	go rf.Loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
