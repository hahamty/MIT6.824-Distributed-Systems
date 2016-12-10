package raftkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ClientID  int64
	Timestamp int
}

type RaftKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	DB               map[string]string
	appliedChans     map[int]chan Op
	ClientTimestamps map[int64]int
}

func (kv *RaftKV) Loop() {
	for {
		applyMsg := <-kv.applyCh
		kv.mu.Lock()
		if applyMsg.UseSnapshot {
			r := bytes.NewBuffer(applyMsg.Snapshot)
			d := gob.NewDecoder(r)
			d.Decode(&kv.DB)
			d.Decode(&kv.ClientTimestamps)
		} else {
			op := applyMsg.Command.(Op)
			if kv.ClientTimestamps[op.ClientID] < op.Timestamp {
				switch op.Operation {
				case "Put":
					kv.DB[op.Key] = op.Value
				case "Append":
					kv.DB[op.Key] += op.Value
				case "Get":
				}
				kv.ClientTimestamps[op.ClientID] = op.Timestamp
			}
			go func(applyMsg raft.ApplyMsg) {
				kv.mu.Lock()
				appliedChan, ok := kv.appliedChans[applyMsg.Index]
				kv.mu.Unlock()
				if ok {
					appliedChan <- applyMsg.Command.(Op)
				}
			}(applyMsg)
			if kv.maxraftstate > 0 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
				w := new(bytes.Buffer)
				e := gob.NewEncoder(w)
				e.Encode(kv.DB)
				e.Encode(kv.ClientTimestamps)
				data := w.Bytes()
				go kv.rf.StartSnapshot(data, applyMsg.Index)
			}
		}
		kv.mu.Unlock()
	}
}

func (kv *RaftKV) AppendNewEntries(op Op) bool {
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false
	}
	kv.mu.Lock()
	appliedChan, ok := kv.appliedChans[index]
	if !ok {
		appliedChan = make(chan Op)
		kv.appliedChans[index] = appliedChan
	}
	kv.mu.Unlock()
	select {
	case appliedOp := <-appliedChan:
		return appliedOp == op
	case <-time.After(150 * time.Millisecond):
		return false
	}
}

func (kv *RaftKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	isLeader := kv.AppendNewEntries(Op{Key: args.Key, Operation: "Get", ClientID: args.ClientID, Timestamp: args.Timestamp})
	if isLeader {
		reply.WrongLeader = false
		kv.mu.Lock()
		defer kv.mu.Unlock()
		if val, ok := kv.DB[args.Key]; ok {
			reply.Err = OK
			reply.Value = val
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.WrongLeader = true
	}
}

func (kv *RaftKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	reply.Err = OK
	reply.WrongLeader = !kv.AppendNewEntries(Op{args.Key, args.Value, args.Op, args.ClientID, args.Timestamp})
}

//
// the tester calls Kill() when a RaftKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *RaftKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots with persister.SaveSnapshot(),
// and Raft should save its state (including log) with persister.SaveRaftState().
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *RaftKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(RaftKV)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// Your initialization code here.
	kv.DB = make(map[string]string)
	kv.appliedChans = make(map[int]chan Op)
	kv.ClientTimestamps = make(map[int64]int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.Loop()

	return kv
}
