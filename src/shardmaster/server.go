package shardmaster

import (
	"encoding/gob"
	"labrpc"
	"raft"
	"reflect"
	"sort"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	appliedChans     map[int]chan Op
	clientTimestamps map[int64]int

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Operation   string
	ClientID    int64
	Timestamp   int
	JoinServers map[int][]string
	LeaveGIDs   []int
	MoveShard   int
	MoveGID     int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.Err = OK
	reply.WrongLeader = !sm.appendNewEntries(Op{Operation: "Join", ClientID: args.ClientID, Timestamp: args.Timestamp, JoinServers: args.Servers})
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.Err = OK
	reply.WrongLeader = !sm.appendNewEntries(Op{Operation: "Leave", ClientID: args.ClientID, Timestamp: args.Timestamp, LeaveGIDs: args.GIDs})
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.Err = OK
	reply.WrongLeader = !sm.appendNewEntries(Op{Operation: "Move", ClientID: args.ClientID, Timestamp: args.Timestamp, MoveShard: args.Shard, MoveGID: args.GID})
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	isLeader := sm.appendNewEntries(Op{Operation: "Query", ClientID: args.ClientID, Timestamp: args.Timestamp})
	if isLeader {
		reply.WrongLeader = false
		reply.Err = OK
		sm.mu.Lock()
		if len(sm.configs) < args.Num || args.Num == -1 {
			reply.Config = sm.configs[len(sm.configs)-1]
		} else {
			reply.Config = sm.configs[args.Num]
		}
		sm.mu.Unlock()
	} else {
		reply.WrongLeader = true
	}
}

func (sm *ShardMaster) appendNewEntries(op Op) bool {
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return false
	}
	sm.mu.Lock()
	appliedChan, ok := sm.appliedChans[index]
	if !ok {
		appliedChan = make(chan Op)
		sm.appliedChans[index] = appliedChan
	}
	sm.mu.Unlock()
	select {
	case appliedOp := <-appliedChan:
		return reflect.DeepEqual(appliedOp, op)
	case <-time.After(150 * time.Millisecond):
		return false
	}
}

func (sm *ShardMaster) copyFromConfig(config Config) Config {
	newConfig := Config{Num: config.Num + 1}
	for index := range config.Shards {
		newConfig.Shards[index] = config.Shards[index]
	}
	newConfig.Groups = map[int][]string{}
	for key, value := range config.Groups {
		newConfig.Groups[key] = value
	}
	return newConfig
}

func (sm *ShardMaster) getShardCount(shards [NShards]int) map[int]int {
	shardCount := map[int]int{}
	for i := 0; i < NShards; i++ {
		shardCount[shards[i]]++
	}
	return shardCount
}

// GIDs must be sorted, or the transfer won't be minimal
func (sm *ShardMaster) getSortedGIDs(groups map[int][]string) []int {
	gids := []int{}
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)
	return gids
}

func (sm *ShardMaster) joinRebalance(config *Config) {
	numberOfKeys := len(config.Groups)
	if numberOfKeys == 0 {
		return
	}
	quotient := NShards / numberOfKeys
	gids := sm.getSortedGIDs(config.Groups)
	for shard, gid := range config.Shards {
		if _, ok := config.Groups[gid]; !ok {
			config.Shards[shard] = gids[0]
		}
	}
	shardCount := sm.getShardCount(config.Shards)
	for i := 0; i < 2; i++ {
		for shard, gid := range config.Shards {
			if shardCount[gid] > quotient+1-i {
				for _, newGID := range gids {
					if shardCount[newGID] < quotient+1-i {
						config.Shards[shard] = newGID
						shardCount[gid]--
						shardCount[newGID]++
						break
					}
				}
			}
		}
	}
}

func (sm *ShardMaster) leaveRebalance(config *Config) {
	numberOfKeys := len(config.Groups)
	if numberOfKeys == 0 {
		return
	}
	quotient := NShards / numberOfKeys
	shardCount := sm.getShardCount(config.Shards)
	gids := sm.getSortedGIDs(config.Groups)
	for i := 0; i < 2; i++ {
		for shard, gid := range config.Shards {
			if _, ok := config.Groups[gid]; !ok {
				for _, newGID := range gids {
					if shardCount[newGID] < quotient+i {
						config.Shards[shard] = newGID
						shardCount[gid]--
						shardCount[newGID]++
						break
					}
				}
			}
		}
	}
}

func (sm *ShardMaster) Loop() {
	for {
		applyMsg := <-sm.applyCh
		if applyMsg.Command == nil {
			continue
		}
		sm.mu.Lock()
		op := applyMsg.Command.(Op)
		if sm.clientTimestamps[op.ClientID] < op.Timestamp {
			switch op.Operation {
			case "Join":
				newConfig := sm.copyFromConfig(sm.configs[len(sm.configs)-1])
				for key, value := range op.JoinServers {
					newConfig.Groups[key] = value
				}
				sm.joinRebalance(&newConfig)
				sm.configs = append(sm.configs, newConfig)
			case "Leave":
				newConfig := sm.copyFromConfig(sm.configs[len(sm.configs)-1])
				for index := range op.LeaveGIDs {
					delete(newConfig.Groups, op.LeaveGIDs[index])
				}
				sm.leaveRebalance(&newConfig)
				sm.configs = append(sm.configs, newConfig)
			case "Move":
				newConfig := sm.copyFromConfig(sm.configs[len(sm.configs)-1])
				newConfig.Shards[op.MoveShard] = op.MoveGID
				sm.configs = append(sm.configs, newConfig)
			case "Query":
			}
			sm.clientTimestamps[op.ClientID] = op.Timestamp
		}
		go func(applyMsg raft.ApplyMsg) {
			sm.mu.Lock()
			appliedChan, ok := sm.appliedChans[applyMsg.Index]
			sm.mu.Unlock()
			if ok {
				appliedChan <- applyMsg.Command.(Op)
			}
		}(applyMsg)
		sm.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	gob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.appliedChans = make(map[int]chan Op)
	sm.clientTimestamps = make(map[int64]int)

	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go sm.Loop()

	return sm
}
