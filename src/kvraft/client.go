package raftkv

import "labrpc"
import "crypto/rand"
import "math/big"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	me int64

	mu        sync.Mutex
	timestamp int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.me = nrand()
	ck.timestamp = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.mu.Lock()
	timestamp := ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()
	for {
		for _, server := range ck.servers {
			args := GetArgs{key, ck.me, timestamp}
			reply := GetReply{}
			ok := server.Call("RaftKV.Get", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					switch reply.Err {
					case OK:
						return reply.Value
					case ErrNoKey:
						return ""
					}
				}
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("RaftKV.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	timestamp := ck.timestamp
	ck.timestamp++
	ck.mu.Unlock()
	for {
		for _, server := range ck.servers {
			args := PutAppendArgs{key, value, op, ck.me, timestamp}
			reply := PutAppendReply{}
			ok := server.Call("RaftKV.PutAppend", &args, &reply)
			if ok {
				if !reply.WrongLeader {
					return
				}
			}
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
