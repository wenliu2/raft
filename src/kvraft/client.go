package raftkv

import (
	"crypto/rand"
	"labrpc"
	"math/big"
	"time"
)

const RetryInterval = time.Duration(125 * time.Millisecond)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	//index of leader
	leader int
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
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	serverIdx := ck.leader
	for {
		DPrintf("key: %s, value: %s, op: %s\n", key, value, op)
		var args = PutAppendArgs{key, value, op}
		var reply = PutAppendReply{false, ""}
		DPrintf("Call server: %d\n", serverIdx)
		ok := ck.servers[serverIdx].Call("KVServer.PutAppend", &args, &reply)
		DPrintf("Reply: %v\n", reply)
		if ok && reply.Err == "" {
			ck.leader = serverIdx
			break
		} else {
			serverIdx = (serverIdx + 1) % len(ck.servers)
			time.Sleep(RetryInterval)
		}
	}
	DPrintf("Call server %d successfully\n", serverIdx)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
