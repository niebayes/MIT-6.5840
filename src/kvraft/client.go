package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

const retryInterval = 100 * time.Millisecond

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clerkId  int64 // the unique id of this clerk.
	nextOpId int   // the next op id to allocate for an op.
	leader   int
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
	ck.clerkId = nrand()
	ck.nextOpId = 0
	ck.leader = 0 // the leader defaults to the servers[0].
	return ck
}

func (ck *Clerk) allocateOpId() int {
	opId := ck.nextOpId
	ck.nextOpId++
	return opId
}

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
func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), Key: key}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leader + i) % len(ck.servers)
			println("C%v sends Get (C=%v Id=%v K=%v) to S%v", args.ClerkId, args.ClerkId, args.OpId, args.Key, serverId)

			var reply GetReply
			if ok := ck.servers[serverId].Call("KVServer.Get", args, &reply); ok {
				if reply.Err == OK {
					ck.leader = serverId
					println("C%v receives Get reply (C=%v Id=%v K=%v) from S%v with value=%v", args.ClerkId, args.ClerkId, args.OpId, args.Key, serverId, reply.Value)
					return reply.Value
				}
				println("C%v receives Get reply (C=%v Id=%v K=%v) from S%v with Err=%v", args.ClerkId, args.ClerkId, args.OpId, args.Key, serverId, reply.Err)
			}
		}
		time.Sleep(retryInterval)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), OpType: op, Key: key, Value: value}

	for {
		for i := 0; i < len(ck.servers); i++ {
			serverId := (ck.leader + i) % len(ck.servers)
			println("C%v sends PutAppend (C=%v Id=%v T=%v K=%v V=%v) to S%v", args.ClerkId, args.ClerkId, args.OpId, args.OpType, args.Key, args.Value, serverId)

			var reply PutAppendReply
			if ok := ck.servers[serverId].Call("KVServer.PutAppend", args, &reply); ok {
				if reply.Err == OK {
					ck.leader = serverId
					println("C%v receives PutAppend reply (C=%v Id=%v T=%v K=%v V=%v) from S%v", args.ClerkId, args.ClerkId, args.OpId, args.OpType, args.Key, args.Value, serverId)
					return
				}
				println("C%v receives PutAppend reply (C=%v Id=%v K=%v) from S%v with Err=%v", args.ClerkId, args.ClerkId, args.OpId, args.Key, serverId, reply.Err)
			}
		}
		time.Sleep(retryInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
