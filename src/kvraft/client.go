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
	leader   int   // known leader, defaults to the servers[0].
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
	ck.leader = 0
	return ck
}

func (ck *Clerk) allocateOpId() int {
	opId := ck.nextOpId
	ck.nextOpId++
	return opId
}

func (ck *Clerk) Get(key string) string {
	args := &GetArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), Key: key}

	for {
		for i := 0; i < len(ck.servers); i++ {
			// try to send to the known first.
			serverId := (ck.leader + i) % len(ck.servers)

			var reply GetReply
			if ok := ck.servers[serverId].Call("KVServer.Get", args, &reply); ok {
				if reply.Err == OK {
					ck.leader = serverId
					return reply.Value
				}
			}
		}
		time.Sleep(retryInterval)
	}
}

func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := &PutAppendArgs{ClerkId: ck.clerkId, OpId: ck.allocateOpId(), OpType: op, Key: key, Value: value}

	for {
		for i := 0; i < len(ck.servers); i++ {
			// try to send to the known first.
			serverId := (ck.leader + i) % len(ck.servers)

			var reply PutAppendReply
			if ok := ck.servers[serverId].Call("KVServer.PutAppend", args, &reply); ok {
				if reply.Err == OK {
					ck.leader = serverId
					return
				}
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
