package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
)

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	persister *raft.Persister

	maxRaftStateSize int // snapshot if raft log grows this big
	gcEnabled        bool

	// key-value store.
	db map[string]string

	// the maximum op id among all applied ops of each clerk.
	maxAppliedOpIdOfClerk map[int64]int

	notifierOfOp map[int64]map[int]chan struct{}
}

// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.Save() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftStateSize int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftStateSize = maxRaftStateSize
	kv.gcEnabled = maxRaftStateSize != -1
	kv.persister = persister

	if kv.gcEnabled && kv.persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(kv.persister.ReadSnapshot(), 0, 0)

	} else {
		kv.db = make(map[string]string)
		kv.maxAppliedOpIdOfClerk = make(map[int64]int)
	}

	kv.notifierOfOp = make(map[int64]map[int]chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// start the executor thread.
	go kv.executor()
	// start a thread to periodically propose no-ops in order to let the server catches up quickly.
	go kv.noOpTicker()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	println("S%v receives Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}

	kv.mu.Lock()
	if !kv.isApplied(op) {
		if !kv.propose(op) {
			kv.mu.Unlock()
			println("S%v is not leader (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
			reply.Err = ErrWrongLeader
			return
		}
	}
	kv.mu.Unlock()

	println("S%v waits Get applied (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
	if applied, value := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK
		reply.Value = value

		println("S%v replies Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	println("S%v receives PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}

	kv.mu.Lock()
	if !kv.isApplied(op) {
		if !kv.propose(op) {
			kv.mu.Unlock()
			println("S%v is not leader (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
			reply.Err = ErrWrongLeader
			return
		}
	}
	kv.mu.Unlock()

	println("S%v waits PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)
	if applied, _ := kv.waitUntilAppliedOrTimeout(op); applied {
		reply.Err = OK

		println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	} else {
		reply.Err = ErrNotApplied
	}
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
