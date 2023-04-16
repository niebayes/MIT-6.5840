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

	// notifier of each clerk.
	notifierOfClerk map[int64]*Notifier
}

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftStateSize int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(&Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.mu = sync.Mutex{}

	kv.maxRaftStateSize = maxRaftStateSize
	kv.gcEnabled = maxRaftStateSize != -1

	if kv.gcEnabled && kv.persister.SnapshotSize() > 0 {
		kv.ingestSnapshot(kv.persister.ReadSnapshot())

	} else {
		kv.db = make(map[string]string)
		kv.maxAppliedOpIdOfClerk = make(map[int64]int)
	}

	kv.notifierOfClerk = map[int64]*Notifier{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.executor()
	go kv.noOpTicker()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}
	reply.Err, reply.Value = kv.waitUntilAppliedOrTimeout(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: args.OpType, Key: args.Key, Value: args.Value}
	reply.Err, _ = kv.waitUntilAppliedOrTimeout(op)
}

func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
