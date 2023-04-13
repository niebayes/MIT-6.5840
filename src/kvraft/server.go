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

	// notifer of each clerk.
	notifierOfClerk map[int64]*Notifier

	nextExecIndex      int
	committedOps       map[int]*Op
	hasNewCommittedOps sync.Cond
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

	kv.notifierOfClerk = map[int64]*Notifier{}
	kv.nextExecIndex = 1
	kv.committedOps = make(map[int]*Op)
	kv.hasNewCommittedOps = *sync.NewCond(&kv.mu)
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.collector()
	go kv.executor()
	go kv.noOpTicker()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	println("S%v receives Get (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
	op := &Op{ClerkId: args.ClerkId, OpId: args.OpId, OpType: "Get", Key: args.Key}
	reply.Err, reply.Value = kv.waitUntilAppliedOrTimeout(op)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	println("S%v receives PutAppend (C=%v Id=%v)", kv.me, args.ClerkId, args.OpId)

	// wrap the request into an op.
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
