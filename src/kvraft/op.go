package kvraft

import (
	"time"
)

const proposeNoOpInterval = 250 * time.Millisecond

type Op struct {
	ClerkId int64
	OpId    int
	OpType  string // "Get", "Put", "Append", "NoOp".
	Key     string
	Value   string
}

func (kv *KVServer) isNoOp(op *Op) bool {
	return op.OpType == "NoOp"
}

func (kv *KVServer) noOpTicker() {
	// warning: the no-op ticker could be omitted.
	for !kv.killed() {
		if kv.isLeader() {
			op := &Op{OpType: "NoOp"}
			kv.propose(op)
		}

		time.Sleep(proposeNoOpInterval)
	}
}
