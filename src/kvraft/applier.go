package kvraft

import (
	"log"
)

func (kv *KVServer) executor() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()

		// FIXME: doubt the snapshot index checking is necessary.
		if m.SnapshotValid {
			kv.ingestSnapshot(m.Snapshot)

		} else if m.CommandValid {
			op := m.Command.(*Op)
			if kv.isNoOp(op) {
				// skip no-ops.

			} else {
				if kv.maybeApplyClientOp(op) {
					println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, m.CommandIndex)
				}
			}

			if kv.gcEnabled && kv.approachGCLimit() {
				kv.checkpoint(m.CommandIndex)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) maybeApplyClientOp(op *Op) bool {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId

		kv.notify(op)
		return true
	}
	return false
}

func (kv *KVServer) applyClientOp(op *Op) {
	switch op.OpType {
	case "Get":
		// only write ops are applied to the database.

	case "Put":
		kv.db[op.Key] = op.Value

	case "Append":
		// note: the default value is returned if the key does not exist.
		kv.db[op.Key] += op.Value

	default:
		log.Fatalf("unexpected client op type %v", op.OpType)
	}
}

func (kv *KVServer) isApplied(op *Op) bool {
	maxAppliedOpId, ok := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return ok && maxAppliedOpId >= op.OpId
}

func (kv *KVServer) waitUntilAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.OpType == "Get" {
		println("S%v receives Get (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	} else {
		println("S%v receives PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	}

	if !kv.isApplied(op) {
		// warning: it might be reasonable to check here if someone is waiting for the same op.
		// however, it is not necessary and it does not increase performance as the benchmarking shows.

		if !kv.propose(op) {
			return ErrWrongLeader, ""
		}

		// wait until applied or timeout.
		kv.makeNotifier(op)
		kv.wait(op)
	}

	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
			println("S%v replies Get (C=%v Id=%v) with Value=%v", kv.me, op.ClerkId, op.OpId, value)
		} else {
			println("S%v replies PutAppend (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
