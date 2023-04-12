package kvraft

import (
	"log"
	"time"
)

const maxWaitTime = 500 * time.Millisecond

func (kv *KVServer) executor() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()

		if m.SnapshotValid {
			kv.ingestSnapshot(m.Snapshot, m.SnapshotIndex, m.SnapshotTerm)

		} else {
			op := m.Command.(*Op)
			if kv.isNoOp(op) {
				// skip no-ops.

			} else {
				kv.maybeApplyClientOp(op, m.CommandIndex)
			}
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) maybeApplyClientOp(op *Op, index int) {
	if !kv.isApplied(op) {
		kv.applyClientOp(op)
		kv.maxAppliedOpIdOfClerk[op.ClerkId] = op.OpId

		kv.notify(op)

		println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, index)
	}
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
	maxApplyOpId, exist := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return exist && maxApplyOpId >= op.OpId
}

func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func (kv *KVServer) waitUntilAppliedOrTimeout(op *Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.isApplied(op) {
		if !kv.propose(op) {
			return ErrWrongLeader, ""
		}

		notifier := kv.getNotifier(op, true)
		kv.makeAlarm(op)
		notifier.done.Wait()
	}

	if kv.isApplied(op) {
		value := ""
		if op.OpType == "Get" {
			value = kv.db[op.Key]
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
