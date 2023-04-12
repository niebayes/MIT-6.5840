package kvraft

import (
	"log"
	"time"
)

const pollRaftInterval = 25 * time.Millisecond
const maxWaitTime = 1000 * time.Millisecond

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

func (kv *KVServer) waitUntilAppliedOrTimeout(op *Op) (bool, string) {
	var value string = ""
	startTime := time.Now()

	for !kv.killed() && time.Since(startTime) < maxWaitTime {
		kv.mu.Lock()

		if kv.isApplied(op) {
			value = kv.db[op.Key]

			// only the leader is eligible to reply Get.
			if op.OpType == "Get" && !kv.isLeader() {
				kv.mu.Unlock()
				break
			}

			kv.mu.Unlock()
			return true, value
		}

		kv.mu.Unlock()
		time.Sleep(pollRaftInterval)
	}
	return false, value
}

func (kv *KVServer) isApplied(op *Op) bool {
	maxApplyOpId, exist := kv.maxAppliedOpIdOfClerk[op.ClerkId]
	return exist && maxApplyOpId >= op.OpId
}
