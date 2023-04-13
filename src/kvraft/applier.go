package kvraft

import (
	"log"
)

func (kv *KVServer) collector() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}
		kv.mu.Lock()
		kv.committedOps[m.CommandIndex] = m.Command.(*Op)
		kv.mu.Unlock()
		kv.hasNewCommittedOps.Signal()
	}
}

func (kv *KVServer) executor() {
	kv.mu.Lock()
	for !kv.killed() {
		if op, committed := kv.committedOps[kv.nextExecIndex]; committed {
			if kv.isNoOp(op) {
				// skip no-ops.

			} else {
				if kv.maybeApplyClientOp(op) {
					println("S%v applied client op (C=%v Id=%v) at N=%v", kv.me, op.ClerkId, op.OpId, kv.nextExecIndex)
				}
			}

			delete(kv.committedOps, kv.nextExecIndex)
			kv.nextExecIndex++

		} else {
			kv.hasNewCommittedOps.Wait()
		}
	}
	kv.mu.Unlock()
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
		}
		return OK, value
	}
	return ErrNotApplied, ""
}
