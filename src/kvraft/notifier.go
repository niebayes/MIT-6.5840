package kvraft

import (
	"sync"
	"time"
)

const maxWaitTime = 500 * time.Millisecond

type Notifier struct {
	done              sync.Cond
	maxRegisteredOpId int
}

func (kv *KVServer) makeNotifier(op *Op) {
	kv.getNotifier(op, true)
	kv.makeAlarm(op)
}

func (kv *KVServer) makeAlarm(op *Op) {
	go func() {
		<-time.After(maxWaitTime)
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.notify(op)
	}()
}

func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if notifer, ok := kv.notifierOfClerk[op.ClerkId]; ok {
		notifer.maxRegisteredOpId = max(notifer.maxRegisteredOpId, op.OpId)
		return notifer
	}

	if !forced {
		return nil
	}

	notifier := new(Notifier)
	notifier.done = *sync.NewCond(&kv.mu)
	notifier.maxRegisteredOpId = op.OpId
	kv.notifierOfClerk[op.ClerkId] = notifier

	return notifier
}

func (kv *KVServer) wait(op *Op) {
	println("S%v waits applied (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)

	// warning: we could only use `notifier.done.Wait` but there's a risk of spurious wakeup or
	// wakeup by stale ops.
	for !kv.killed() {
		if notifier := kv.getNotifier(op, false); notifier != nil {
			notifier.done.Wait()
		} else {
			break
		}
	}
}

func (kv *KVServer) notify(op *Op) {
	if notifer := kv.getNotifier(op, false); notifer != nil {
		if op.OpId == notifer.maxRegisteredOpId {
			delete(kv.notifierOfClerk, op.ClerkId)
		}
		notifer.done.Broadcast()
		println("S%v notifies applied (C=%v Id=%v)", kv.me, op.ClerkId, op.OpId)
	}
}
