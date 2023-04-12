package kvraft

import "sync"

type Notifier struct {
	done sync.Cond
}

func (kv *KVServer) notify(op *Op) {
	if notifer := kv.getNotifier(op, false); notifer != nil {
		notifer.done.Broadcast()
		delete(kv.notifierOfClerk, op.ClerkId)
	}
}

func (kv *KVServer) getNotifier(op *Op, forced bool) *Notifier {
	if _, ok := kv.notifierOfClerk[op.ClerkId]; ok {
		return kv.notifierOfClerk[op.ClerkId]
	}

	if !forced {
		return nil
	}

	notifier := new(Notifier)
	notifier.done = *sync.NewCond(&kv.mu)
	kv.notifierOfClerk[op.ClerkId] = notifier

	return notifier
}
