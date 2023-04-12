package kvraft

func (kv *KVServer) getNotifier(op *Op) chan struct{} {
	return nil
}

func (kv *KVServer) delNotifier(op *Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// note: delete is no-op if the key does not exist.
	delete(kv.notifierOfOp[op.ClerkId], op.OpId)
}
