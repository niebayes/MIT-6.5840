package kvraft

func (kv *KVServer) propose(op *Op) bool {
	_, _, isLeader := kv.rf.Start(op)
	return isLeader
}

func (kv *KVServer) isLeader() bool {
	_, isLeader := kv.rf.GetState()
	return isLeader
}
