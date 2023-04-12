package kvraft

// a snapshotting starts if the raft state size is higher than GCRatio * maxRaftStateSize.
const GCRatio = 0.8

func (kv *KVServer) approachGCLimit() bool {
	return float32(kv.persister.RaftStateSize()) > GCRatio*float32(kv.maxRaftStateSize)
}

func (kv *KVServer) ingestSnapshot(snapshot []byte, snapshotIndex, snapshotTerm int) {
	// TODO: decode kv data and server state from the snapshot.
	// TODO: update server state accordingly.
}

func (kv *KVServer) makeSnapshot() []byte {
	// TODO: encode kv data into a snapshot, including all necessary server state.
	return nil
}

func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
}
