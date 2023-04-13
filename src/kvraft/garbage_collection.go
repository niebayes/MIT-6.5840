package kvraft

import (
	"6.5840/labgob"
	"bytes"
)

// a snapshotting starts if the raft state size is higher than GCRatio * maxRaftStateSize.
const GCRatio = 0.8

func (kv *KVServer) deleteStaleOps() {
	for i := kv.nextExecIndex; i <= kv.snapshotIndex; i++ {
		if _, ok := kv.committedOps[i]; !ok {
			break
		}
		delete(kv.committedOps, i)
	}
}

func (kv *KVServer) approachGCLimit() bool {
	// note: persister has its own mutex and hence no race would be raised with raft.
	return float32(kv.persister.RaftStateSize()) > GCRatio*float32(kv.maxRaftStateSize)
}

func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.snapshotIndex) != nil || d.Decode(&kv.db) != nil || d.Decode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}

	kv.nextExecIndex = max(kv.nextExecIndex, kv.snapshotIndex+1)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// FIXME: do I need to encode a deep clone?
	if e.Encode(kv.snapshotIndex) != nil || e.Encode(kv.db) != nil || e.Encode(kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to encode some fields")
	}
	return w.Bytes()
}

func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)
	kv.snapshotIndex = index
}
