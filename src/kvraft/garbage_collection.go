package kvraft

import (
	"6.5840/labgob"
	"bytes"
)

// a snapshotting starts if the raft state size is higher than GCRatio * maxRaftStateSize.
const GCRatio = 0.8

func (kv *KVServer) approachGCLimit() bool {
	// note: persister has its own mutex and hence no race would be raised with raft.
	return float32(kv.persister.RaftStateSize()) > GCRatio*float32(kv.maxRaftStateSize)
}

func (kv *KVServer) ingestSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	if d.Decode(&kv.db) != nil || d.Decode(&kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}

	// kv.lastApplied = kv.snapshotIndex

	// println("S%v ingests snapshot (SI=%v)", kv.me, )
	// println("S%v ingests with (SI=%v db=%v maxAppliedOpId=%v)", kv.me, kv.snapshotIndex, kv.db, kv.maxAppliedOpIdOfClerk)
	println("S%v ingests with (db=%v maxAppliedOpId=%v)", kv.me, kv.db, kv.maxAppliedOpIdOfClerk)
}

func (kv *KVServer) makeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// FIXME: do I need to encode a deep clone?
	if e.Encode(kv.db) != nil || e.Encode(kv.maxAppliedOpIdOfClerk) != nil {
		panic("failed to encode some fields")
	}
	return w.Bytes()
}

func (kv *KVServer) checkpoint(index int) {
	snapshot := kv.makeSnapshot()
	kv.rf.Snapshot(index, snapshot)

	println("S%v checkpoints (SI=%v)", kv.me, index)
	db := make(map[string]string)
	maxAppliedOpIdOfClerk := make(map[int64]int)
	nw := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(nw)
	if d.Decode(&db) != nil || d.Decode(&maxAppliedOpIdOfClerk) != nil {
		panic("failed to decode some fields")
	}
	// println("S%v checkpoints with (SI=%v db=%v maxAppliedOpId=%v)", kv.me, snapshotIndex, db, maxAppliedOpIdOfClerk)
	println("S%v checkpoints with (db=%v maxAppliedOpId=%v)", kv.me, db, maxAppliedOpIdOfClerk)
}
