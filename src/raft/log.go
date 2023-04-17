package raft

import (
	"errors"
)

// warning: not used actually.
var ErrOutOfBound = errors.New("index out of bound")

type Entry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

type Snapshot struct {
	Data  []byte
	Index uint64
	Term  uint64
}

// Log manages log entries, its struct look like:
//
//	     snapshot/first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	// compacted log entries.
	snapshot           Snapshot
	hasPendingSnapshot bool // true if the snapshot is not yet delivered to the application.

	// persisted log entries.
	entries []Entry

	// TODO: rename applied with delivered. Update comments and docs as well.
	applied   uint64 // the highest log index of the log entry raft knows that the application has applied.
	committed uint64 // the highest log index of the log entry raft knows that the raft cluster has committed.

	logger *Logger
}

func makeLog() Log {
	log := Log{
		snapshot:           Snapshot{Data: nil, Index: 0, Term: 0},
		hasPendingSnapshot: false,
		entries:            []Entry{{Index: 0, Term: 0}}, // use a dummy entry to simplify indexing operations.
		applied:            0,
		committed:          0,
	}

	return log
}

func (log *Log) toArrayIndex(index uint64) uint64 {
	// warning: an unstable implementation may incur integer underflow. (my implementation is stable now)
	return index - log.firstIndex()
}

// always return the snapshot index.
func (log *Log) firstIndex() uint64 {
	return log.entries[0].Index
}

func (log *Log) lastIndex() uint64 {
	return log.entries[len(log.entries)-1].Index
}

func (log *Log) term(index uint64) (uint64, error) {
	if index < log.firstIndex() || index > log.lastIndex() {
		return 0, ErrOutOfBound
	}
	index = log.toArrayIndex(index)
	return log.entries[index].Term, nil
}

func (log *Log) clone(entries []Entry) []Entry {
	cloned := make([]Entry, len(entries))
	copy(cloned, entries)
	return cloned
}

func (log *Log) slice(start, end uint64) []Entry {
	if start == end {
		// can only happen when sending a heartbeat.
		return nil
	}
	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)
	return log.clone(log.entries[start:end])
}

// TODO: rewrite by seems not using the checking.
func (log *Log) truncateSuffix(index uint64) {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return
	}

	index = log.toArrayIndex(index)
	if len(log.entries[index:]) > 0 {
		log.entries = log.entries[:index]
	}
}

func (log *Log) append(entries []Entry) {
	log.entries = append(log.entries, entries...)
}

func (log *Log) committedTo(index uint64) {
	if index > log.committed {
		log.committed = index
	}
}

func (log *Log) newCommittedEntries() []Entry {
	start := log.toArrayIndex(log.applied + 1)
	end := log.toArrayIndex(log.committed + 1)
	if start >= end {
		// note: len(nil slice) == 0.
		return nil
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) appliedTo(index uint64) {
	if index > log.applied {
		log.applied = index
	}
}

func (log *Log) compactedTo(snapshot Snapshot) {
	suffix := make([]Entry, 0)
	suffixStart := snapshot.Index + 1
	if suffixStart <= log.lastIndex() {
		suffixStart = log.toArrayIndex(suffixStart)
		suffix = log.entries[suffixStart:]
	}

	log.entries = append(make([]Entry, 1), suffix...)
	log.snapshot = snapshot
	// set the dummy entry.
	log.entries[0] = Entry{Index: snapshot.Index, Term: snapshot.Term}

	log.committedTo(log.snapshot.Index)
	log.appliedTo(log.snapshot.Index)
}

// FIXME: doubt the clone is necessary for working around races.
func (log *Log) clonedSnapshot() Snapshot {
	cloned := Snapshot{Data: make([]byte, len(log.snapshot.Data)), Index: log.snapshot.Index, Term: log.snapshot.Term}
	copy(cloned.Data, log.snapshot.Data)
	return cloned
}
