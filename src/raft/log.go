package raft

import "errors"

var ErrOutOfBound = errors.New("index out of bound")

type Entry struct {
	Index uint64
	Term  uint64
	Data  interface{}
}

// Log manages log entries, its struct look like:
//
//	     snapshot/first.....applied....committed.....last
//	-------------|--------------------------------------|
//	  compacted           persisted log entries
type Log struct {
	// compacted log entries.
	snapshotIndex uint64 // the log index of the last compacted log entry.
	snapshotTerm  uint64 // the term of the last compacted log entry.

	// persisted log entries.
	entries []Entry

	applied   uint64 // the highest log index of the log entry raft knows that the application has applied.
	committed uint64 // the highest log index of the log entry raft knows that the raft cluster has committed.

	logger *Logger
}

func makeLog() Log {
	log := Log{
		snapshotIndex: 0,
		snapshotTerm:  0,
		entries:       make([]Entry, 0),
		applied:       0,
		committed:     0,
	}

	// append a dummy log entry to simplify log operations.
	// the index and term of the dummy log entry are always synced with the snapshotIndex and snapshotTerm.
	log.entries = append(log.entries, Entry{Index: log.snapshotIndex, Term: log.snapshotTerm})

	return log
}

func (log *Log) toArrayIndex(index uint64) uint64 {
	return index - log.firstIndex()
}

// always return the snapshot index.
func (log *Log) firstIndex() uint64 {
	return log.entries[0].Index
}

// return the index of the last log entry that has not yet been compacted, if there're any.
// otherwise, return the snapshot index.
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

// slice out log entries in the range [start, end).
// if the start index is less than or equal to the first index, an error is returned.
// if the end index is greater than the last log index, it's delimited to the last log index.
// note, this is a slice by copy, not slice by clone.
func (log *Log) slice(start, end uint64) ([]Entry, error) {
	if start <= log.firstIndex() {
		return nil, ErrOutOfBound
	}

	end = min(end, log.lastIndex()+1)

	if start == end {
		return make([]Entry, 0), nil
	}

	if start > end {
		DPrintf("[start=%v, end=%v)\n", start, end)
		panic("Invalid [start, end) index pair")
	}

	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)

	return log.clone(log.entries[start:end]), nil
}

func (log *Log) truncateSuffix(index uint64) bool {
	if index <= log.firstIndex() || index > log.lastIndex() {
		return false
	}

	index = log.toArrayIndex(index)
	if len(log.entries[index:]) > 0 {
		log.entries = log.entries[:index]
		log.logger.discardEnts(log.entries[index:])
		return true
	}
	return false
}

func (log *Log) append(entries []Entry) {
	log.entries = append(log.entries, entries...)
	log.logger.appendEnts(entries)
}

func (log *Log) committedTo(index uint64) {
	oldCommitted := log.committed
	log.committed = index
	log.logger.updateCommitted(oldCommitted)
}

func (log *Log) newCommittedEntries() []Entry {
	start := log.toArrayIndex(log.applied + 1)
	end := log.toArrayIndex(log.committed + 1)
	if start >= end {
		return make([]Entry, 0)
	}
	return log.clone(log.entries[start:end])
}

func (log *Log) appliedTo(index uint64) {
	oldApplied := log.applied
	log.applied = index
	log.logger.updateApplied(oldApplied)
}

func (log *Log) compactedTo(index, term uint64) {
	log.snapshotIndex = index
	log.snapshotTerm = term
}
