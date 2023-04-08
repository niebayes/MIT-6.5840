package raft

import "errors"

var ErrOutOfBound = errors.New("Index out of bound")

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

// slice out log entries in the range [start, end).
// if the start index is less than or equal to the first index, an error is returned.
// if the end index is greater than the last log index, it's delimited to the last log index.
// note, this is a slice by copy, not slice by clone.
func (log *Log) slice(start, end uint64) ([]Entry, error) {
	if start <= log.firstIndex() {
		return nil, ErrOutOfBound
	}

	end = min(end, log.lastIndex())

	if start == end {
		return make([]Entry, 0), nil
	}

	if start > end {
		panic("Invalid [start, end) index pair")
	}

	start = log.toArrayIndex(start)
	end = log.toArrayIndex(end)

	return log.entries[start:end], nil
}

func (log *Log) committedTo(index uint64) {
	log.committed = index
}

func (log *Log) appliedTo(index uint64) {
	log.applied = index
}

func (log *Log) compactedTo(index uint64) {
}
