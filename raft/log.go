// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	snapLastTerm  uint64
	snapLastIndex uint64
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// I think here should be some more tips for the conversion of storage and RaftLog
// mainly for the descriptions of firstIndex and LastIndex of storage can be associated with RaftLog.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic(errors.New("storage cannot be nil"))
	}
	raftLog := &RaftLog{
		storage: storage,
	}
	// fmt.Println("fine newLog")
	firIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	raftLog.committed = firIndex - 1
	raftLog.applied = firIndex - 1
	// fmt.Println("fine newLog")

	snapLastTerm, err := storage.Term(firIndex - 1)
	if err != nil {
		panic(err)
	}
	raftLog.snapLastTerm = snapLastTerm
	raftLog.snapLastIndex = firIndex - 1
	// fmt.Println("fine newLog")

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	raftLog.stabled = lastIndex
	// fmt.Println("fine newLog")

	entries, err := storage.Entries(firIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	raftLog.entries = entries
	// fmt.Println("fine newLog")

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	var firstEntryIndex, firstEntryTerm uint64
	var err error
	for {
		firstEntryIndex, err = l.storage.FirstIndex()
		if err != nil {
			panic(err)
		}
		firstEntryTerm, err = l.storage.Term(firstEntryIndex - 1)
		if err == ErrCompacted {
			if i, _ := l.storage.FirstIndex(); i != firstEntryIndex {
				continue
			}
		}
		if err != nil {
			panic(err)
		} else {
			break
		}
	}
	numCompact := firstEntryIndex - l.snapLastIndex - 1
	if numCompact > 0 && numCompact < uint64(len(l.entries)) {
		l.entries = l.entries[numCompact:]
		l.snapLastIndex = firstEntryIndex - 1
		l.snapLastTerm = firstEntryTerm
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if int(l.stabled-l.snapLastIndex) > len(l.entries) {
		return nil
	}

	return l.entries[l.stabled-l.snapLastIndex:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firIndex := max(l.applied+1, l.FirstIndex())
	if firIndex >= l.committed+1 || firIndex < l.FirstIndex() || l.committed > l.LastIndex() || len(l.entries) == 0 {
		return nil
	}
	return l.entries[firIndex-l.snapLastIndex-1 : l.committed+1-l.snapLastIndex-1]
}

func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) != 0 {
		return l.entries[0].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.snapLastIndex
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		return l.entries[len(l.entries)-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.snapLastIndex
}

func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).

	if i == l.snapLastIndex {
		return l.snapLastTerm, nil
	}

	if i <= l.snapLastIndex {
		return 0, ErrCompacted
	}
	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}

	if i > l.snapLastIndex+uint64(len(l.entries)) {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.snapLastIndex-1].Term, nil
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if len(l.entries) == 0 {
		return nil, nil
	}
	if lo > hi {
		panic(errors.New("Lo exceeds Hi"))
	}
	fi := l.FirstIndex()
	if lo < fi {
		return nil, ErrCompacted
	}

	if hi > l.LastIndex()+1 {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}

	return l.entries[lo-l.snapLastIndex-1 : hi-l.snapLastIndex-1], nil
}

func (l *RaftLog) matchForTerm(index, term uint64) bool {
	matchFlag := false
	if logTerm, err := l.Term(index); err == nil {
		matchFlag = (logTerm == term)
	}
	return matchFlag
}

func (l *RaftLog) greaterForTerm(index, term uint64) bool {
	greaterFlag := false
	if logTerm, err := l.Term(index); err == nil {
		greaterFlag = (logTerm < term)
	} else {
		greaterFlag = false
	}
	return greaterFlag
}

func (l *RaftLog) matchEntries(ents []pb.Entry) uint64 {
	for _, entry := range ents {
		if !l.matchForTerm(entry.Index, entry.Term) {
			return entry.Index
		}
	}
	return 0
}

func (l *RaftLog) AppendEntries(ents []pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.LastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		panic(errors.New("Appending entries that have been commiteted!"))
	}
	after := ents[0].Index
	if after == l.LastIndex()+1 {
		// fmt.Println("Append Entries consectively")
		l.entries = append(l.entries, ents...)
		return l.LastIndex()
	}
	if after > l.LastIndex()+1 {
		// fmt.Println("AppendEntries get entries jump over some index from ", l.LastIndex(), "to ", after)
		panic("raftLog AppendEntries cause lack of entry")
	}
	// fmt.Println("Append Entries replaced prev entries")

	if after-1 < l.stabled {
		l.stabled = after - 1
	}
	l.entries = append([]pb.Entry{}, l.entries[:after-l.snapLastIndex-1]...)
	l.entries = append(l.entries, ents...)
	l.maybeCompact()
	return l.LastIndex()
}

func (l *RaftLog) Append(index, term, committed uint64, entries ...pb.Entry) (last_index uint64, ok bool) {
	if l.matchForTerm(index, term) {
		last_index = index + uint64(len(entries))
		confilict_index := l.matchEntries(entries)
		// fmt.Println("confilict_index ", confilict_index, " l.committed ", l.committed)
		switch {
		case confilict_index == 0:

		case confilict_index <= l.committed:
		default:
			offset := index + 1
			l.AppendEntries(entries[confilict_index-offset:])
		}
		commit_index := min(committed, last_index)
		if l.committed < commit_index {
			if l.LastIndex() < commit_index {
				// fmt.Println("commit_index ", commit_index, "excceed LastIndex ", l.LastIndex())
				panic("commit_index excceed LastIndex ")
			}
			l.committed = commit_index
		}
		return last_index, true
	}
	return 0, false
}

func (l *RaftLog) Commit(index, term uint64) bool {
	if index > l.committed && l.matchForTerm(index, term) {
		if l.LastIndex() < index {
			panic(errors.New("Commit unexist entry"))
		}
		l.committed = index
		return true
	}
	return false
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			panic(errors.New("Non-Consective CommitIndex"))
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) Applied(index uint64) {
	if index == 0 {
		return
	}
	if l.committed < index || index < l.applied {
		panic(errors.New("Apply invalid entry"))
	}
	l.applied = index
}

func (l *RaftLog) Stabled(index, term uint64) {
	if l.matchForTerm(index, term) && l.stabled < index {
		l.stabled = index
	}
}

func (l *RaftLog) StableSnap(index uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == index {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) restore(s pb.Snapshot) {
	l.committed = s.Metadata.Index
	l.entries = nil
	l.stabled = s.Metadata.Index
	l.snapLastIndex = s.Metadata.Index
	l.snapLastTerm = s.Metadata.Term
	l.pendingSnapshot = &s
}

func (l *RaftLog) Entries(i uint64) ([]pb.Entry, error) {
	if i < l.FirstIndex() {
		return nil, ErrCompacted
	}
	if i > l.LastIndex() {
		return nil, nil
	}
	return l.entries[i-l.snapLastIndex-1:], nil
}
