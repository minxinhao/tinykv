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

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// RaftLog manage the log entries, its struct look like:
//
//  truncated.....first.....applied....committed....stabled.....last
//  --------|     |------------------------------------------------|
//                                  log entries
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

	// log entries with index <= stabled are stabled to storage
	// Not very understanding the meaning of stable here
	// What is the relationship between stabled and commited?
	// I thought snapshot was only aimed at logs that were already commited.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).

}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
// I think here should be some more tips for the conversion of storage and RaftLog
// mainly for the descriptions of firstIndex and LastIndex of storage can be associated with RaftLog.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	raftLog := new(RaftLog)
	raftLog.storage = storage

	firIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	raftLog.committed = firIndex - 1
	raftLog.applied = firIndex - 1

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	raftLog.stabled = lastIndex

	entries, err := storage.Entries(firIndex, lastIndex+1)
	if err != nil {
		panic(err)
	}
	raftLog.entries = entries

	return raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if l.stabled >= l.LastIndex() {
		return nil
	}
	firIndex := l.FirstIndex()

	return l.entries[l.stabled-firIndex+1:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	firIndex := max(l.applied+1, l.FirstIndex())
	if firIndex >= l.committed+1 {
		return nil
	}
	return l.entries[firIndex : l.committed+1]
}

func (l *RaftLog) FirstIndex() uint64 {
	return l.entries[0].Index
}

// LastIndex return the last index of the lon entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	return l.entries[len(l.entries)-1].Index
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i <= l.FirstIndex() {
		return l.entries[0].Term, nil
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.FirstIndex()].Term, nil
}
