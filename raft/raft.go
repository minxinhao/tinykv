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
	"fmt"
	"math/rand"
	"sort"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in raft thesis 3.10.
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	raftLog := newLog(c.Storage)
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	raft := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	peers := c.peers
	if len(confState.Nodes) > 0 {
		if len(peers) > 0 {
			panic(errors.New("Both config.peers and confState.Nodes are specified"))
		}
		peers = confState.Nodes
	}
	for _, p := range peers {
		raft.Prs[p] = &Progress{Next: 1}
	}

	if !IsEmptyHardState(hardState) {
		if hardState.Commit < raft.RaftLog.committed || hardState.Commit > raft.RaftLog.LastIndex() {
			panic(errors.New("Invalid hardState"))
		}
		raft.Term = hardState.GetTerm()
		raft.Vote = hardState.GetVote()
		raft.RaftLog.committed = hardState.GetCommit()
	}

	if c.Applied > 0 {
		raft.RaftLog.Applied(c.Applied)
	}

	raft.becomeFollower(raft.Term, None)

	return raft
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	progress := r.Prs[to]
	message := pb.Message{
		To:   to,
		From: r.id,
	}

	term, err_term := r.RaftLog.Term(progress.Next - 1)

	var entries []pb.Entry
	var err_entries error
	if progress.Next < r.RaftLog.FirstIndex() {
		// fmt.Println("sendAppend case 1")
		entries, err_entries = nil, ErrCompacted
	} else if progress.Next > r.RaftLog.LastIndex() {
		// fmt.Println("sendAppend case 2 LastIndex ", r.RaftLog.LastIndex(), " Next ", progress.Next)
		entries, err_entries = nil, nil
	} else {
		entries, err_entries = r.RaftLog.entries[progress.Next-r.RaftLog.snapLastIndex-1:], nil
	}

	// progress.Next - 1 < snapLastIndex || progress.Next - 1 > r.RaftLog.LastIndex()
	if err_term != nil || err_entries != nil {
		message.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.snapshot()
		if err != nil {
			panic(err)
		}
		if IsEmptySnap(&snapshot) {
			panic(errors.New("Empty SnapShot"))
		}
		message.Snapshot = &snapshot
	} else {
		message.MsgType = pb.MessageType_MsgAppend
		message.Index = progress.Next - 1
		message.LogTerm = term

		message_entries := make([]*pb.Entry, 0, len(entries))
		for i := range entries {
			message_entries = append(message_entries, &entries[i])
		}
		message.Entries = message_entries
		message.Commit = r.RaftLog.committed
	}

	r.send(message)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	message := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		Commit:  commit,
	}

	r.send(message)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed++
		// I failed test TestFollowerStartElection2AA without this
		// why leader need to maintain electionElapsed
		r.electionElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			r.Step(pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgBeat})
		}
	} else {
		r.electionElapsed++
		if _, ok := r.Prs[r.id]; ok {
			if r.electionElapsed >= r.randomizedElectionTimeout {
				r.electionElapsed = 0
				r.Step(pb.Message{From: r.id, Term: r.Term, MsgType: pb.MessageType_MsgHup})
			}
		}
	}
}

func (r *Raft) fresh() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.Lead = None
	r.leadTransferee = None
	r.PendingConfIndex = 0

	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	v := rand.Intn(r.electionTimeout)
	r.randomizedElectionTimeout = r.electionTimeout + v

	r.votes = make(map[uint64]bool)
	for id, progress := range r.Prs {
		*progress = Progress{Next: r.RaftLog.LastIndex() + 1}
		if id == r.id {
			progress.Match = r.RaftLog.LastIndex()
		}
	}

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.fresh()
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.fresh()
	r.Term += 1
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.fresh()
	r.Lead = r.id
	r.State = StateLeader
	r.PendingConfIndex = r.RaftLog.LastIndex()
	emptyEnt := pb.Entry{Data: nil}
	r.AppendEntry(emptyEnt)
}

func (r *Raft) AppendEntry(es ...pb.Entry) {
	lastIndex := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = lastIndex + 1 + uint64(i)
	}
	lastIndex = r.RaftLog.AppendEntries(es)
	progress := r.Prs[r.id]
	if progress.Match < lastIndex {
		progress.Match = lastIndex
	}
	if progress.Next < lastIndex+1 {
		progress.Next = lastIndex + 1
	}

	matchIndexs := make(uint64Slice, len(r.Prs))
	index := 0
	for _, p := range r.Prs {
		matchIndexs[index] = p.Match
		index++
	}
	sort.Sort(matchIndexs)
	max_consist_index := matchIndexs[len(matchIndexs)-r.VoteThreshold()]
	r.RaftLog.Commit(max_consist_index, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// Rules for all servers
	if m.Term < r.Term && m.MsgType != pb.MessageType_MsgHup && m.MsgType != pb.MessageType_MsgPropose && m.MsgType != pb.MessageType_MsgBeat {
		return nil
	}
	if m.Term == r.Term && m.MsgType == pb.MessageType_MsgAppend && r.Vote == m.From {
		r.Lead = m.From
	}
	// if m.Term > r.Term && r.Lead == None {
	if m.Term > r.Term {
		if m.MsgType == pb.MessageType_MsgAppend || m.MsgType == pb.MessageType_MsgHeartbeat || m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}
	switch r.State {
	case StateFollower:
		if err := r.FollowerStep(m); err != nil {
			return err
		}
	case StateCandidate:
		if err := r.CanditateStep(m); err != nil {
			return err
		}
	case StateLeader:
		if err := r.LeaderStep(m); err != nil {
			return err
		}
	}
	return nil
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All campaign messages need to have the term set when sending.
			// - MessageType_MsgRequestVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MessageType_MsgRequestVoteResponse: m.Term is the new r.Term if the MessageType_MsgRequestVote was
			//   granted, non-zero for the same reason MessageType_MsgRequestVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MessageType_MsgPropose
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) VoteThreshold() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) StepMsgHup() {
	// entries, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1)
	// num_Conf := 0
	// for i := range entries {
	// 	if entries[i].EntryType == pb.EntryType_EntryConfChange {
	// 		num_Conf++
	// 	}
	// }
	// if num_Conf != 0 && r.RaftLog.committed > r.RaftLog.applied {
	// 	return
	// }

	r.becomeCandidate()
	index := r.RaftLog.LastIndex()
	logTerm, err := r.RaftLog.Term(index)
	if err != nil {
		panic(err)
	}

	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			Term:    r.Term,
			To:      id,
			From:    r.id,
			MsgType: pb.MessageType_MsgRequestVote,
			Index:   index,
			LogTerm: logTerm})
	}
	votesCount := 0
	r.votes[r.id] = true
	for _, vote := range r.votes {
		if vote {
			votesCount++
		}
	}
	if r.VoteThreshold() <= votesCount {
		r.becomeLeader()
	}
}

func (r *Raft) LeaderStep(m pb.Message) error {
	progress := r.Prs[m.From]
	if progress == nil && m.MsgType != pb.MessageType_MsgBeat && m.MsgType != pb.MessageType_MsgPropose && m.MsgType != pb.MessageType_MsgRequestVote {
		return nil
	}
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		for id, _ := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendHeartbeat(id)
		}
	case pb.MessageType_MsgPropose:
		// fmt.Println("Leader step MessageType_MsgPropose")
		if len(m.Entries) == 0 {
			panic("Invalid propose message with empty entries")
		}
		if _, ok := r.Prs[r.id]; !ok {
			return ErrProposalDropped
		}

		entries := make([]pb.Entry, 0, len(m.Entries))
		for _, entry := range m.Entries {
			entries = append(entries, *entry)
		}

		r.AppendEntry(entries...)
		for id, _ := range r.Prs {
			if id == r.id {
				continue
			}
			r.sendAppend(id)
		}
		return nil
	case pb.MessageType_MsgAppendResponse:
		if m.Reject {
			rejectFlag := true
			if m.Index <= progress.Match {
				rejectFlag = false
			}
			// m.RejectHint maybe 0 if follower has  empty entry and  snapshot
			if progress.Next = min(m.Index, m.RejectHint+1); progress.Next < 1 {
				progress.Next = 1
			}
			if rejectFlag {
				r.sendAppend(m.From)
			}
		} else {
			updatedFlag := false
			if progress.Match < m.Index {
				progress.Match = m.Index
				updatedFlag = true
			}
			if progress.Next < m.Index+1 {
				progress.Next = m.Index + 1
			}
			if updatedFlag {
				matchIndexs := make(uint64Slice, len(r.Prs))
				id := 0
				for _, p := range r.Prs {
					matchIndexs[id] = p.Match
					id++
				}
				sort.Sort(matchIndexs)
				max_consist_index := matchIndexs[len(matchIndexs)-r.VoteThreshold()]
				commitFlag := r.RaftLog.Commit(max_consist_index, r.Term)
				// fmt.Println("max_consist_index ", max_consist_index, " matchIndex ", matchIndexs)

				if commitFlag {
					for id, _ := range r.Prs {
						if id == r.id {
							continue
						}
						r.sendAppend(id)
					}
				}
			}
		}
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	case pb.MessageType_MsgHeartbeatResponse:
		if progress.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	case pb.MessageType_MsgTransferLeader:
	}
	return nil
}

func (r *Raft) CanditateStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.StepMsgHup()
	case pb.MessageType_MsgPropose:
		// return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		// When it enters here,m.Term==r.Term
		// There is a valid leader occur
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
	case pb.MessageType_MsgRequestVoteResponse:
		r.votes[m.From] = !m.Reject
		votesCount := 0
		for _, vote := range r.votes {
			if vote {
				votesCount++
			}
		}
		// fmt.Println("Candidate gets votes ", votesCount)
		if votesCount == r.VoteThreshold() {
			r.becomeLeader()
			//broadcast
			for id, _ := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendAppend(id)
			}
		} else if votesCount+r.VoteThreshold() == len(r.votes) {
			// Election failed,mostly peers rejected this node
			r.becomeFollower(r.Term, None)
		}
	case pb.MessageType_MsgTimeoutNow:

	}
	return nil
}

func (r *Raft) FollowerStep(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.StepMsgHup()
	case pb.MessageType_MsgPropose:
		// return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		if m.Term < r.Term {
			//Invalid Leader
			r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Term: r.Term, Reject: true})
			return nil
		}
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgRequestVote:
		voteFlag := (r.Vote == None && r.Lead == None) || (r.Vote == m.From)
		updateFlag := (m.LogTerm > r.RaftLog.LastTerm()) ||
			(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())
		if voteFlag && updateFlag {
			r.electionElapsed = 0
			r.Vote = m.From
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse})
		} else {
			r.send(pb.Message{To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: true})
		}
	// case pb.MessageType_MsgTransferLeader:
	// 	if r.Lead == None {
	// 		return nil
	// 	}
	// 	m.To = r.Lead
	// 	r.send(m)
	case pb.MessageType_MsgTimeoutNow:
		if _, ok := r.Prs[r.id]; ok {
			r.StepMsgHup()
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	entries := make([]pb.Entry, 0, len(m.Entries))
	for _, ent := range m.Entries {
		entries = append(entries, *ent)
	}
	// fmt.Println("handleAppendEntries Index", m.Index, "LogTerm ", m.LogTerm)
	if lastIndex, ok := r.RaftLog.Append(m.Index, m.LogTerm, m.Commit, entries...); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: lastIndex})
	} else {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: m.Index, Reject: true, RejectHint: r.RaftLog.LastIndex()})
	}

}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	if r.RaftLog.committed < m.Commit {
		if r.RaftLog.LastIndex() < m.Commit {
			panic(errors.New("Lost committed entries"))
		}
		r.RaftLog.committed = m.Commit
	}
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) SoftState() *SoftState {
	return &SoftState{Lead: r.Lead, RaftState: r.State}
}

func (r *Raft) HardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}
