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
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
	"strconv"
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
	votes         map[uint64]bool
	votesGranted  int
	votesRejected int

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
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
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

	logger *Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hardState, confState, _ := c.Storage.InitialState()

	r := &Raft{
		id:               c.ID,
		Term:             hardState.Term,
		Vote:             hardState.Vote,
		RaftLog:          newLog(c.Storage),
		State:            StateFollower,
		Prs:              map[uint64]*Progress{},
		votes:            map[uint64]bool{},
		msgs:             []pb.Message{},
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		leadTransferee:   None,
		PendingConfIndex: None,
		logger:           NewLogger(c.ID),
	}

	r.resetRandomizedElectionTimeout()

	for _, id := range c.peers {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}

	if len(c.peers) == 0 {
		for _, id := range confState.Nodes {
			r.Prs[id] = &Progress{
				Match: 0,
				Next:  r.RaftLog.LastIndex() + 1,
			}
		}
	}

	r.checkPendingConfIndex()

	r.logger.Infof("new raft lastIndex:%v\tsnapshotIndex:%v", r.RaftLog.LastIndex(), r.RaftLog.snapshotIndex)

	return r
}

func (r *Raft) checkPendingConfIndex() {
	r.PendingConfIndex = None
	ent, err := r.RaftLog.slice(max(r.RaftLog.firstIndex(), r.RaftLog.applied+1), r.RaftLog.LastIndex()+1)
	if err == nil {
		for _, e := range ent {
			if e.EntryType == pb.EntryType_EntryConfChange {
				r.PendingConfIndex = e.Index
				break
			}
		}
	}
}

func (r *Raft) sortState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

func (r *Raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + rand.Intn(r.electionTimeout)
}

func (r *Raft) reset(term, lead uint64) {
	if term > r.Term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = lead
	r.logger.setStatus(r.id, r.Term, r.Lead, r.State)
}

func (r *Raft) appendEntry(es []*pb.Entry) {
	r.logger.Debugf("appendEntry len:%v", len(es))

	li := r.RaftLog.LastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 + uint64(i)
	}
	r.RaftLog.append(es)
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.Prs[r.id].Match + 1

	// 单节点情况下直接可以应用
	if len(r.Prs) == 1 {
		r.RaftLog.committedTo(r.RaftLog.LastIndex())
	}

	// 向其他节点发送消息
	for i := range r.Prs {
		if i == r.id {
			continue
		}
		r.sendAppend(i)
	}
}

func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	m.Term = r.Term

	r.logger.msgInfo(m)
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	msg := pb.Message{To: to}

	nextIndex := r.Prs[to].Next
	preLogIndex := r.Prs[to].Next - 1
	if nextIndex <= r.RaftLog.snapshotIndex {

		msg.MsgType = pb.MessageType_MsgSnapshot
		snapshot, err := r.RaftLog.storage.Snapshot()
		if err != nil {
			return false
		}
		msg.Snapshot = &snapshot
		log.Infof("Raft:%v\tsend Snapshot Metadata: %v\tTo:%v", r.id, msg.Snapshot.Metadata.String(), to)
	} else {

		msg.MsgType = pb.MessageType_MsgAppend

		if preLogIndex == 0 {
			msg.LogTerm = r.Term
		} else {
			term, err := r.RaftLog.Term(preLogIndex)
			if err == nil {
				msg.LogTerm = term
			} else {
				return false
			}
		}

		msg.Index = preLogIndex
		ens, err := r.RaftLog.slicePoi(nextIndex, r.RaftLog.LastIndex()+1)
		if err != nil {
			return false
		}
		msg.Entries = ens

		if len(msg.Entries) == 0 && msg.Index == 0 {
			return false
		}

		var msgLastIdx uint64
		if len(msg.Entries) > 0 {
			msgLastIdx = msg.Entries[len(msg.Entries)-1].Index
		}
		msg.Commit = min(r.RaftLog.committed, max(r.Prs[to].Match, msgLastIdx))

		log.Infof("Raft:%v\tsend Append\tIdx:%v\tTerm:%v\tLen:%v\tCommIdx:%v", r.id, msg.Index, msg.LogTerm, len(msg.Entries), msg.Commit)
	}

	r.send(msg)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.logger.DebugfX("sendHeartbeat: to %v", to)
	commit := min(r.RaftLog.committed, r.Prs[to].Match)
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  commit,
	})
}

func (r *Raft) sendTransferLeader(to uint64) {
	//r.logger.Infof("sendTransferLeader", to)
	p, ok := r.Prs[to]
	if !ok {
		r.leadTransferee = None
		return
	}
	if p.Match != r.RaftLog.LastIndex() {
		r.sendAppend(to)
		return
	}
	r.send(pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
	})
}

func (r *Raft) hup() {
	if _, ok := r.Prs[r.id]; !ok {
		return
	}

	r.becomeCandidate()

	// 单节点状态下选举
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}

	// 发送选举信息
	logTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())

	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.send(pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			To:      id,
			From:    r.id,
			Term:    r.Term,
			LogTerm: logTerm,
			Index:   r.RaftLog.LastIndex(),
		})
	}
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A)
	switch r.State {
	case StateLeader:
		r.heartbeatElapsed++
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat}); err != nil {
				r.logger.Panic("tick MessageType_MsgBeat err")
			}
		}
	default:
		r.electionElapsed++
		if r.electionElapsed >= r.randomizedElectionTimeout {
			r.electionElapsed = 0
			if err := r.Step(pb.Message{MsgType: pb.MessageType_MsgHup}); err != nil {
				r.logger.Panic("tick MessageType_MsgHup err")
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).

	if r.State != StateFollower {
		r.logger.Info("becomeFollower")
	}

	r.State = StateFollower
	r.Lead = lead

	r.leadTransferee = None
	r.PendingConfIndex = None

	r.reset(term, lead)

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.State = StateCandidate
	r.reset(r.Term+1, None)

	r.resetRandomizedElectionTimeout()
	r.Vote = r.id

	r.votes = map[uint64]bool{}

	r.votes[r.id] = true
	r.votesGranted = 1
	r.votesRejected = 0

	peers := ""
	for id := range r.Prs {
		peers += strconv.FormatUint(id, 10) + " "
	}

	log.Infof("Raft:%v\tbecomeCandidate\tPeers:%v", r.id, peers)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if _, ok := r.Prs[r.id]; !ok {
		log.Infof("becomeLeader Prs 内不存在 %v", r.id)
		r.becomeFollower(r.Term, None)
		return
	}

	r.leadTransferee = None
	r.State = StateLeader
	r.reset(r.Term, r.id)

	for i := range r.Prs {
		r.Prs[i].Match = 0
		r.Prs[i].Next = r.RaftLog.LastIndex() + 1
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()

	// no-op 日志
	r.appendEntry([]*pb.Entry{&pb.Entry{}})

	r.checkPendingConfIndex()

	peers := ""
	for id := range r.Prs {
		peers += strconv.FormatUint(id, 10) + " "
	}
	log.Infof("Raft:%v\tbecomeLeader\tTerm:%v\tent(%v:%v)\tPeers:%v", r.id, r.Term, r.RaftLog.LastIndex(), r.RaftLog.LastIndex(), peers)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
	case m.Term > r.Term:
		if m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	case m.Term == r.Term:
		if m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		}
	case m.Term < r.Term:
		// 回复让 Leader 下线
		r.send(pb.Message{MsgType: pb.MessageType_MsgHeartbeatResponse, To: m.From})
		return nil
	}

	// RPC 消息
	switch r.State {
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResp(m)
		case pb.MessageType_MsgHeartbeatResponse:
			if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
				r.sendAppend(m.From)
			}
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResp(m)
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		}
	default:
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.handleVote(m)
		case pb.MessageType_MsgAppend:
			r.handleAppendEntries(m)
		case pb.MessageType_MsgSnapshot:
			r.handleSnapshot(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgTimeoutNow:
			r.hup()
		}
	}

	// 内部消息
	switch r.State {
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgBeat:
			for id, _ := range r.Prs {
				if id == r.id {
					continue
				}
				r.sendHeartbeat(id)
			}
		case pb.MessageType_MsgPropose:
			if r.leadTransferee != None {
				return ErrProposalDropped
			}
			r.appendEntry(m.Entries)
		case pb.MessageType_MsgTransferLeader:
			if m.From == r.id {
				return nil
			}
			r.leadTransferee = m.From
			r.sendTransferLeader(m.From)
		}
	default:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.hup()
		case pb.MessageType_MsgTransferLeader:
			r.hup()
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From}
	if m.Index < r.RaftLog.snapshotIndex || r.RaftLog.matchTerm(m.Index, m.LogTerm) {
		r.RaftLog.appendOver(m.Entries)
		msg.Index = r.RaftLog.LastIndex()

		committedIdx := min(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.committedTo(committedIdx)

		log.Infof("Raft:%v\thandleAppendEntries r(Idx:%v, T:%v, Len:%v) s(Idx:%v) cuurCom:%v", r.id,
			m.Index, m.LogTerm, len(m.Entries), r.RaftLog.LastIndex(), r.RaftLog.committed)

	} else {
		msg.Reject = true
		index := min(m.Index, r.RaftLog.LastIndex())
		index = r.RaftLog.findConflictByTerm(index, m.LogTerm)
		msg.Index = index
		msg.LogTerm, _ = r.RaftLog.Term(index)

		t, _ := r.RaftLog.Term(m.Index)
		log.Infof(
			"Raft:%v\thandleAppendEntries Reject: r(Idx:%v, T:%v : cT:%v) s(Idx:%v, T:%v)", r.id,
			m.Index, m.LogTerm, t, msg.Index, msg.LogTerm)
	}

	r.send(msg)
}

func (r *Raft) handleAppendResp(m pb.Message) {
	r.logger.Infof("handleAppendResp\tfrom:%v", m.From)
	prs, ok := r.Prs[m.From]
	if !ok {
		return
	}
	if m.Reject {
		nextIdx := m.Index + 1

		if nextIdx != 0 && nextIdx <= prs.Match {
			return
		}

		if m.LogTerm > 0 {
			nextIdx = r.RaftLog.findConflictByTerm(m.Index, m.LogTerm)
		}

		prs.Next = max(1, nextIdx)
		r.sendAppend(m.From)
	} else {
		// 判断是否为已删除节点的过期信息

		log.Infof("Raft:%v\thandleAppendResp from:%v,  idx:%v", r.id, m.From, m.Index)

		prs.Next = max(prs.Next, m.Index+1)
		prs.Match = max(prs.Match, m.Index)

		if r.maybeCommit() {
			for i := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendAppend(i)
			}
		} else if r.Prs[m.From].Next <= r.RaftLog.LastIndex() {
			if _, ok := r.Prs[m.From]; ok {
				r.sendAppend(m.From)
			}
		}

		// 日志满足转移 Leader 的条件，开始转移
		if r.leadTransferee != None && r.leadTransferee == m.From && r.Prs[m.From].Match == r.RaftLog.LastIndex() {
			r.sendTransferLeader(r.leadTransferee)
		}
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.electionElapsed = 0
	if m.Commit != util.RaftInvalidIndex {
		r.RaftLog.committedTo(min(m.Commit, r.RaftLog.LastIndex()))
	}

	r.send(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      r.Lead,
		From:    r.id,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	msg := pb.Message{MsgType: pb.MessageType_MsgAppendResponse, To: m.From}
	metaData := m.Snapshot.Metadata

	if r.RaftLog.LastIndex() < metaData.Index || !r.RaftLog.matchTerm(metaData.Index, metaData.Term) {
		r.RaftLog.pendingSnapshot = m.Snapshot
		r.RaftLog.maybeCompact()

		r.Prs = map[uint64]*Progress{}
		for _, id := range metaData.ConfState.Nodes {
			r.Prs[id] = &Progress{}
		}

		log.Infof("Raft:%v\tApplySnapshot metaData:%v\tcurrLastIdx:%v", r.id, metaData.String(), r.RaftLog.snapshotIndex)
	}

	msg.Index = r.RaftLog.LastIndex()

	log.Infof("Raft:%v\thandleSnapshot r(Idx:%v, T:%v) s(Idx:%v)", r.id,
		metaData.Index, metaData.Term, r.RaftLog.LastIndex())

	r.send(msg)
}

func (r *Raft) handleVote(m pb.Message) {
	r.logger.DebugfX("handleVote from:%v", m.From)

	isVote := false
	msg := pb.Message{MsgType: pb.MessageType_MsgRequestVoteResponse, From: r.id, To: m.From, Term: r.Term}
	if r.Vote == m.From || r.Vote == None {
		currLogTerm, _ := r.RaftLog.Term(r.RaftLog.LastIndex())
		isVote = m.LogTerm > currLogTerm || (m.LogTerm == currLogTerm && m.Index >= r.RaftLog.LastIndex())
		if isVote {
			r.Vote = m.From
			r.logger.Debugf("投票至 %v", r.Vote)
		}

		log.Infof("Raft:%v\tVote\tr(id:%v, Term:%v, Idx:%v, logTerm:%v)\tcurrLast(Idx:%v, Term:%v)\tisVote:%v", r.id, m.From, m.Term, m.Index, m.LogTerm,
			r.RaftLog.LastIndex(), currLogTerm, isVote)
	}

	msg.Reject = !isVote
	r.send(msg)
}

func (r *Raft) handleVoteResp(m pb.Message) {
	if _, ok := r.votes[m.From]; m.Term == r.Term && !ok {
		r.votes[m.From] = !m.Reject
		if m.Reject {
			r.votesRejected++
		} else {
			r.votesGranted++
		}

		if !m.Reject {
			r.logger.Infof("收到 %v 的选举同意票", m.From)
		} else {
			r.logger.DebugfX("收到 %v 的选举回复", m.From)
		}

		quorum := len(r.Prs) / 2
		if r.votesGranted > quorum {
			r.becomeLeader()
			// 开始日志同步
			for i := range r.Prs {
				if i == r.id {
					continue
				}
				r.sendAppend(i)
			}
		} else if r.votesRejected > quorum {
			r.becomeFollower(r.Term, None)
		}
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	r.Prs[id] = &Progress{
		Match: util.RaftInvalidIndex,
		Next:  r.RaftLog.LastIndex() + 1,
	}
	r.PendingConfIndex = None
	_ = r.Step(pb.Message{MsgType: pb.MessageType_MsgBeat})
	r.logger.Infof("Raft add Node:%v\tpeer num:%v", id, len(r.Prs))
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	delete(r.Prs, id)
	delete(r.votes, id)
	r.PendingConfIndex = None
	r.logger.Infof("Raft Remove Node:%v\tpeer num:%v", id, len(r.Prs))
	r.maybeCommit()
}

func (r *Raft) advance(rd Ready) {
	if len(rd.CommittedEntries) > 0 {
		e := rd.CommittedEntries[len(rd.CommittedEntries)-1]
		r.RaftLog.appliedTo(e.Index)
	}

	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.RaftLog.stableTo(e.Index)
	}

	if !IsEmptySnap(&rd.Snapshot) {
		r.RaftLog.stableSnapTo(rd.Snapshot.Metadata.Index)
	}
}

func (r *Raft) maybeCommit() bool {
	match := make([]uint64, 0)
	for _, p := range r.Prs {
		match = append(match, p.Match)
	}

	if len(match) == 0 {
		return false
	}

	sort.Slice(match, func(i, j int) bool {
		return match[i] > match[j]
	})
	newCommit := match[len(match)/2]
	if newCommit != 0 && r.RaftLog.matchTerm(newCommit, r.Term) && newCommit > r.RaftLog.committed {
		r.logger.Infof("maybeCommit update committedTo %v", newCommit)

		t, _ := r.RaftLog.Term(newCommit)
		log.Infof("Raft:%v\tupdate committedTo %v:%v", r.id, newCommit, t)

		r.RaftLog.committedTo(newCommit)
		return true
	}
	return false
}
