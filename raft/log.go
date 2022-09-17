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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pkg/errors"
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
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	nextApplied uint64

	snapshotIndex uint64
	snapshotTerm  uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	hardState, _, err := storage.InitialState()
	if err != nil {
		panic(err)
	}

	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	entries, err := storage.Entries(firstIndex, lastIndex+1)
	if err != nil {
		entries = []pb.Entry{}
	}

	var snapshotIndex uint64 = 0
	var snapshotTerm uint64 = 0
	if lastIndex > 1 {
		snapshotIndex = firstIndex - 1
		snapshotTerm, _ = storage.Term(snapshotIndex)
	}

	log.Infof("newRaftLog\tfirstIdx:%v\tlastIdx:%v\tsnapIdx:%v\tlen:%v", firstIndex, lastIndex, snapshotIndex, len(entries))

	l := &RaftLog{
		storage:       storage,
		committed:     hardState.Commit,
		applied:       snapshotIndex,
		stabled:       lastIndex,
		entries:       entries,
		snapshotTerm:  snapshotTerm,
		snapshotIndex: snapshotIndex,
	}

	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
	if l.pendingSnapshot == nil {
		panic("maybeCompact pendingSnapshot nil")
	}
	metaData := l.pendingSnapshot.Metadata
	l.snapshotIndex = metaData.Index
	l.snapshotTerm = metaData.Term

	l.committed = metaData.Index
	l.applied = metaData.Index
	l.stabled = metaData.Index

	l.entries = []pb.Entry{}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	ens, err := l.slice(l.stabled+1, l.LastIndex()+1)
	if err != nil {
		return []pb.Entry{}
	}
	return ens
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	start := max(l.applied+1, l.firstIndex())
	if l.applied < l.committed {
		ents, _ = l.slice(start, l.committed+1)
	}
	return
}

func (l *RaftLog) firstIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[0].Index
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	if i == 0 {
		return true
	}
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) == 0 {
		return l.snapshotIndex
	}
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *RaftLog) lastTerm(i uint64) uint64 {
	ret, _ := l.Term(i)
	return ret
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i == 0 {
		return 0, nil
	}

	if i == l.snapshotIndex {
		return l.snapshotTerm, nil
	}

	if len(l.entries) > 0 && i >= l.firstIndex() && i <= l.LastIndex() {
		return l.entries[i-l.firstIndex()].Term, nil
	}

	return 0, errors.New("undefined index")
}

func (l *RaftLog) append(ents []*pb.Entry) {
	for _, e := range ents {
		l.entries = append(l.entries, *e)
	}
}

func (l *RaftLog) appendOver(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}

	firstIndex := ents[0].Index
	lastIndex := ents[len(ents)-1].Index
	lastTerm := ents[len(ents)-1].Term

	if lastIndex <= l.committed || l.matchTerm(lastIndex, lastTerm) {
		return
	}

	// 去掉 ents 内 committed 之前的日志
	if firstIndex <= l.committed {
		startIdx := l.committed - firstIndex + 1
		ents = ents[startIdx:]
		firstIndex = ents[0].Index
	}

	// 去掉 l.entries 被覆盖的日志
	if firstIndex <= l.LastIndex() {
		endIndex := firstIndex - l.firstIndex()
		l.entries = l.entries[0:endIndex]
		l.stabled = min(l.stabled, l.LastIndex())
	}

	for _, e := range ents {
		l.entries = append(l.entries, *e)
	}
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	if lo > hi || lo < l.firstIndex() || hi > l.LastIndex()+1 || lo <= l.snapshotIndex {
		return []pb.Entry{}, ErrCompacted
	}
	if lo == hi {
		return []pb.Entry{}, nil
	}
	lo -= l.firstIndex()
	hi -= l.firstIndex()
	return l.entries[lo:hi], nil
}

func (l *RaftLog) slicePoi(lo, hi uint64) ([]*pb.Entry, error) {
	if lo > hi || lo < l.firstIndex() || hi > l.LastIndex()+1 || lo <= l.snapshotIndex {
		return nil, ErrCompacted
	}

	lo -= l.firstIndex()
	hi -= l.firstIndex()
	ret := make([]*pb.Entry, 0)
	for i := lo; i < hi; i++ {
		ret = append(ret, &l.entries[i])
	}
	return ret, nil
}

func (l *RaftLog) stableTo(i uint64) {
	l.stabled = i
}

func (l *RaftLog) committedTo(i uint64) {
	if i <= l.committed {
		return
	}
	l.committed = i
}

func (l *RaftLog) appliedTo(i uint64) {
	l.applied = i
}

func (l *RaftLog) stableSnapTo(i uint64) {
	if l.pendingSnapshot != nil && l.pendingSnapshot.Metadata.Index == i {
		l.pendingSnapshot = nil
	}
}

func (l *RaftLog) findConflictByTerm(index, term uint64) uint64 {
	for {
		logTerm, err := l.Term(index)
		if logTerm <= term || err != nil {
			break
		}
		index--
	}
	return index
}
