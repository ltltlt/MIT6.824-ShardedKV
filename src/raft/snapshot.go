package raft

type InstallSnapshotRequest struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int32
	Offset            int
	Data              []byte

	AppendEntriesId int32
}

type InstallSnapshotReply struct {
	Term int32
}

func (rf *Raft) InstallSnapshot(req *InstallSnapshotRequest, reply *InstallSnapshotReply) {
	term := rf.currentTerm.Load()
	if term > req.Term {
		reply.Term = term
		rf.printf("Receive InstallSnapshot from %v in old term %v, my term %v, ignore",
			req.LeaderId, req.Term, term)
		return
	}

	rf.mu.Lock()
	if req.Term == term && req.AppendEntriesId < rf.receiveAppendEntriesId {
		rf.printf("receive old AppendEntries from %v for appendEntriesId %v, latest id %v, ignore...",
			req.LeaderId, req.AppendEntriesId, rf.receiveAppendEntriesId)
		rf.mu.Unlock()
		return
	}

	rf.receiveAppendEntriesId = req.AppendEntriesId

	needPersist := rf.receiveHeartbeatLocked(req.Term, term, req.LeaderId)

	defer func() {
		if needPersist {
			rf.persist()
		}
	}()

	snapshotEndIdx := rf.snapshotEndIndex
	if req.LastIncludedIndex <= snapshotEndIdx {
		rf.printf("the snapshot from leader %v is shorter than mine (%v vs %v), ignore",
			req.LeaderId, req.LastIncludedIndex, snapshotEndIdx)
		rf.mu.Unlock()
		return
	}
	rf.snapshot = req.Data
	oldLastLogIndex := len(rf.logEntries) - 1 + rf.snapshotEndIndex + 1
	oldSnapshotEndTerm := rf.snapshotEndTerm

	rf.snapshotEndTerm = req.LastIncludedTerm
	rf.snapshotEndIndex = req.LastIncludedIndex
	if req.LastIncludedIndex >= oldLastLogIndex {
		rf.logEntries = nil
	} else {
		// in this case, leader snapshot is longer than us, we need to trim some logEntries to match the snapshot
		count := oldLastLogIndex - req.LastIncludedIndex
		rf.logEntries = rf.logEntries[len(rf.logEntries)-count:]
	}
	if req.LastIncludedIndex >= int(rf.commitIdx.Load()) {
		// 2 conditions match so we can update commitIdx
		// 1. we have receive those entries
		// 2. leader must already commit those entries (snapshot only on committed entries)
		rf.commitIdx.Store(int32(req.LastIncludedIndex))
	}
	rf.printf("InstallSnapshot from %v, totalLogLen %v => %v, snapshotEndIndex %v => %v, snapshotEndTerm %v => %v",
		req.LeaderId, oldLastLogIndex+1, len(rf.logEntries)-1+rf.snapshotEndIndex+1,
		snapshotEndIdx, rf.snapshotEndIndex, oldSnapshotEndTerm, req.LastIncludedTerm)

	rf.updateApplyChSignals++
	rf.updateApplyChCond.Signal()
	rf.mu.Unlock()

	needPersist = true
}

func (rf *Raft) sendInstallSnapshotToFollower(peer int, currTerm, appendEntriesId int32, heatbeatChan chan int) {
	rf.printf("InstallSnapshot to follower %v, in term %v, id %v", peer, currTerm, appendEntriesId)
	rf.mu.Lock()
	req := &InstallSnapshotRequest{
		Term:              currTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapshotEndIndex,
		LastIncludedTerm:  rf.snapshotEndTerm,
		Offset:            0, // always send full snapshot
		Data:              rf.snapshot,
		AppendEntriesId:   appendEntriesId,
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if !rf.peers[peer].Call("Raft.InstallSnapshot", req, &reply) {
		rf.printf("failed to rpc InstallSnapshot to %v", peer)
		return
	}
	if reply.Term > currTerm {
		rf.printf("current term %v, receive higher term %v from %v, change to follower", currTerm, reply.Term, peer)
		rf.state.Store(StateFollower)
		return
	}
	rf.printf("success to InstallSnapshot to %v, lastIncludedTerm %v, lastIncludeIndex %v",
		peer, req.LastIncludedTerm, req.LastIncludedIndex)
	rf.mu.Lock()
	rf.matchIndex[peer] = req.LastIncludedIndex
	rf.nextIndex[peer] = req.LastIncludedIndex + 1
	lastTotalLogIdx := len(rf.logEntries) - 1 + rf.snapshotEndIndex + 1
	rf.mu.Unlock()
	if heatbeatChan != nil {
		heatbeatChan <- peer
	}

	if req.LastIncludedIndex < lastTotalLogIdx {
		rf.appendEntriesToFollower(peer, appendEntriesId, nil, currTerm)
	}
	rf.mu.Lock()
	rf.updateCommitIdxSignals++
	rf.updateCommitIdxCond.Signal()
	rf.mu.Unlock()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	index-- // raft is 1 based index, while internally we use 0 based index, so we need to shift
	rf.mu.Lock()
	if index <= rf.snapshotEndIndex {
		rf.printf("try to Snapshot %v shorter than previous snapshot %v, ignore...", index, rf.snapshotEndIndex)
		rf.mu.Unlock()
		return
	}
	rf.printf("create new snapshot, endIdx %v", index)
	logIdx := index - rf.snapshotEndIndex - 1
	rf.snapshotEndTerm = rf.logEntries[logIdx].Term
	lastTotalLogIdx := len(rf.logEntries) - 1 + rf.snapshotEndIndex + 1
	count := lastTotalLogIdx - index // [index+1 ~ lastTotalLogIdx]
	if count <= 0 {
		rf.logEntries = nil
	} else {
		startIdx := len(rf.logEntries) - count
		rf.logEntries = rf.logEntries[startIdx:] // [startIdx ~ len of logEntries)
	}
	rf.snapshotEndIndex = index
	rf.snapshot = snapshot

	rf.persistLocked()

	rf.updateApplyChSignals++
	rf.updateApplyChCond.Signal()
	rf.mu.Unlock()
}
