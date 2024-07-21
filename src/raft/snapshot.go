package raft

type InstallSnapshotRequest struct {
	Term              int32
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int32
	Offset            int
	Data              []byte
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
	rf.receiveHeartbeat(req.Term, term, req.LeaderId)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	snapshotEndIdx := rf.snapshotEndIndex
	if req.LastIncludedIndex <= snapshotEndIdx {
		rf.printf("the snapshot from leader %v is shorter than mine (%v vs %v), ignore",
			req.LeaderId, req.LastIncludedIndex, snapshotEndIdx)
		return
	}
	rf.snapshot = req.Data
	oldLastAppliedIdx := rf.lastAppliedIdx
	oldSnapshotEndTerm := rf.snapshotEndTerm

	rf.snapshotEndTerm = req.LastIncludedTerm
	rf.snapshotEndIndex = req.LastIncludedIndex
	if req.LastIncludedIndex >= rf.lastAppliedIdx {
		rf.lastAppliedIdx = req.LastIncludedIndex
		rf.logEntries = nil
	} else {
		// in this case, leader snapshot is longer than us, we need to trim some logEntries to match the snapshot
		count := rf.lastAppliedIdx - req.LastIncludedIndex
		rf.logEntries = rf.logEntries[len(rf.logEntries)-count:]
	}
	rf.printf("InstallSnapshot from %v, lastAppliedIdx %v => %v, snapshotEndIndex %v => %v, snapshotEndTerm %v => %v",
		req.LeaderId, oldLastAppliedIdx, rf.lastAppliedIdx,
		snapshotEndIdx, rf.snapshotEndIndex, oldSnapshotEndTerm, req.LastIncludedTerm)
	rf.persistLocked()
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
	}
	rf.mu.Unlock()
	var reply InstallSnapshotReply
	if rf.peers[peer].Call("Raft.InstallSnapshot", req, &reply) {
		if reply.Term > currTerm {
			rf.printf("current term %v, receive higher term %v from %v, change to follower", currTerm, reply.Term, peer)
			rf.works <- work{
				workType: WorkType_ToFollower,
			}
		} else {
			rf.printf("success to InstallSnapshot to %v, lastIncludedTerm %v, lastIncludeIndex %v",
				peer, req.LastIncludedTerm, req.LastIncludedIndex)
			rf.mu.Lock()
			rf.matchIndex[peer] = req.LastIncludedIndex
			rf.nextIndex[peer] = req.LastIncludedIndex + 1
			rf.mu.Unlock()
			if heatbeatChan != nil {
				heatbeatChan <- peer
			}
		}
	} else {
		rf.printf("failed to rpc InstallSnapshot to %v", peer)
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	index-- // raft is 1 based index, while internally we use 0 based index, so we need to shift
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.snapshotEndIndex {
		rf.printf("try to Snapshot %v shorter than previous snapshot %v, ignore...", index, rf.snapshotEndIndex)
		return
	}
	logIdx := index - rf.snapshotEndIndex - 1
	rf.snapshotEndTerm = rf.logEntries[logIdx].Term
	count := rf.lastAppliedIdx - index // [index+1 ~ lastApplied]
	if count <= 0 {
		rf.logEntries = nil
	} else {
		startIdx := len(rf.logEntries) - count
		rf.logEntries = rf.logEntries[startIdx:] // [startIdx ~ len of logEntries)
	}
	rf.snapshotEndIndex = index
	rf.snapshot = snapshot

	rf.persistLocked()
}
