package raft

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if rf.currentTerm != args.Term {
		return
	}

	if rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist()
		return
	}

	rf.nextIndex[server] = rf.getAbsoluteLastIndex() + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.updateCommitIndex()
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		return
	} else {
		if args.LastIncludedIndex < rf.getAbsoluteLastIndex() {
			if rf.log[rf.getRelativeIndex(args.LastIncludedIndex)].Term != args.LastIncludedTerm {
				rf.log = make([]LogEntry, 0)
			} else {
				entries := make([]LogEntry, rf.getAbsoluteLastIndex()-args.LastIncludedIndex)
				copy(entries, rf.log[rf.getRelativeIndex(args.LastIncludedIndex)+1:])
				rf.log = entries
			}
		} else {
			rf.log = make([]LogEntry, 0)
		}
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), args.Data)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	rf.lastApplied = args.LastIncludedIndex
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if index <= rf.lastIncludedIndex {
		return
	}

	compactLogLen := index - rf.lastIncludedIndex

	rf.lastIncludedTerm = rf.log[rf.getRelativeIndex(index)].Term
	rf.lastIncludedIndex = index

	afterLog := make([]LogEntry, len(rf.log)-compactLogLen)
	copy(afterLog, rf.log[compactLogLen:])
	rf.log = afterLog

	rf.persister.SaveStateAndSnapshot(rf.raftStateForPersist(), snapshot)
}
