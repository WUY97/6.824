package raft

func (rf *Raft) broadcastAppendEntries(isHeartbeat bool) {
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if !isHeartbeat && rf.nextIndex[i] < rf.getAbsoluteLastIndex() {
			continue
		}

		if rf.nextIndex[i] <= rf.lastIncludedIndex {
			snapshotArgs := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			snapshotReply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(i, &snapshotArgs, &snapshotReply)
			continue
		}

		args := AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[i] - 1

		if args.PrevLogIndex == rf.lastIncludedIndex {
			args.PrevLogTerm = rf.lastIncludedTerm
		} else {
			args.PrevLogTerm = rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term
		}

		args.Entries = make([]LogEntry, 0)
		args.Entries = append(args.Entries, rf.log[rf.getRelativeIndex(args.PrevLogIndex+1):]...)

		appendEntriesReply := AppendEntriesReply{}
		go rf.sendAppendEntries(i, &args, &appendEntriesReply)
	}
}
