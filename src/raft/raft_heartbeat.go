package raft

import "fmt"

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for i := range rf.peers {
		if i != rf.me {
			if rf.nextIndex[i] <= rf.lastIncludedIndex {
				fmt.Printf("leader commit index: %d, last included index: %d\n", rf.commitIndex, rf.lastIncludedIndex)
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
			args.PrevLogIndex = rf.nextIndex[i] - 1
			if args.PrevLogIndex == rf.lastIncludedIndex {
				args.PrevLogTerm = rf.lastIncludedTerm
			} else if args.PrevLogIndex > rf.lastIncludedIndex {
				relativePrevLogIndex := rf.getRelativeIndex(args.PrevLogIndex)
				if relativePrevLogIndex >= 0 && relativePrevLogIndex < len(rf.log) {
					args.PrevLogTerm = rf.log[relativePrevLogIndex].Term
				} else {
					// Should not reach here if you're managing logs correctly. Log error or panic as needed.
					continue
				}
			} else {
				// The PrevLogIndex is before the snapshot, so skip this iteration
				continue
			}
			args.LeaderCommit = rf.commitIndex
			entriesStart := rf.getRelativeIndex(rf.nextIndex[i])
			entries := rf.log[entriesStart:]
			args.Entries = make([]LogEntry, len(entries))
			// make a deep copy of the entries to send
			copy(args.Entries, entries)
			appendEntriesReply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &appendEntriesReply)

		}
	}
}
