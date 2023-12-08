package raft

import (
	"math"
	"sort"
)

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if len(rf.log) == 0 {
		return false
	}

	return rf.log[rf.getRelativeLastIndex()].Term == rf.currentTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}

	rf.sendToChannel(rf.heartbeatCh, true)

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.ConflictIndex = 1
		return
	} else if args.PrevLogIndex == rf.lastIncludedIndex {
		if args.PrevLogTerm != rf.lastIncludedTerm {
			reply.ConflictIndex = 1
			return
		}
	} else {
		if args.PrevLogIndex > rf.getAbsoluteLastIndex() {
			reply.ConflictIndex = rf.getAbsoluteLastIndex() + 1
			return
		}

		if args.PrevLogTerm != rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term {
			reply.ConflictTerm = rf.log[rf.getRelativeIndex(args.PrevLogIndex)].Term
			for index := rf.lastIncludedIndex + 1; index <= args.PrevLogIndex; index++ {
				if rf.log[rf.getRelativeIndex(index)].Term == reply.ConflictTerm {
					reply.ConflictIndex = index
					break
				}
			}
			return
		}
	}

	for i, logEntry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		logPos := rf.getRelativeIndex(index)
		if index > rf.getAbsoluteLastIndex() {
			rf.log = append(rf.log, logEntry)
		} else {
			if rf.log[logPos].Term != logEntry.Term {
				rf.log = rf.log[:logPos]
				rf.log = append(rf.log, logEntry)
			}
		}
	}

	rf.persist()
	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getAbsoluteLastIndex())))
		// go rf.applyLogEntries()
		rf.applyCond.Broadcast()
	}
}

// func (rf *Raft) applyLogEntries() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
// 		if i <= rf.lastIncludedIndex {
// 			continue
// 		}

// 		relativeIndex := rf.getRelativeIndex(i)
// 		if relativeIndex >= len(rf.log) || relativeIndex < 0 {
// 			continue
// 		}

// 		rf.applyCh <- ApplyMsg{
// 			CommandValid: true,
// 			Command:      rf.log[relativeIndex].Command,
// 			CommandIndex: i,
// 		}
// 		rf.lastApplied = i
// 	}
// }

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	if reply.Success {
		rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		rf.updateCommitIndex()
	} else {
		if reply.ConflictTerm != -1 {
			conflictTermIndex := -1
			for index := args.PrevLogIndex; index > rf.lastIncludedIndex; index-- {
				if rf.log[rf.getRelativeIndex(index)].Term == reply.ConflictTerm {
					conflictTermIndex = index
					break
				}
			}
			if conflictTermIndex != -1 {
				rf.nextIndex[server] = conflictTermIndex
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := make([]int, 0)
	sortedMatchIndex = append(sortedMatchIndex, rf.getAbsoluteLastIndex())
	for i := range rf.peers {
		if i != rf.me {
			sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
		}
	}
	sort.Ints(sortedMatchIndex)
	n := sortedMatchIndex[len(sortedMatchIndex)/2]
	if n > rf.commitIndex && (n <= rf.lastIncludedIndex || rf.log[rf.getRelativeIndex(n)].Term == rf.currentTerm) {
		rf.commitIndex = n
		// go rf.applyLogEntries()
		rf.applyCond.Broadcast()
	}
}
