package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) getRelativeLastIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getAbsoluteLastIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getRelativeIndex(absoluteIndex int) int {
	return absoluteIndex - rf.lastIncludedIndex
}

func (rf *Raft) getAbsoluteIndex(relativeIndex int) int {
	return relativeIndex + rf.lastIncludedIndex
}

func (rf *Raft) getLastTerm() int {
	return rf.log[rf.getRelativeLastIndex()].Term
}

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if lastLogTerm == rf.getLastTerm() {
		return lastLogIndex >= rf.getAbsoluteLastIndex()
	}

	return lastLogTerm > rf.getLastTerm()
}

func randomElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Intn(int(ElectionBaseTimeout)))
}

func (rf *Raft) sendToChannel(channel chan bool, value bool) {
	select {
	case channel <- value:
	default:
	}
}

func (rf *Raft) resetChannels() {
	rf.heartbeatCh = make(chan bool)
	rf.voteCh = make(chan bool)
	rf.leaderCh = make(chan bool)
	rf.stepDownCh = make(chan bool)
}
