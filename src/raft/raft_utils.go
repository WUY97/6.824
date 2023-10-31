package raft

import (
	"math/rand"
	"time"
)

func (rf *Raft) isUpToDate(lastLogIndex int, lastLogTerm int) bool {
	if lastLogTerm == rf.getLastTerm() {
		return lastLogIndex >= rf.getLastIndex()
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
