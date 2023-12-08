package raft

import "time"

const HeartbeatInterval = 100 * time.Millisecond
const ElectionMaxIncrement = 50 * time.Millisecond
const ElectionBaseTimeout = 500 * time.Millisecond
