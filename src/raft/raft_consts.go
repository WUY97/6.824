package raft

import "time"

const HeartbeatInterval = 100 * time.Millisecond
const ElectionMaxIncrement = 400 * time.Millisecond
const ElectionBaseTimeout = 400 * time.Millisecond
