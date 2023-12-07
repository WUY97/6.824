package shardkv

import (
	"bytes"
	"log"

	"6.824/labgob"
	"6.824/shardctrler"
)

func (kv *ShardKV) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate
}

func (kv *ShardKV) createSnapshot(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.DPrintf("server %d serialize to a snapshot\n", kv.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.latestAppliedRaftIndex)
	e.Encode(kv.latestAppliedRequest)
	e.Encode(kv.prevConfig)
	e.Encode(kv.currConfig)
	shardCopy := kv.getShardCopy()
	e.Encode(shardCopy)

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) getShardCopy() map[int]*Shard {
	shardCopy := make(map[int]*Shard)
	for shardId, shard := range kv.shards {
		shardCopy[shardId] = &Shard{
			Storage: make(map[string]string),
			Status:  shard.Status,
		}
		for k, v := range shard.Storage {
			shardCopy[shardId].Storage[k] = v
		}

	}
	return shardCopy
}

func (kv *ShardKV) readSnapshot(snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if snapshot == nil || len(snapshot) < 1 {
		kv.DPrintf("server %d has no snapshot to recover\n", kv.me)
		return
	}

	kv.DPrintf("server %d read persister to recover\n", kv.me)

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var persistLatestAppliedRaftIndex int
	var persistLatestAppliedRequest map[int64]int64
	var persistPrevConfig shardctrler.Config
	var persistCurrConfig shardctrler.Config
	var persistShards map[int]*Shard

	if d.Decode(&persistLatestAppliedRaftIndex) != nil || d.Decode(&persistLatestAppliedRequest) != nil ||
		d.Decode(&persistPrevConfig) != nil || d.Decode(&persistCurrConfig) != nil || d.Decode(&persistShards) != nil {
		log.Fatalf("gid %d server %d read persister error\n", kv.gid, kv.me)
	} else {
		kv.latestAppliedRaftIndex = persistLatestAppliedRaftIndex
		kv.latestAppliedRequest = persistLatestAppliedRequest
		kv.prevConfig = persistPrevConfig
		kv.currConfig = persistCurrConfig
		kv.shards = persistShards
	}
}
