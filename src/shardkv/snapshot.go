package shardkv

import (
	"bytes"
	"log"

	"6.824/labgob"
	"6.824/shardctrler"
)

type ShardCopy struct {
	StorageMap map[string]string
	Status     ShardStatus
}

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
	e.Encode(kv.getSnapshotShard())

	snapshot := w.Bytes()
	kv.rf.Snapshot(index, snapshot)
}

func (kv *ShardKV) getSnapshotShard() map[int]*SnapshotShard {
	shardCopy := make(map[int]*SnapshotShard)
	for shardId, shard := range kv.shards {
		storageMap := make(map[string]string)
		shard.Storage.Range(func(key, value interface{}) bool {
			storageMap[key.(string)] = value.(string)
			return true
		})
		shardCopy[shardId] = &SnapshotShard{
			StorageMap: storageMap,
			Status:     shard.Status,
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
	var persistShards map[int]*SnapshotShard

	if d.Decode(&persistLatestAppliedRaftIndex) != nil || d.Decode(&persistLatestAppliedRequest) != nil ||
		d.Decode(&persistPrevConfig) != nil || d.Decode(&persistCurrConfig) != nil || d.Decode(&persistShards) != nil {
		log.Fatalf("gid %d server %d read persister error\n", kv.gid, kv.me)
	} else {
		kv.latestAppliedRaftIndex = persistLatestAppliedRaftIndex
		kv.latestAppliedRequest = persistLatestAppliedRequest
		kv.prevConfig = persistPrevConfig
		kv.currConfig = persistCurrConfig
		kv.getRuntimeShard(persistShards)
	}
}

func (kv *ShardKV) getRuntimeShard(persistShards map[int]*SnapshotShard) {
	kv.shards = make(map[int]*Shard)
	for shardId, snapshotShard := range persistShards {
		newShard := &Shard{
			Status: snapshotShard.Status,
		}
		for k, v := range snapshotShard.StorageMap {
			newShard.Storage.Store(k, v)
		}
		kv.shards[shardId] = newShard
	}
}
