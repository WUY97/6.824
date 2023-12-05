package shardkv

func (kv *ShardKV) isSameOp(issued Op, applied Op) bool {
	return issued.ClientId == applied.ClientId && issued.RequestId == applied.RequestId
}

func (kv *ShardKV) isShardMatched(key string) bool {
	return kv.config.Shards[key2shard(key)] == kv.gid
}
