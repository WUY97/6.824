package shardkv

// func (kv *ShardKV) needSnapshot() bool {
// 	return kv.maxraftstate != -1 && kv.rf.RaftStateSize() >= kv.maxraftstate
// }

// func (kv *ShardKV) createSnapshot(index int) {
// 	w := new(bytes.Buffer)
// 	e := labgob.NewEncoder(w)
// 	e.Encode(kv.db)
// 	e.Encode(kv.latestRequestId)
// 	snapshot := w.Bytes()
// 	kv.rf.Snapshot(index, snapshot)
// }

// func (kv *ShardKV) readSnapshot(snapshot []byte) {
// 	if snapshot == nil || len(snapshot) < 1 {
// 		return
// 	}
// 	r := bytes.NewBuffer(snapshot)
// 	d := labgob.NewDecoder(r)
// 	var db map[string]string
// 	var latestRequestId map[int64]int64
// 	if d.Decode(&db) != nil || d.Decode(&latestRequestId) != nil {
// 		panic("read snapshot error")
// 	} else {
// 		kv.db = db
// 		kv.latestRequestId = latestRequestId
// 	}
// }
