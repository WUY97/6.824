package shardkv

import (
	"fmt"
	"log"
)

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		rightHalf := fmt.Sprintf(format, a...)
		log.Printf("shardkv gid: %v | %s", kv.gid, rightHalf)
	}
	return
}

// func (kv *ShardKV) fromSyncMapToMap(syncMap *sync.Map) map[string]string {
// 	kvMap := make(map[string]string)
// 	syncMap.Range(func(key, value interface{}) bool {
// 		kvMap[key.(string)] = value.(string)
// 		return true
// 	})
// 	return kvMap
// }

// func (kv *ShardKV) fromMapToSyncMap(kvMap map[string]string) *sync.Map {
// 	syncMap := &sync.Map{}
// 	for k, v := range kvMap {
// 		syncMap.Store(k, v)
// 	}
// 	return syncMap
// }
