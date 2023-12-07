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
