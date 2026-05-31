package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Tuple struct {
	value   string
	version rpc.Tversion
}

// server侧需要持有什么？
// 利用什么数据结构？

// 注意引用指针和修改版本号的问题
type KVServer struct {
	mu    sync.Mutex
	cache map[string]Tuple
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.cache = make(map[string]Tuple)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	tuple, ok := kv.cache[key]

	// 不存在
	if !ok {
		reply.Err = rpc.ErrNoKey
		return
	}

	// 存在
	reply.Value = tuple.value
	reply.Version = tuple.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 先上锁，为了线性化
	kv.mu.Lock()
	defer kv.mu.Unlock()

	key := args.Key
	tuple, ok := kv.cache[key]

	// 不存在
	if !ok {
		// version不为0,err
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		// version为0：创建 key，server 上 version 变为 1
		kv.cache[key] = Tuple{
			value:   args.Value,
			version: 1,
		}
		reply.Err = rpc.OK
		return
	}

	// 存在
	// 版本号相同
	if args.Version == tuple.version {
		kv.cache[key] = Tuple{
			value:   args.Value,
			version: tuple.version + 1,
		}
		reply.Err = rpc.OK
		return
	}

	// 版本号不同
	reply.Err = rpc.ErrVersion
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
