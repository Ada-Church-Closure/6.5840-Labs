package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// client调用去获取某个value
	args := rpc.GetArgs{
		Key: key,
	}

	reply := rpc.GetReply{}

	// 直接进行无限的重发，因为不会更改server的状态
	for ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) != true {
		time.Sleep(100 * time.Millisecond)
	}

	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{
		Key:     key,
		Value:   value,
		Version: version,
	}

	reply := rpc.PutReply{}

	retried := false

	for ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) != true {
		retried = true
		// 重试之前可以稍微等待一下
		time.Sleep(100 * time.Millisecond)
	}

	if reply.Err != rpc.ErrVersion {
		return reply.Err
	} else {
		if retried {
			// 目的就是在这里给上层一个更加明确的语义
			return rpc.ErrMaybe
		} else {
			return rpc.ErrVersion
		}
	}
}
