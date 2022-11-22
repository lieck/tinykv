package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	server.Latches.AcquireLatches([][]byte{req.Key})
	defer server.Latches.ReleaseLatches([][]byte{req.Key})
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	txn := mvcc.NewMvccTxn(sr, req.Version)

	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		return nil, err
	}
	if lock != nil && lock.Ts < req.GetVersion() {
		return &kvrpcpb.GetResponse{
			RegionError: nil,
			Error: &kvrpcpb.KeyError{
				Locked: lock.Info(req.GetKey()),
			},
			Value:    nil,
			NotFound: false,
		}, nil
	}

	value, err := txn.GetValue(req.GetKey())
	if err != nil {
		return nil, err
	}
	if value == nil {
		return &kvrpcpb.GetResponse{
			NotFound: true,
		}, nil
	}
	return &kvrpcpb.GetResponse{
		Value: value,
	}, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()

	// 加锁
	var keys [][]byte
	for _, op := range req.Mutations {
		keys = append(keys, op.Key)
	}
	server.Latches.AcquireLatches(keys)
	defer server.Latches.ReleaseLatches(keys)

	txn := mvcc.NewMvccTxn(sr, req.StartVersion)
	var keyErr []*kvrpcpb.KeyError

	// 检查是否存在锁
	for _, k := range keys {
		l, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if l != nil {
			keyErr = append(keyErr, &kvrpcpb.KeyError{Locked: l.Info(k)})
		}
	}
	if len(keyErr) != 0 {
		return &kvrpcpb.PrewriteResponse{Errors: keyErr}, nil
	}

	// 检查在 StartVersion 之后是否存在写记录
	for _, k := range keys {
		_, ts, err := txn.MostRecentWrite(k)
		if err != nil {
			return nil, err
		}
		if ts > req.StartVersion {
			keyErr = append(keyErr, &kvrpcpb.KeyError{Conflict: &kvrpcpb.WriteConflict{
				StartTs:    req.StartVersion,
				ConflictTs: ts,
				Key:        k,
				Primary:    req.PrimaryLock,
			}})
		}
	}
	if len(keyErr) != 0 {
		return &kvrpcpb.PrewriteResponse{Errors: keyErr}, nil
	}

	// 写入新的记录和锁
	for _, op := range req.Mutations {
		var kind mvcc.WriteKind
		switch op.Op {
		case kvrpcpb.Op_Put:
			kind = mvcc.WriteKindPut
		case kvrpcpb.Op_Del:
			kind = mvcc.WriteKindDelete
		}

		txn.PutLock(op.Key, &mvcc.Lock{
			Primary: req.PrimaryLock,
			Ts:      req.StartVersion,
			Ttl:     req.LockTtl,
			Kind:    kind,
		})

		switch op.Op {
		case kvrpcpb.Op_Put:
			txn.PutValue(op.Key, op.Value)
		case kvrpcpb.Op_Del:
			txn.DeleteValue(op.Value)
		}
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.PrewriteResponse{}, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()
	txn := mvcc.NewMvccTxn(sr, req.StartVersion)

	// 判断是否重复的提交
	var isSameReq bool = true
	for _, k := range req.Keys {
		w, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}
		if w != nil {
			if w.Kind == mvcc.WriteKindDelete || w.Kind == mvcc.WriteKindRollback {
				return &kvrpcpb.CommitResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "true",
					},
				}, nil
			}
		}
		if w == nil || w.StartTS != req.StartVersion {
			isSameReq = false
			break
		}
	}
	if isSameReq {
		return &kvrpcpb.CommitResponse{}, nil
	}

	for _, k := range req.Keys {
		// 判断锁是否被更改
		l, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if l == nil || l.Ts != req.StartVersion {
			return &kvrpcpb.CommitResponse{
				Error: &kvrpcpb.KeyError{
					Retryable: "true",
				},
			}, nil
		}

		// 删除锁并写入记录
		txn.DeleteLock(k)
		txn.PutWrite(k, req.CommitVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    l.Kind,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.CommitResponse{}, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.Version)
	scan := mvcc.NewScanner(req.StartKey, txn)
	defer scan.Close()

	ret := &kvrpcpb.ScanResponse{}
	for i := uint32(0); i < req.Limit; i++ {
		key, value, err := scan.Next()
		if err != nil {
			return nil, err
		}
		if key == nil || value == nil {
			break
		}
		ret.Pairs = append(ret.Pairs, &kvrpcpb.KvPair{
			Key:   key,
			Value: value,
		})
	}
	return ret, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()

	server.Latches.AcquireLatches([][]byte{req.PrimaryKey})
	defer server.Latches.ReleaseLatches([][]byte{req.PrimaryKey})

	txn := mvcc.NewMvccTxn(sr, req.LockTs)
	write, commitTs, err := txn.CurrentWrite(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	if write != nil {
		switch write.Kind {
		case mvcc.WriteKindRollback:
			return &kvrpcpb.CheckTxnStatusResponse{}, nil
		default:
			return &kvrpcpb.CheckTxnStatusResponse{
				CommitVersion: commitTs,
			}, nil
		}
	}

	lock, err := txn.GetLock(req.PrimaryKey)
	if err != nil {
		return nil, err
	}

	ret := &kvrpcpb.CheckTxnStatusResponse{}

	if lock != nil {
		if mvcc.PhysicalTime(lock.Ts)+lock.Ttl > mvcc.PhysicalTime(req.CurrentTs) {
			return &kvrpcpb.CheckTxnStatusResponse{
				LockTtl: lock.Ttl,
			}, nil
		}
		txn.DeleteLock(req.GetPrimaryKey())
		txn.DeleteValue(req.GetPrimaryKey())
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: lock.Ts,
			Kind:    mvcc.WriteKindRollback,
		})
		ret.Action = kvrpcpb.Action_TTLExpireRollback
	} else {
		txn.PutWrite(req.PrimaryKey, req.LockTs, &mvcc.Write{
			StartTS: req.LockTs,
			Kind:    mvcc.WriteKindRollback,
		})
		ret.Action = kvrpcpb.Action_LockNotExistRollback
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return ret, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).

	server.Latches.AcquireLatches(req.Keys)
	defer server.Latches.ReleaseLatches(req.Keys)

	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.StartVersion)

	for _, k := range req.Keys {
		write, _, err := txn.CurrentWrite(k)
		if err != nil {
			return nil, err
		}
		if write != nil {
			switch write.Kind {
			case mvcc.WriteKindRollback:
				continue
			default:
				return &kvrpcpb.BatchRollbackResponse{
					Error: &kvrpcpb.KeyError{
						Abort: "true",
					},
				}, nil
			}
		}

		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			txn.DeleteLock(k)
		}

		txn.DeleteValue(k)
		txn.PutWrite(k, req.StartVersion, &mvcc.Write{
			StartTS: req.StartVersion,
			Kind:    mvcc.WriteKindRollback,
		})
	}

	err = server.storage.Write(req.Context, txn.Writes())
	if err != nil {
		return nil, err
	}

	return &kvrpcpb.BatchRollbackResponse{}, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	sr, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.StartVersion)
	iter := sr.IterCF(engine_util.CfLock)
	defer iter.Close()

	var keys [][]byte
	for ; iter.Valid(); iter.Next() {
		k := iter.Item().Key()
		lock, err := txn.GetLock(k)
		if err != nil {
			return nil, err
		}
		if lock != nil && lock.Ts == req.StartVersion {
			keys = append(keys, k)
		}
	}

	ret := &kvrpcpb.ResolveLockResponse{}
	if req.CommitVersion == 0 {
		brr, err := server.KvBatchRollback(nil, &kvrpcpb.BatchRollbackRequest{
			Context:      req.Context,
			StartVersion: req.StartVersion,
			Keys:         keys,
		})
		if err != nil {
			return nil, err
		}
		ret.Error = brr.Error
		ret.RegionError = brr.RegionError
	} else {
		cr, err := server.KvCommit(nil, &kvrpcpb.CommitRequest{
			Context:       req.Context,
			StartVersion:  req.StartVersion,
			Keys:          keys,
			CommitVersion: req.CommitVersion,
		})
		if err != nil {
			return nil, err
		}
		ret.Error = cr.Error
		ret.RegionError = cr.RegionError
	}

	return ret, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
