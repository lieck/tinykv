package raftstore

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/errorpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) notifyHeartbeatScheduler(region *metapb.Region, peer *peer) {
	clonedRegion := new(metapb.Region)
	err := util.CloneMsg(region, clonedRegion)
	if err != nil {
		return
	}
	d.ctx.schedulerTaskSender <- &runner.SchedulerRegionHeartbeatTask{
		Region:          clonedRegion,
		Peer:            peer.Meta,
		PendingPeers:    peer.CollectPendingPeers(),
		ApproximateSize: peer.ApproximateSize,
	}
}

func (d *peerMsgHandler) applySplit(msg *raft_cmdpb.RaftCmdRequest) bool {
	split := msg.AdminRequest.Split
	splitKey := split.SplitKey

	log.Infof("%v\tstart applySplit\t%v", d.Tag, split.String())
	// check
	if util.CheckKeyInRegionExclusive(splitKey, d.Region()) != nil {
		log.Infof("%v\tapplySplit\tCheckKeyInRegionExclusive err", d.Tag)
		return false
	}
	if len(split.NewPeerIds) != len(d.Region().Peers) {
		log.Infof("%v\tapplySplit\tPeers len err", d.Tag)
		return false
	}
	metaData := d.ctx.storeMeta
	//metaData.Lock()
	//if _, ok := metaData.regions[split.NewRegionId]; ok {
	//	metaData.Unlock()
	//	log.Infof("%%v\tapplySplit\tnew region id err", d.Tag)
	//	return false
	//}
	//metaData.Unlock()

	leftRegion := &metapb.Region{}
	rightRegion := &metapb.Region{}
	if err := util.CloneMsg(d.Region(), leftRegion); err != nil {
		panic(err)
	}
	if err := util.CloneMsg(d.Region(), rightRegion); err != nil {
		panic(err)
	}

	// 更新 Region 信息
	leftRegion.RegionEpoch.Version++
	rightRegion.RegionEpoch.Version++

	rightRegion.StartKey = splitKey
	rightRegion.EndKey = leftRegion.EndKey
	leftRegion.EndKey = splitKey

	rightRegion.Id = split.NewRegionId
	rightRegion.Peers = make([]*metapb.Peer, 0)

	for idx, id := range split.NewPeerIds {
		rightRegion.Peers = append(rightRegion.Peers, &metapb.Peer{
			Id:      id,
			StoreId: leftRegion.Peers[idx].StoreId,
		})
	}

	log.Infof("%v\tapplySplit\toldRegion:%v", d.Tag, leftRegion.String())
	log.Infof("%v\tapplySplit\tnewRegion:%v", d.Tag, rightRegion.String())

	metaData.Lock()
	//metaData.regionRanges.Delete(&regionItem{region: d.Region()})
	metaData.regionRanges.ReplaceOrInsert(&regionItem{region: leftRegion})

	metaData.regionRanges.ReplaceOrInsert(&regionItem{region: rightRegion})
	metaData.regions[leftRegion.Id] = leftRegion
	metaData.regions[rightRegion.Id] = rightRegion
	metaData.Unlock()

	d.SetRegion(leftRegion)

	// 持久化
	kvBW := engine_util.WriteBatch{}
	meta.WriteRegionState(&kvBW, rightRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(&kvBW, leftRegion, rspb.PeerState_Normal)
	if err := kvBW.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		panic(err)
	}

	// 注册 Peerp
	storeId := d.ctx.store.Id
	regionId := rightRegion.Id
	peer, err := createPeer(storeId, d.ctx.cfg, d.peerStorage.regionSched, d.peerStorage.Engines, rightRegion)
	if err != nil {
		panic(err)
	}

	// 注册路由
	d.ctx.router.register(peer)

	// 启动 peer
	if err := d.ctx.router.send(regionId, message.Msg{RegionID: regionId, Type: message.MsgTypeStart}); err != nil {
		panic(err)
	}

	d.notifyHeartbeatScheduler(leftRegion, d.peer)
	d.notifyHeartbeatScheduler(rightRegion, peer)

	return true
}

func (d *peerMsgHandler) applyEntryNormal(e pb.Entry) {
	if e.Data == nil {
		return
	}

	msg := raft_cmdpb.RaftCmdRequest{}
	if err := msg.Unmarshal(e.Data); err != nil {
		log.Panic(err)
	}

	var isResp bool = false
	for len(d.proposals) > 0 {
		p := d.proposals[0]

		if e.Term > p.term {
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		if e.Term == p.term && e.Index > p.index {
			p.cb.Done(ErrRespStaleCommand(p.term))
			d.proposals = d.proposals[1:]
			continue
		}

		isResp = e.Term == p.term && e.Index == p.index
		break
	}

	var cb *message.Callback

	if isResp {
		cb = d.proposals[0].cb
		d.proposals = d.proposals[1:]
	}

	if len(msg.Requests) > 0 { // 普通请求

		if !d.checkRaftCmdRequestRegionEpoch(&msg, cb, e.Index) {
			return
		}

		kvWB := engine_util.WriteBatch{}
		resp := raft_cmdpb.RaftCmdResponse{}

		for _, r := range msg.Requests {
			rr := raft_cmdpb.Response{}
			rr.CmdType = r.CmdType

			switch r.CmdType {
			case raft_cmdpb.CmdType_Put:
				kvWB.SetCF(r.Put.Cf, r.Put.Key, r.Put.Value)
				d.SizeDiffHint += uint64(len(r.Put.Key))
				d.SizeDiffHint += uint64(len(r.Put.Value))
				log.Infof("%v\tapplyEntries idx:%v\tPUT\tkey:%v\tval:%v", d.Tag, e.Index, string(r.Put.Key), string(r.Put.Value))
			case raft_cmdpb.CmdType_Delete:
				kvWB.DeleteCF(r.Delete.Cf, r.Delete.Key)
				d.SizeDiffHint -= uint64(len(r.Delete.Key))
				log.Infof("%v\tapplyEntries idx:%v\tDEL\tkey:%v", d.Tag, e.Index, string(r.Delete.Key))
			}

			if isResp {
				switch r.CmdType {
				case raft_cmdpb.CmdType_Put:
					rr.Put = &raft_cmdpb.PutResponse{}
				case raft_cmdpb.CmdType_Delete:
					rr.Delete = &raft_cmdpb.DeleteResponse{}
				case raft_cmdpb.CmdType_Snap:
					rr.CmdType = raft_cmdpb.CmdType_Snap
					rr.Snap = &raft_cmdpb.SnapResponse{
						Region: d.Region(),
					}
					cb.Txn = d.peerStorage.Engines.Kv.NewTransaction(false)
				case raft_cmdpb.CmdType_Get:
					val, _ := engine_util.GetCF(d.peerStorage.Engines.Kv, r.Get.Cf, r.Get.Key)
					rr.Get = &raft_cmdpb.GetResponse{Value: val}
				}

				resp.Responses = append(resp.Responses, &rr)
			}
		}

		if err := kvWB.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
			log.Panic(err)
		}

		if isResp {
			resp.Header = &raft_cmdpb.RaftResponseHeader{
				Error:       nil,
				Uuid:        nil,
				CurrentTerm: d.Term(),
			}

			cb.Done(&resp)
			log.Infof("%v\tapplyEntries Resp\tidx:%v\tterm:%v\ttype:%v", d.Tag, e.Index, e.Term, resp.Responses[0].CmdType)
		}

	} else {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_CompactLog:
			d.peerStorage.applyState.TruncatedState.Index = msg.AdminRequest.CompactLog.CompactIndex
			d.peerStorage.applyState.TruncatedState.Term = msg.AdminRequest.CompactLog.CompactTerm
			log.Infof("%v\tapplyEntries AdminCmdType_CompactLog", d.Tag)
			d.ScheduleCompactLog(msg.AdminRequest.CompactLog.CompactIndex)
		case raft_cmdpb.AdminCmdType_Split:
			if d.applySplit(&msg) {
				d.lastApplySplitIdx = d.nextProposalIndex()
			}
		}
	}
}

func (d *peerMsgHandler) checkConfChange(e *pb.ConfChange) bool {
	if e.ChangeType == pb.ConfChangeType_RemoveNode {
		if len(d.Region().Peers) == 2 && d.IsLeader() && e.NodeId == d.PeerId() {

			tId := d.Region().Peers[0].Id
			if tId == d.PeerId() {
				tId = d.Region().Peers[1].Id
			}

			log.Infof("%v\tpeers.len == 2 and Leader ==> transferLeader %v", d.Tag, tId)
			d.RaftGroup.TransferLeader(tId)
			return false
		}
	}
	return true
}

func (d *peerMsgHandler) applyEntryConfChange(e pb.Entry) bool {
	cc := pb.ConfChange{}
	if err := cc.Unmarshal(e.Data); err != nil {
		log.Panic(err)
	}

	if !d.checkConfChange(&cc) {
		return false
	}

	region := d.Region()

	changePeerRequest := raft_cmdpb.ChangePeerRequest{}
	if err := changePeerRequest.Unmarshal(cc.Context); err != nil {
		panic(err)
	}

	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		for _, p := range region.Peers {
			if p.GetId() == cc.NodeId {
				return true
			}
		}

		peer := metapb.Peer{
			Id:      cc.NodeId,
			StoreId: changePeerRequest.Peer.StoreId,
		}

		log.Infof("%v\tapplyEntryConfChange\tidx:%v\tAddNode：%v\tStoreId:%v", d.Tag, e.Index, cc.NodeId, peer.StoreId)

		region.Peers = append(region.Peers, &peer)

		d.insertPeerCache(&peer)
	case pb.ConfChangeType_RemoveNode:

		vis := true
		for _, p := range region.Peers {
			if p.GetId() == cc.NodeId {
				vis = false
			}
		}
		if vis {
			return true
		}

		log.Infof("%v\tapplyEntryConfChange\tidx:%v\tRemoveNode：%v", d.Tag, e.Index, cc.NodeId)
		if cc.NodeId == d.peer.PeerId() {
			d.destroyPeer()
			return true
		}

		var peers []*metapb.Peer
		for _, p := range region.Peers {
			if p.Id == cc.NodeId {
				continue
			}
			peers = append(peers, p)
		}

		region.Peers = peers
		d.removePeerCache(cc.NodeId)
	}

	region.RegionEpoch.ConfVer++

	// 持久化 region 信息
	kvBW := engine_util.WriteBatch{}
	meta.WriteRegionState(&kvBW, region, rspb.PeerState_Normal)
	if err := kvBW.WriteToDB(d.peerStorage.Engines.Kv); err != nil {
		log.Panic(err)
	}

	//metaData := d.ctx.storeMeta
	//metaData.Lock()
	//if metaData.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
	//	panic(d.Tag + "\tapplyEntryConfChange del err")
	//}
	//if metaData.regionRanges.ReplaceOrInsert(&regionItem{region: newRegion}) != nil {
	//	panic(d.Tag + "\tapplyEntryConfChange insert err")
	//}
	//metaData.regions[newRegion.Id] = newRegion
	//metaData.Unlock()

	d.RaftGroup.ApplyConfChange(cc)
	d.notifyHeartbeatScheduler(d.Region(), d.peer)

	log.Infof("%v\t%v", d.Tag, d.Region().String())
	return true
}

func (d *peerMsgHandler) applyEntries(ens []pb.Entry, rd *raft.Ready) {
	for idx, e := range ens {
		if e.Index <= d.peerStorage.applyState.AppliedIndex {
			continue
		}
		switch e.EntryType {
		case pb.EntryType_EntryNormal:
			d.applyEntryNormal(e)
		case pb.EntryType_EntryConfChange:
			if !d.applyEntryConfChange(e) {
				rd.CommittedEntries = rd.CommittedEntries[:idx]
			}

			if d.stopped {
				return
			}

			d.peerStorage.applyState.AppliedIndex = e.Index
			if err := engine_util.PutMeta(d.peerStorage.Engines.Kv, meta.ApplyStateKey(d.peerStorage.Region().Id), d.peerStorage.applyState); err != nil {
				panic(err)
			}
		}
	}

	if len(ens) > 0 {
		d.peerStorage.applyState.AppliedIndex = ens[len(ens)-1].Index
		if err := engine_util.PutMeta(d.peerStorage.Engines.Kv, meta.ApplyStateKey(d.peerStorage.Region().Id), d.peerStorage.applyState); err != nil {
			panic(err)
		}
		log.Infof("%v\tapplyEntries\tapplyIdx:%v", d.Tag, ens[len(ens)-1].Index)
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).
	if d.RaftGroup.HasReady() {
		rd := d.RaftGroup.Ready()

		applySnapResult, err := d.peerStorage.SaveReadyState(&rd)
		if err != nil {
			panic(err)
		}
		if applySnapResult != nil {
			// 应用快照状态
			d.peerCache = map[uint64]*metapb.Peer{}
			for _, p := range d.Region().Peers {
				d.peerCache[p.GetId()] = &metapb.Peer{
					Id:      p.Id,
					StoreId: p.StoreId,
				}
			}

			d.SetRegion(applySnapResult.Region)
			metaData := d.ctx.storeMeta
			metaData.Lock()
			//metaData.regionRanges.Delete(&regionItem{region: applySnapResult.PrevRegion})
			metaData.regionRanges.ReplaceOrInsert(&regionItem{region: d.Region()})
			metaData.regions[applySnapResult.Region.GetId()] = d.Region()
			metaData.Unlock()

			log.Infof("%v\tapplySnapResult\tpre:%v\tcurr:%v", d.Tag, applySnapResult.PrevRegion.String(), d.Region().String())
		}

		if len(rd.CommittedEntries) > 0 {
			d.applyEntries(rd.CommittedEntries, &rd)
		}

		if d.stopped {
			return
		}

		d.Send(d.ctx.trans, rd.Messages)

		d.RaftGroup.Advance(rd)
	}
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) proposeChangePeer(msg *raft_cmdpb.RaftCmdRequest) {
	if d.RaftGroup.Raft.PendingConfIndex != raft.None {
		log.Infof("%v\tproposeChangePeer\t存在未应用的成员变更", d.Tag)
		return
	}

	cp := msg.AdminRequest.ChangePeer
	ctx, _ := cp.Marshal()
	cc := pb.ConfChange{
		ChangeType: cp.ChangeType,
		NodeId:     cp.Peer.Id,
		Context:    ctx,
	}

	if !d.checkConfChange(&cc) {
		return
	}

	peers := d.Region().Peers
	if cc.ChangeType == pb.ConfChangeType_AddNode {
		for _, p := range peers {
			if p.GetId() == cc.NodeId {
				log.Warnf("%v\tproposeChangePeer\tadd Node:%v 已经存在", d.Tag, cc.NodeId)
				return
			}
		}
	} else if cc.ChangeType == pb.ConfChangeType_RemoveNode {
		vis := true
		for _, p := range peers {
			if p.GetId() == cc.NodeId {
				vis = false
			}
		}
		if vis {
			log.Warnf("%v\tproposeChangePeer 删除不存在的 peer:%v", d.Tag, cc.NodeId)
			return
		}
	}

	d.RaftGroup.Raft.PendingConfIndex = d.nextProposalIndex()
	log.Infof("%v\tproposeChangePeer\t%v\t%v\tidx:%v", d.Tag, cc.ChangeType, cc.NodeId, d.RaftGroup.Raft.PendingConfIndex)
	if err := d.RaftGroup.ProposeConfChange(cc); err != nil {
		panic(err)
	}
}

func (d *peerMsgHandler) checkRaftCmdRequestRegionEpoch(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback, idx uint64) bool {
	for _, r := range msg.Requests {
		var key []byte
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = r.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = r.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = r.Delete.Key
		case raft_cmdpb.CmdType_Snap:
			if idx != 0 && idx < d.lastApplySplitIdx {
				resp := &raft_cmdpb.RaftCmdResponse{}
				resp.Header = &raft_cmdpb.RaftResponseHeader{
					Error: &errorpb.Error{
						KeyNotInRegion: &errorpb.KeyNotInRegion{},
					},
				}
				cb.Done(resp)
				return false
			}
			continue
		default:
			continue
		}
		err := util.CheckKeyInRegion(key, d.Region())

		if err == nil {
			continue
		}

		if cb != nil {
			resp := &raft_cmdpb.RaftCmdResponse{}
			resp.Header = &raft_cmdpb.RaftResponseHeader{
				Error: &errorpb.Error{
					KeyNotInRegion: &errorpb.KeyNotInRegion{
						Key:      key,
						RegionId: d.regionId,
						StartKey: d.Region().StartKey,
						EndKey:   d.Region().EndKey,
					},
				},
			}
			log.Infof("%v\tcheckRaftCmdRequestRegionEpoch\tkey:%v", d.Tag, string(key))
			cb.Done(resp)
		}
		return false
	}
	return true
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// Your Code Here (2B).
	if msg.AdminRequest != nil {
		switch msg.AdminRequest.CmdType {
		case raft_cmdpb.AdminCmdType_TransferLeader:
			p := msg.AdminRequest.TransferLeader.Peer
			d.RaftGroup.TransferLeader(p.GetId())
			if cb != nil {
				lead := msg.AdminRequest.TransferLeader.Peer.Id
				go func(lead uint64, cb *message.Callback) {
					start := time.Now().Unix() / 1e6
					for {
						if d.RaftGroup.Raft.Lead == lead {
							resp := raft_cmdpb.RaftCmdResponse{}
							resp.Header = &raft_cmdpb.RaftResponseHeader{CurrentTerm: d.Term()}
							resp.AdminResponse = &raft_cmdpb.AdminResponse{
								CmdType: raft_cmdpb.AdminCmdType_TransferLeader,
							}
							cb.Done(&resp)
						}
						time.Sleep(time.Duration(10) * time.Millisecond)
						if time.Now().Unix()/1e6-start >= 1000 {
							return
						}
					}
				}(lead, cb)
			}
			return
		case raft_cmdpb.AdminCmdType_ChangePeer:
			d.proposeChangePeer(msg)
			return
		}
	}

	// 校验 region
	if !d.checkRaftCmdRequestRegionEpoch(msg, cb, 0) {
		return
	}

	if cb != nil {
		idx := d.nextProposalIndex()
		term := d.Term()
		d.proposals = append(d.proposals, &proposal{
			index: idx,
			term:  term,
			cb:    cb,
		})
	}

	idx := d.nextProposalIndex()
	for _, r := range msg.Requests {
		if r.Put != nil || r.Delete != nil {
			log.Infof("%vproposeRaftCommand idx:%v\t%v", d.Tag, idx, r.String())
		}
	}

	if cb != nil {
		if msg.AdminRequest != nil {
			log.Infof("%v\tproposeRaftCommand CB\tidx:%v\tType:%v", d.Tag, d.nextProposalIndex(), msg.AdminRequest.CmdType)
		} else {
			log.Infof("%v\tproposeRaftCommand CB\tidx:%v", d.Tag, d.nextProposalIndex())
		}
	} else {
		if msg.AdminRequest != nil {
			log.Infof("%v\tproposeRaftCommand\tidx:%v\tType:%v", d.Tag, d.nextProposalIndex(), msg.AdminRequest.CmdType)
		} else {
			log.Infof("%v\tproposeRaftCommand\tidx:%v", d.Tag, d.nextProposalIndex())
		}
	}

	data, err := msg.Marshal()
	if err != nil {
		log.Panic(err)
	}

	if err := d.RaftGroup.Propose(data); err != nil {
		switch err {
		case raft.ErrProposalDropped:
			if cb != nil {
				cb.Done(&raft_cmdpb.RaftCmdResponse{
					Header: &raft_cmdpb.RaftResponseHeader{
						Error: &errorpb.Error{
							NotLeader: &errorpb.NotLeader{},
						},
					},
				})
			}
		default:
			panic(err)
		}
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
