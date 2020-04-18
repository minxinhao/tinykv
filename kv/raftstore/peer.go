package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap/errors"
)

func NotifyStaleReq(term uint64, cb *message.Callback) {
	cb.Done(ErrRespStaleCommand(term))
}

func NotifyReqRegionRemoved(regionId uint64, cb *message.Callback) {
	regionNotFound := &util.ErrRegionNotFound{RegionId: regionId}
	resp := ErrResp(regionNotFound)
	cb.Done(resp)
}

// If we create the peer actively, like bootstrap/split/merge region, we should
// use this function to create the peer. The region must contain the peer info
// for this store.
func createPeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, region *metapb.Region) (*peer, error) {
	metaPeer := util.FindPeer(region, storeID)
	// fmt.Println("fine createPeer")
	if metaPeer == nil {
		return nil, errors.Errorf("find no peer for store %d in region %v", storeID, region)
	}
	log.Infof("region %v create peer with ID %d", region, metaPeer.Id)
	// fmt.Println("fine createPeer")
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

// The peer can be created from another node with raft membership changes, and we only
// know the region_id and peer_id when creating this replicated peer, the region info
// will be retrieved later after applying snapshot.
func replicatePeer(storeID uint64, cfg *config.Config, sched chan<- worker.Task,
	engines *engine_util.Engines, regionID uint64, metaPeer *metapb.Peer) (*peer, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("[region %v] replicates peer with ID %d", regionID, metaPeer.GetId())
	region := &metapb.Region{
		Id:          regionID,
		RegionEpoch: &metapb.RegionEpoch{},
	}
	return NewPeer(storeID, cfg, engines, region, sched, metaPeer)
}

type proposal struct {
	// index + term for unique identification
	index uint64
	term  uint64
	cb    *message.Callback
}

type peer struct {
	// The ticker of the peer, used to trigger
	// * raft tick
	// * raft log gc
	// * region heartbeat
	// * split check
	opCnt  uint64
	ticker *ticker
	// Instance of the Raft module
	RaftGroup *raft.RawNode
	// The peer storage for the Raft module
	peerStorage *PeerStorage

	// Record the meta information of the peer
	Meta     *metapb.Peer
	regionId uint64
	// Tag which is useful for printing log
	Tag string

	// Record the callback of the proposals
	// (Used in 2B)
	proposals []*proposal

	// Index of last scheduled compacted raft log.
	// (Used in 2C)
	LastCompactedIdx uint64

	// Cache the peers information from other stores
	// when sending raft messages to other peers, it's used to get the store id of target peer
	// (Used in 3B conf change)
	peerCache map[uint64]*metapb.Peer
	// Record the instants of peers being added into the configuration.
	// Remove them after they are not pending any more.
	// (Used in 3B conf change)
	PeersStartPendingTime map[uint64]time.Time
	// Mark the peer as stopped, set when peer is destroyed
	// (Used in 3B conf change)
	stopped bool

	// An inaccurate difference in region size since last reset.
	// split checker is triggered when it exceeds the threshold, it makes split checker not scan the data very often
	// (Used in 3B split)
	SizeDiffHint uint64
	// Approximate size of the region.
	// It's updated everytime the split checker scan the data
	// (Used in 3B split)
	ApproximateSize *uint64
}

// Contruct a new peer with given information
func NewPeer(storeId uint64, cfg *config.Config, engines *engine_util.Engines, region *metapb.Region, regionSched chan<- worker.Task,
	meta *metapb.Peer) (*peer, error) {
	if meta.GetId() == util.InvalidID {
		return nil, fmt.Errorf("invalid peer id")
	}
	tag := fmt.Sprintf("[region %v] %v", region.GetId(), meta.GetId())
	// fmt.Println("fine NewPeer")
	ps, err := NewPeerStorage(engines, region, regionSched, tag)
	ps.FirstIndex()
	if err != nil {
		return nil, err
	}

	appliedIndex := ps.AppliedIndex()
	// fmt.Println("fine NewPeer")

	raftCfg := &raft.Config{
		ID:            meta.GetId(),
		ElectionTick:  cfg.RaftElectionTimeoutTicks,
		HeartbeatTick: cfg.RaftHeartbeatTicks,
		Applied:       appliedIndex,
		Storage:       ps,
	}
	// fmt.Println("fine NewPeer")

	raftGroup, err := raft.NewRawNode(raftCfg)
	if err != nil {
		return nil, err
	}
	p := &peer{
		Meta:                  meta,
		regionId:              region.GetId(),
		RaftGroup:             raftGroup,
		peerStorage:           ps,
		peerCache:             make(map[uint64]*metapb.Peer),
		PeersStartPendingTime: make(map[uint64]time.Time),
		Tag:                   tag,
		ticker:                newTicker(region.GetId(), cfg),
		opCnt:                 0,
	}
	// fmt.Println("fine NewPeer")

	// If this region has only one peer and I am the one, campaign directly.
	if len(region.GetPeers()) == 1 && region.GetPeers()[0].GetStoreId() == storeId {
		err = p.RaftGroup.Campaign()
		if err != nil {
			return nil, err
		}
	}
	// fmt.Println("fine NewPeer")
	// fmt.Println(p.Region())
	return p, nil
}

// Set given peer's id and storeID in peerCache
func (p *peer) insertPeerCache(peer *metapb.Peer) {
	p.peerCache[peer.GetId()] = peer
}

// Delete given peer from peerCache
func (p *peer) removePeerCache(peerID uint64) {
	delete(p.peerCache, peerID)
}

// Read the id and storeID of given peerID
func (p *peer) getPeerFromCache(peerID uint64) *metapb.Peer {
	if peer, ok := p.peerCache[peerID]; ok {
		return peer
	}
	for _, peer := range p.peerStorage.Region().GetPeers() {
		if peer.GetId() == peerID {
			p.insertPeerCache(peer)
			return peer
		}
	}
	return nil
}

// Return the index after raftLog's lastIndex
func (p *peer) nextProposalIndex() uint64 {
	return p.RaftGroup.Raft.RaftLog.LastIndex() + 1
}

/// Tries to destroy itself. Returns a job (if needed) to do more cleaning tasks.
func (p *peer) MaybeDestroy() bool {
	if p.stopped {
		log.Infof("%v is being destroyed, skip", p.Tag)
		return false
	}
	return true
}

/// Does the real destroy worker.Task which includes:
/// 1. Set the region to tombstone;
/// 2. Clear data;
/// 3. Notify all pending requests.
func (p *peer) Destroy(engine *engine_util.Engines, keepData bool) error {
	start := time.Now()
	region := p.Region()
	log.Infof("%v begin to destroy", p.Tag)

	// Set Tombstone state explicitly
	kvWB := new(engine_util.WriteBatch)
	raftWB := new(engine_util.WriteBatch)
	if err := p.peerStorage.clearMeta(kvWB, raftWB); err != nil {
		return err
	}
	meta.WriteRegionState(kvWB, region, rspb.PeerState_Tombstone)
	// write kv rocksdb first in case of restart happen between two write
	if err := kvWB.WriteToDB(engine.Kv); err != nil {
		return err
	}
	if err := raftWB.WriteToDB(engine.Raft); err != nil {
		return err
	}

	if p.peerStorage.isInitialized() && !keepData {
		// If we meet panic when deleting data and raft log, the dirty data
		// will be cleared by a newer snapshot applying or restart.
		p.peerStorage.ClearData()
	}

	for _, proposal := range p.proposals {
		NotifyReqRegionRemoved(region.Id, proposal.cb)
	}
	p.proposals = nil

	log.Infof("%v destroy itself, takes %v", p.Tag, time.Now().Sub(start))
	return nil
}

func (p *peer) isInitialized() bool {
	return p.peerStorage.isInitialized()
}

func (p *peer) storeID() uint64 {
	return p.Meta.StoreId
}

func (p *peer) Region() *metapb.Region {
	return p.peerStorage.Region()
}

/// Set the region of a peer.
///
/// This will update the region of the peer, caller must ensure the region
/// has been preserved in a durable device.
func (p *peer) SetRegion(region *metapb.Region) {
	p.peerStorage.SetRegion(region)
}

func (p *peer) PeerId() uint64 {
	return p.Meta.GetId()
}

func (p *peer) LeaderId() uint64 {
	return p.RaftGroup.Raft.Lead
}

func (p *peer) IsLeader() bool {
	return p.RaftGroup.Raft.State == raft.StateLeader
}

// Send messages in msgs through trans
func (p *peer) Send(trans Transport, msgs []eraftpb.Message) {
	for _, msg := range msgs {
		err := p.sendRaftMessage(msg, trans)
		if err != nil {
			log.Debugf("%v send message err: %v", p.Tag, err)
		}
	}
}

/// Collects all pending peers and update `peers_start_pending_time`.
func (p *peer) CollectPendingPeers() []*metapb.Peer {
	pendingPeers := make([]*metapb.Peer, 0, len(p.Region().GetPeers()))
	truncatedIdx := p.peerStorage.truncatedIndex()
	for id, progress := range p.RaftGroup.GetProgress() {
		if id == p.Meta.GetId() {
			continue
		}
		if progress.Match < truncatedIdx {
			if peer := p.getPeerFromCache(id); peer != nil {
				pendingPeers = append(pendingPeers, peer)
				if _, ok := p.PeersStartPendingTime[id]; !ok {
					now := time.Now()
					p.PeersStartPendingTime[id] = now
					log.Debugf("%v peer %v start pending at %v", p.Tag, id, now)
				}
			}
		}
	}
	return pendingPeers
}

// Delete all peers from PeersStartPendingTime
func (p *peer) clearPeersStartPendingTime() {
	for id := range p.PeersStartPendingTime {
		delete(p.PeersStartPendingTime, id)
	}
}

/// Returns `true` if any new peer catches up with the leader in replicating logs.
/// And updates `PeersStartPendingTime` if needed.
func (p *peer) AnyNewPeerCatchUp(peerId uint64) bool {
	if len(p.PeersStartPendingTime) == 0 {
		return false
	}
	if !p.IsLeader() {
		p.clearPeersStartPendingTime()
		return false
	}
	if startPendingTime, ok := p.PeersStartPendingTime[peerId]; ok {
		truncatedIdx := p.peerStorage.truncatedIndex()
		progress, ok := p.RaftGroup.Raft.Prs[peerId]
		if ok {
			if progress.Match >= truncatedIdx {
				delete(p.PeersStartPendingTime, peerId)
				elapsed := time.Since(startPendingTime)
				log.Debugf("%v peer %v has caught up logs, elapsed: %v", p.Tag, peerId, elapsed)
				return true
			}
		}
	}
	return false
}

// If there's more than one peer in region and parentIsLeader para is true
// Launched a campaign
func (p *peer) MaybeCampaign(parentIsLeader bool) bool {
	// The peer campaigned when it was created, no need to do it again.
	if len(p.Region().GetPeers()) <= 1 || !parentIsLeader {
		return false
	}

	// If last peer is the leader of the region before split, it's intuitional for
	// it to become the leader of new split region.
	p.RaftGroup.Campaign()
	return true
}

// Return the term of the raft in RaftGroup
func (p *peer) Term() uint64 {
	return p.RaftGroup.Raft.Term
}

func (p *peer) HeartbeatScheduler(ch chan<- worker.Task) {
	ch <- &runner.SchedulerRegionHeartbeatTask{
		Region:          p.Region(),
		Peer:            p.Meta,
		PendingPeers:    p.CollectPendingPeers(),
		ApproximateSize: p.ApproximateSize,
	}
}

// Construct RaftMessage from msg and set peer information in it
// Send constructed RaftMessage through trans
func (p *peer) sendRaftMessage(msg eraftpb.Message, trans Transport) error {
	sendMsg := new(rspb.RaftMessage)
	sendMsg.RegionId = p.regionId
	// set current epoch
	sendMsg.RegionEpoch = &metapb.RegionEpoch{
		ConfVer: p.Region().RegionEpoch.ConfVer,
		Version: p.Region().RegionEpoch.Version,
	}

	fromPeer := *p.Meta
	toPeer := p.getPeerFromCache(msg.To)
	if toPeer == nil {
		return fmt.Errorf("failed to lookup recipient peer %v in region %v", msg.To, p.regionId)
	}
	log.Debugf("%v, send raft msg %v from %v to %v", p.Tag, msg.MsgType, fromPeer, toPeer)

	sendMsg.FromPeer = &fromPeer
	sendMsg.ToPeer = toPeer

	// There could be two cases:
	// 1. Target peer already exists but has not established communication with leader yet
	// 2. Target peer is added newly due to member change or region split, but it's not
	//    created yet
	// For both cases the region start key and end key are attached in RequestVote and
	// Heartbeat message for the store of that peer to check whether to create a new peer
	// when receiving these messages, or just to wait for a pending region split to perform
	// later.
	if p.peerStorage.isInitialized() && util.IsInitialMsg(&msg) {
		sendMsg.StartKey = append([]byte{}, p.Region().StartKey...)
		sendMsg.EndKey = append([]byte{}, p.Region().EndKey...)
	}
	sendMsg.Message = &msg
	return trans.Send(sendMsg)
}

func (p *peer) HandleRaftReady(pdScheduler chan<- worker.Task, trans Transport) *ApplySnapResult {
	if p.stopped {
		return nil
	}
	if !p.RaftGroup.HasReady() {
		// fmt.Println(p.PeerId(), " has no ready")
		return nil
	}
	ready := p.RaftGroup.Ready()
	if ready.Snapshot.GetMetadata() == nil {
		ready.Snapshot.Metadata = &eraftpb.SnapshotMetadata{}
	}

	p.Send(trans, ready.Messages)
	ready.Messages = ready.Messages[:0]

	ss := ready.SoftState
	if ss != nil && ss.RaftState == raft.StateLeader {
		p.HeartbeatScheduler(pdScheduler)
	}

	applySnapResult, err := p.peerStorage.SaveReadyState(&ready)
	if err != nil {
		panic(err)
	}

	// fmt.Println(p.PeerId(), " get  ready ", ready.CommittedEntries)
	p.ShowReady(&ready)
	if len(ready.CommittedEntries) == 0 {
		fmt.Println(p.PeerId(), " get ready with empty CommittedEntries")
	}
	// fmt.Println(p.PeerId(), " handleready get ready commitentries  ", ready.CommittedEntries)
	if len(ready.CommittedEntries) > 0 {
		p.ApplyReady(&ready)
	}

	p.RaftGroup.Advance(ready)
	return applySnapResult
}

func (p *peer) ShowReady(ready *raft.Ready) {
	fmt.Println(p.PeerId(), "op ", p.opCnt, " get  ready with  ", len(ready.CommittedEntries), " entries")
	for _, entry := range ready.CommittedEntries {
		req := new(raft_cmdpb.RaftCmdRequest)
		err := req.Unmarshal(entry.Data)
		if err != nil {
			fmt.Println("fail marsh to request command")
		} else {
			fmt.Println(p.PeerId(), "op ", p.opCnt, "get req ", req)
		}
	}
	p.opCnt++
}
func (p *peer) ApplyReady(ready *raft.Ready) {
	for _, entry := range ready.CommittedEntries {
		if len(entry.Data) > 0 {
			req := new(raft_cmdpb.RaftCmdRequest)
			err := req.Unmarshal(entry.Data)
			if err != nil {
				panic(err)
			}
			// fmt.Println(p.PeerId(), "op ", p.opCnt, " HandleReady get req ", req)
			resp, txn := p.ApplyRaftCommand(req, new(engine_util.WriteBatch))
			if p.proposals == nil || len(p.proposals) == 0 {
				// fmt.Println(p.PeerId(), " exits handleready with empty proposal  ")
				break
			} else {
				cb := p.findCallback(entry.Index, entry.Term)
				if cb == nil {
					// fmt.Println("No match callback for index ", entry.Index, " term ", entry.Term, "with resp ", resp)
				} else {
					cb.Resp = resp
					cb.Txn = txn
					cb.Done(resp)
					// fmt.Printf("Set callback with %v\n", resp)
				}
			}
		} else {
			//Deal with empty entry send by new leader
			for _, proposal := range p.proposals {
				if proposal.term >= entry.Term {
					break
				}
				proposal.cb.Txn = nil
				proposal.cb.Done(ErrRespStaleCommand(entry.Term))
			}
		}

	}
	ready.CommittedEntries = nil
	p.proposals = p.proposals[:0]
}

func (p *peer) popProposal(term uint64) *proposal {
	if len(p.proposals) == 0 {
		return nil
	}
	proposal := p.proposals[0]
	if proposal.term > term {
		return nil
	}
	p.proposals = p.proposals[1:]
	return proposal
}

func (p *peer) findCallback(index, term uint64) *message.Callback {
	for _, propose := range p.proposals {
		if propose == nil {
			break
		}
		if propose.index == index && propose.term == term && propose.cb.Resp == nil {
			return propose.cb
		}
		if propose.cb.Resp == nil {
			propose.cb.Done(ErrRespStaleCommand(term))
		}
	}
	return nil
}

func (p *peer) proposeKvOp(cfg *config.Config, req *raft_cmdpb.RaftCmdRequest) (uint64, error) {
	data, err := req.Marshal()
	if err != nil {
		return 0, err
	}
	// fmt.Println("Propose req ", req)

	proposeIndex := p.nextProposalIndex()
	err = p.RaftGroup.Propose(data)
	if err != nil {
		return 0, err
	}

	// Proposing in raftgroup failed
	if proposeIndex == p.nextProposalIndex() {
		return 0, &util.ErrNotLeader{RegionId: p.regionId}
	}

	return proposeIndex, nil
}

func (p *peer) Propose(cfg *config.Config, cb *message.Callback, req *raft_cmdpb.RaftCmdRequest, errResp *raft_cmdpb.RaftCmdResponse) bool {
	if p.stopped {
		return false
	}
	// fmt.Println(p.Region())

	req_type, err := p.GetRequestType(req)
	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}
	var index uint64
	switch req_type {
	case RequestType_KvOp:
		index, err = p.proposeKvOp(cfg, req)
	case RequestType_TransferLeader:
		return true
	case RequestType_ConfChange:
		return true
	}

	if err != nil {
		BindRespError(errResp, err)
		cb.Done(errResp)
		return false
	}
	proposal := &proposal{
		index: index,
		term:  p.Term(),
		cb:    cb,
	}
	p.proposals = append(p.proposals, proposal)
	return true
}

type RequestType int

const (
	RequestType_Invalid RequestType = 0 + iota
	RequestType_KvOp
	RequestType_TransferLeader
	RequestType_ConfChange
)

func (p *peer) GetRequestType(req *raft_cmdpb.RaftCmdRequest) (RequestType, error) {
	if req.AdminRequest != nil {
		if req.AdminRequest.ChangePeer != nil {
			return RequestType_ConfChange, nil
		}
		if req.AdminRequest.TransferLeader != nil {
			return RequestType_TransferLeader, nil
		}
	}

	readFlag := false
	writeFlag := false
	for _, request := range req.Requests {
		switch request.CmdType {
		case raft_cmdpb.CmdType_Get, raft_cmdpb.CmdType_Snap:
			readFlag = true
		case raft_cmdpb.CmdType_Delete, raft_cmdpb.CmdType_Put:
			writeFlag = true
		case raft_cmdpb.CmdType_Invalid:
			return RequestType_Invalid, errors.New("CmdType_Invalid can't be proposed")
		}

		if readFlag && writeFlag {
			return RequestType_Invalid, errors.New("Request with both read operation and write operation can't be proposed")
		}
	}
	return RequestType_KvOp, nil
}

func (p *peer) GetProposals() []*proposal {
	if len(p.proposals) == 0 {
		return nil
	}
	proposals := p.proposals
	p.proposals = nil
	return proposals
}

func (p *peer) ApplyRaftCommand(req *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) (
	*raft_cmdpb.RaftCmdResponse, *badger.Txn) {
	wb.SetSafePoint()
	resp, txn, err := p.RunRaftCommand(req, wb)
	if err != nil {
		wb.RollbackToSafePoint()
		if txn != nil {
			txn.Discard()
			txn = nil
		}
		resp = ErrResp(err)
	}
	// wb.PrintEntries()
	wb.MustWriteToDB(p.peerStorage.Engines.Kv)
	return resp, txn
}

func (p *peer) RunRaftCommand(req *raft_cmdpb.RaftCmdRequest, wb *engine_util.WriteBatch) (
	resp *raft_cmdpb.RaftCmdResponse, txn *badger.Txn, err error) {
	err = util.CheckRegionEpoch(req, p.Region(), false)
	if err != nil {
		return
	}
	requests := req.GetRequests()
	resps := make([]*raft_cmdpb.Response, 0, len(requests))
	writeFlag, readFlag := false, false
	for _, req := range requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Put:
			var r *raft_cmdpb.Response
			r, err = p.RunPut(req.GetPut(), wb)
			resps = append(resps, r)
			writeFlag = true
		case raft_cmdpb.CmdType_Delete:
			var r *raft_cmdpb.Response
			r, err = p.RunDelete(req.GetDelete(), wb)
			resps = append(resps, r)
			writeFlag = true
		case raft_cmdpb.CmdType_Get:
			var r *raft_cmdpb.Response
			r, err = p.RunGet(req.GetGet())
			resps = append(resps, r)
			readFlag = true
		case raft_cmdpb.CmdType_Snap:
			resps = append(resps, &raft_cmdpb.Response{
				CmdType: raft_cmdpb.CmdType_Snap,
				Snap:    &raft_cmdpb.SnapResponse{Region: p.Region()},
			})
			txn = p.peerStorage.Engines.Kv.NewTransaction(false)
			readFlag = true
		}
	}
	if writeFlag && readFlag {
		panic("Invalid request withi both write and read operation")
	}
	resp = newCmdRespForReq(req)
	resp.Responses = resps
	return
}

// func (p *peer) TestScan(txn *badger.Txn) {
// 	st := strconv.Itoa(0) + " " + fmt.Sprintf("%08d", 0)
// 	ed := strconv.Itoa(0) + " " + fmt.Sprintf("%08d", 2)
// 	start, end := []byte(st), []byte(ed)
// 	values := make([][]byte, 0)
// 	key := start
// 	region := p.Region()
// 	iter := raft_storage.NewRegionReader(txn, *region).IterCF(engine_util.CfDefault)
// 	for iter.Seek(key); iter.Valid(); iter.Next() {
// 		if engine_util.ExceedEndKey(iter.Item().Key(), end) {
// 			break
// 		}
// 		value, err := iter.Item().ValueCopy(nil)
// 		if err != nil {
// 			panic(err)
// 		}
// 		values = append(values, value)
// 	}
// 	iter.Close()

// 	key = region.EndKey
// 	if len(key) == 0 {
// 		break
// 	}
// }

func (p *peer) RunPut(req *raft_cmdpb.PutRequest, wb *engine_util.WriteBatch) (*raft_cmdpb.Response, error) {
	key, value := req.GetKey(), req.GetValue()
	if err := util.CheckKeyInRegion(key, p.Region()); err != nil {
		return nil, err
	}
	if cf := req.GetCf(); len(cf) != 0 {
		wb.SetCF(cf, key, value)
		// fmt.Println("Current leader is ", p.RaftGroup.Raft.Lead, " peer ", p.PeerId(), " RunPut key ", string(engine_util.KeyWithCF(cf, key)), " value ", string(value))
	} else {
		wb.SetCF(engine_util.CfDefault, key, value)
		// fmt.Println("Current leader is ", p.RaftGroup.Raft.Lead, " peer ", p.PeerId(), " RunPut key ", string(engine_util.KeyWithCF(engine_util.CfDefault, key)), " value ", string(value))
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Put,
	}, nil
}

func (p *peer) RunGet(req *raft_cmdpb.GetRequest) (*raft_cmdpb.Response, error) {
	key := req.GetKey()
	if err := util.CheckKeyInRegion(key, p.Region()); err != nil {
		return nil, err
	}
	var val []byte
	var err error
	if cf := req.GetCf(); len(cf) != 0 {
		val, err = engine_util.GetCF(p.peerStorage.Engines.Kv, cf, key)
		// fmt.Println("Current Leader is ", p.RaftGroup.Raft.Lead, " RunGet key ", string(engine_util.KeyWithCF(cf, key)), " Get value ", string(val))
	} else {
		val, err = engine_util.GetCF(p.peerStorage.Engines.Kv, engine_util.CfDefault, key)
		// fmt.Println("Current Leader is ", p.RaftGroup.Raft.Lead, " RunGet key ", string(engine_util.KeyWithCF(engine_util.CfDefault, key)), " Get value ", string(val))
	}
	if err == badger.ErrKeyNotFound {
		err = nil
		val = nil
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Get,
		Get:     &raft_cmdpb.GetResponse{Value: val},
	}, err
}

func (p *peer) RunDelete(req *raft_cmdpb.DeleteRequest, wb *engine_util.WriteBatch) (*raft_cmdpb.Response, error) {
	key := req.GetKey()
	if err := util.CheckKeyInRegion(key, p.Region()); err != nil {
		return nil, err
	}

	if cf := req.GetCf(); len(cf) != 0 {
		wb.DeleteCF(cf, key)
		// fmt.Println("Current Leader is ", p.RaftGroup.Raft.Lead, " RunDelete key ", string(engine_util.KeyWithCF(cf, key)))
	} else {
		wb.DeleteCF(engine_util.CfDefault, key)
		// fmt.Println("Current Leader is ", p.RaftGroup.Raft.Lead, " RunDelete key ", string(engine_util.KeyWithCF(engine_util.CfDefault, key)))
	}
	return &raft_cmdpb.Response{
		CmdType: raft_cmdpb.CmdType_Delete,
	}, nil
}
