## LAB2 RaftKV
Raft is a consensus algorithm that is designed to be easy to understand. You can read material about the Raft itself at the Raft site, an interactive visualization of the Raft, and other resources, including the extended Raft paper. 
In this lab you will implement a high available kv server based on raft,  which needs you not only just implement the Raft algorithm but also use it practically, and bring you more challenges like manage raft’s persisted state with `badger`, add flow control for snapshot message, etc.
The lab have 3 parts, including:
1. Implement the basic Raft algorithm
2. Build a fault tolerant KV server on top of Raft
3. Add the support of raftlog GC and snapshot 

### Part A
#### The Code
In this part you will implement the basic raft algorithm, the code you need to implement is under `raft/` and the proto file in `proto/proto/eraftpb.proto`. Inside `raft/`, there are some skeleton code and test cases waiting for you. Difference from 6.824,  the raft algorithm you're gonna implement here is `tick based`, and internally to the `raft` package time is represented by an abstract "tick", the upper application will call `RawNode.Tick()` at regular intervals to drive the election and heartbeat timeout
This part can be broken down into 3 steps, including:

1. Leader election
2. Log replication
3. Raw node interface

#### Implement the Raft algorithm
`raft.Raft` in `raft/raft.go` provides the core of the Raft algorithm including message handling, driving the logic clock, etc.
#####  Leader election
To implement leader election, you may want to start with `raft.Raft.tick()` which is used to advance the internal logical clock by a single tick and hence drive the election timeout or heartbeat timeout. If you need to send out a message just push it to `raft.Raft.msgs` and all messages the raft received will be pass to `raft.Raft.Step()`, which  is the entrance of message handling, now you can handle some messages like `MsgRequestVote`, `MsgHeartbeat`  and their response. And also implement functions like `raft.Raft.becomeXXX` which used to update the raft internal state when the raft’s role changes.

> This part only need to implement

##### Log replication
To implement log replication, you may want to start with handling `MsgAppend` and `MsgAppendResponse` on both sender and receiver side. Checkout `raft.RaftLog` in `raft/log.go` which is a helper struct that help you manage the raft log, in here you also need to interact with the upper application by the `Storage` interface define in `raft/storage.go` to get the persisted data like log entries and snapshot.
#### Implement the raw node interface
`raft.RawNode` in `raft/rawnode.go` is the interface we interact with the upper application, `raft.RawNode` contains `raft.Raft` and provide some wrapper functions like `RawNode.Tick()`and `RawNode.Step()`, also `raft.RawNode` provide `RawNode.Propose()` to propose new raft log. Another important struct `Ready` is also defined here, in `raft.Raft` when we handling message or advances logical clock, some interaction with the upper allipcation like sending message, stabling and applying logs, etc, are not happen immediately, instead these interaction are encapsulated in `Ready` and return by `RawNode.Ready()` to the upper application, after handling the returned `Ready`, the upper application also need to calling some functions like `RawNode.Advance()` to update `raft.Raft`'s internal state.
The upper application may use raft like:
```
for {
  select {
  case <-s.Ticker:
    Node.Tick()
  default:
    if Node.HasReady() {
      rd := Node.Ready()
      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
      send(rd.Messages)
      if !raft.IsEmptySnap(rd.Snapshot) {
        processSnapshot(rd.Snapshot)
      }
      for _, entry := range rd.CommittedEntries {
        process(entry)
      }
      s.Node.Advance(rd)
    }
}
```
Here are some hint on this part:
Add any state you need to `raft.Raft`, `raft.RaftLog`, `raft.RawNode` and message on `eraftpb.proto`
The tests assume that the first time start raft should have term 0
The tests assume that the new elected leader should append a noop entry on its term
Put common checks like the message's term on `raft.Step()` instead of  `raft.stepLeader()`, `raft.stepFollower()`, etc.
Handling messages for all roles like `RequestVote` on `raft.Step()` instead of  `raft.stepLeader()`, `raft.stepFollower()`, etc.
The log entries append are quite different between leader and non-leader, there are different sources, checking and handling, be careful with that.
Don’t forget the election timeout should be different between peers.
Some wrapper functions in `rawnode.go` can implement with `raft.Step(local message)`
When start a new raft, get the last stabled state from `Storage` to initialize `raft.Raft` and `raft.RaftLog`





### Notes

#### RaftLog

1. truncated就是snapshot，snapshot和first始终保证是连续的。 
2. pendingSnapshot用于接受从Leader发送过来的snapshot，和当前的entris的可能连续或者重合
3. Lab2a中不需要处理与snapshot以及压缩相关的内容(即将first-applied中的数据整合到snapshot中去)
4. hint
   1. 需要记录snapshot的最大index和对应的term。
   2. entry的index不是在entries中的位置。访问entries的数据使用index-lastSnapIndex来访问
   3. entries可能为空
   4. 需要额外实现的RaftLog对象的方法
      1. FirstIndex() 返回entries中第一个entry的Index
      2. LastTerm() 返回entries中最后一个entry的Term
      3. slice(s,e) 返回entries中从s，e的切片
      4. Append(index, term, committed uint64, entries ...pb.Entry) 用于实现AppendRPC，根据index、term的匹配情况，将entries放入到当前entries的合适位置
      5. Commit(index, term uint64)、Stabled(index, term uint64)、Applied(index uint64)根据index和term的匹配情况来更新committed、stabled和applied

### Raft

1. 给的那一本参考论文中部分细节容易忘记或者没有说出
   1. 成为Leader后需要将一个空entry加入Raftlog的entries中
   2. 忽略term小于当前term的消息
   3. 只要append消息的term不小于当前term，当前节点就成为消息发送者的follower
2. 不需要处理leadTransferee和PendingConfIndex
3. 使用electionTimeout用来判断heartbeat超时
4. 对于MessageType_MsgHup、MessageType_MsgBeat、MessageType_MsgPropose在step中需要特殊处理，因为在测试程序中对于这一类消息的Term字段都没有设置值，他们默认是0 。对他们需要无视Term字段。
5. 注意send函数，它的逻辑我无法理解，但是暂时按他需要的设置term吧。
6. 目前的实现需要处理除了MessageType_MsgTransferLeader和MessageType_MsgTimeoutNow之外的所有message
7. message中注意的字段
   1. Reject AppendRpc和投票RPC的success和voteGranted，代表操作结果。
   2. RejectHint 不用处理，忽略
   3. Index、LogTerm  AppendRpc中的prevLogIndex 、prevLogTerm
   4. Term 发送者的Term 
> send方法中对term字段做的处理以及local  Message类型让我对term字段有点迷惑。
8. hint
   1. 在step函数在最前面对消息的term进行判断，因为这是所有服务器公共的部分
   2. 建议实现的方法
      1. reset方法 重置一个raft结构
      2. 广播操作 用于leader向所有节点发送消息
      3. VoteThreshold 返回达成一致需要的节点数
      4. 统计投票 统计获得的选票数
      5. AppendEntry 向raftlog中添加entries，并更新match、next以及raftlog的commit
      6. SoftState、HardState 将raft对应的信息封装成对应的SoftState和HardState



### Part B

In this part you will build a fault-tolerant key-value storage service using the Raft module implemented in part A.  Your key/value service will be a replicated state machine, consisting of several key/value servers that use Raft for replication. Your key/value service should continue to process client requests as long as a majority of the servers are alive and can communicate, in spite of other failures or network partitions. 
In lab1 you have implemented a standalone kv server, so you should already be familiar with the kv server API and `Storage` interface.  
Before introducing the code, you need to understand three terms first: `Store`, `Peer` and `Region` which are defined in `proto/proto/metapb.proto`. 
Store stands for an instance of tinykv-server
Peer stands for a Raft node which is running on a Store
Region is a collection of Peers, also called Raft group

Now you can simply think that there would be only one Peer on a Store and one Region in a cluster. They will be further introduced in lab3.
#### The Code
First, the code that you should take a look at is  `RaftStorage` located in `kv/storage/raft_storage/raft_server.go` which also implements the `Storage` interface. It mainly creates a `RaftBatchSystem` to drive Raft.  When calling the `Reader` or `Write` function,  it actually sends a raft command defined in `proto/proto/raft_cmdpb.proto` with four basic command types(Get/Put/Delete/Snap) to Raft and returns the response after Raft commits and applies the command.
Then, here comes the core of TinyKV — raftstore. The structure is a little complicated, you can read the reference of TiKV to give you a better understanding of the design:
https://pingcap.com/blog-cn/the-design-and-implementation-of-multi-raft/#raftstore  (Chinese Version)
https://pingcap.com/blog/2017-08-15-multi-raft/#raftstore (English Version)
TODO: raftstore 各种 worker 之间的关系图 
The entrance of raftstore is `RaftBatchSystem`, see `kv/raftstore/batch_system.go`.  It starts some workers to handle specific tasks asynchronously,  and most of them aren’t used now so you can just ignore them. All you need to focus on is `raftWorker`.
The whole process is divided into two parts: raft worker polls `raftCh` to get the messages, the messages includes the base tick to drive Raft module and Raft commands to be proposed as Raft entries; it gets and handles ready from Raft module, including send raft messages, persist the state, apply the committed entries. Once committed, return the response to clients.
#### Implement peer storage
Peer storage is what you interact with through the `Storage` interface in part A, but in addition to the raft log and snapshot, peer storage also manages other persisted metadata which is very important to  restore the consistent state machine after restart. Moreover, there are three important state defined in `proto/proto/raft_serverpb.proto`:
RaftLocalState: Used to store HardState of the current Raft and the last Log Index.
RaftApplyState: Used to store the last Log index that Raft applies and some truncated Log information.
RegionLocalState: Used to store Region information and the corresponding Peer state on this Store. Normal indicates that this Peer is normal, Applying means this Peer hasn’t finished the apply snapshot operation and Tombstone shows that this Peer has been removed from Region and cannot join in Raft Group.
These should be created and updated in `PeerStorage`. When creating PeerStorage, it gets the previous RaftLocalState, RaftApplyState and last_term of this Peer from underlying engine. The value of both RAFT_INIT_LOG_TERM and RAFT_INIT_LOG_INDEX is 5 (as long as it's larger than 1). These will be cached to memory for the subsequent quick access.
These states are stored in two badger instances: raftdb and kvdb. raftdb stores raft log and `RaftLocalState`; kvdb stores key-value data in different column families, `RegionLocalState` and `RaftApplyState`. The format is as below and some helper functions are provided in `kv/raftstore/meta`, and set them to badger with writebatch.SetMeta()

| Key              | KeyFormat                        | Value            | DB   |
| ---------------- | -------------------------------- | ---------------- | ---- |
| raft_log_key     | 0x01 0x02 region_id 0x01 log_idx | Entry            | raft |
| raft_state_key   | 0x01 0x02 region_id 0x02         | RaftLocalState   | raft |
| apply_state_key  | 0x01 0x02 region_id 0x03         | RaftApplyState   | kv   |
| region_state_key | 0x01 0x03 region_id 0x01         | RegionLocalState | kv   |

The code you need to implement in this part is only one function:  `PeerStorage.SaveReadyState`, what this function does is to save the data in `raft.Ready` to badger, including: apply snapshot, append log entries and save the hard state.
 To apply snapshot, first unmarshal `eraftpb.Snapshot.Data` at `raft.Ready.Snapshot` to `rspb.RaftSnapshotData` which contains the new region meta and along with the `eraftpb.Snapshot.Metadata`, you can now update the peer storage’s memory state like `RaftLocalState`, `RaftApplyState` and `RegionLocalState`, also don’t forget to persist these state to badger and remove stale state from badger. Besides, you also need to update `PeerStorage.snapState` to `snap.SnapState_Applying` and send `runner.RegionTaskApply` task to region worker through `PeerStorage.regionSched` and  wait until region worker finish.
To append log  entries, simply save all log entries at `raft.Ready.Entries` to badger and delete any previously appended log entries which will never be committed. Also update the peer storage’s `RaftLocalState` and save it to badger.
To save the hard state is also very easy, just update peer storage’s `RaftLocalState.HardState` and save it to badger.
Here are some hint on this part:
Use `WriteBatch` to save these states at once.
See other functions at `peer_storage.go` for how to read and write these states.
#### Implement Raft ready process
In lab2 part A, you have built a tick based Raft module. Now you need to write the outer process to drive it. Most of the code is already implemented under `kv/raftstore/peer_msg_handler.go` and `kv/raftstore/peer.go`.  So you need to learn the code and finish the logic of `proposeRaftCommand` and `HandleRaftReady`. Here are some interpretations of the framework.  
The Raft `RawNode` is already created with `PeerStorage` and stored in `peer`. In the raft worker, you can see that it takes the `peer` and wraps it by `peerMsgHandler`.  The `peerMsgHandler` mainly has two functions: one is `HandleMsgs` and the other is `HandleRaftReady`.
`HandleMsgs` processes all the messages received from raftCh, including `MsgTypeTick` which calls `RawNode.Tick()`  to drive the Raft, `MsgTypeRaftCmd` which wraps the request from clients and `MsyTypeRaftMessage` which is the messages transported between Raft peers. All the message types are defined in `kv/raftstore/message/msg.go`. You can check it for detail and some of them will be used in later sections.
After the message is processed, the Raft node should have some state updates. So `HandleRaftReady` should get the ready from Raft module and do corresponding actions like persisting log entries, applying committed entries and sending raft messages to other peers through the network. 
Here are some hints for this step:
`PeerStorage` implements the `Storage` interface of  Raft module, you should use the provided method  `SaveRaftReady()` to persist the Raft related states. 
Use `WriteBatch` in `engine_util` to make multiple writes atomically, for example, you need to make sure apply the committed entires and update applied index in one write batch. 
Use `Transport` to send raft messages to other peers, it’s in the `GlobalContext`, 
The server should not complete a get RPC if it is not part of a majority and do not has up-to-date data. You can just put the get operation into the log, or implement the optimization for read-only operations that is described in Section 8 in the Raft paper. 
You can apply the committed Raft log entries in an asynchronous way just like TiKV does. It’s not necessary, though a big challenge to improve performance.
Record the callback of the command when proposing, and return the callback after applying.
For the snap command response, should set badger Txn to callback explicitly.
After this the whole process of a read or write would be like this:
Clients calls RPC RawGet/RawPut/RawDelete/RawScan
RPC handler calls `RaftStorage` related method
`RaftStorage` sends a Raft command request to raftstore, and waits for the response
`RaftStore` proposes the Raft command request as a Raft log
Raft module appends the log, and persist by `PeerStorage`
Raft module commits the log
Raft worker executes the Raft command when handing Raft ready, and returns the response by callback
`RaftStorage` receive the response from callback and returns to RPC handler
RPC handler does some actions and returns the RPC response to clients.
You should run `make lab2b` to pass all the tests.

### Notes

1. Store stands for an instance of tinykv-server
> 改成 Store stands for an instance of tinykv-server that stores  keys for a specified range
2. Now you can simply think that there would be only one Peer on a Store and one Region in a cluster. They will be further introduced in lab3.
> 没有说过cluster是什么
3. 对于在raftdb and kvdb中按照文档中给出的格式存储元数据花了一段时间才反应过来。
4. 设计的各种结构题中总是有很多重复存储的信息，增加了理解的难度
> Raft worker executes the Raft command when handing Raft ready, and returns the response by callback
5. 这句话让我以为kv命令的处理以及callback的设置都是由raft worker完成了，在测试的过程中才发现需要自己设置。
6. 可以看一下raftworker相关的代码。
> Do not forget to `writeApplyState()` is provided for you to persist apply state
7. 没有这个函数。
8. 对于proposals没有任何提示的逻辑？
> The server should not complete a get RPC if it is not part of a majority and do not has up-to-date data. You can just put the get operation into the log, or implement the optimization for read-only operations that is described in Section 8 in the Raft paper. 
9. 所以实际上对Get并不需要特殊处理？因为Raft的leader直接保证了大多数的一致性，并且我们只通过leader来和上层的cluster以及raft storage进行交互。
10. GenericTest真的太长了，并且测试的逻辑在debug的时候也不方便。


### Part C
As things stand now with your code, it's not practical for a long-running server to remember the complete Raft log forever. Instead, the server will check the number of Raft log, and discard log entries exceeding the threshold from time to time.
In this part, you will implement the Snapshot handling based on the above two part implementation. Generally, Snapshot is just a raft message like AppendEntrie used to replicate data to follower, what make it different is its size, Snapshot contains the whole state machine data in some point of time, and to build and send such a big message at once will consume many resource and time, which may block the handling of other raft message, to amortize this problem, Snapshot message will use an independent connect, and split the data into chunks to transport. That’s the reason why there is a snapshot RPC API for TinyKV service. If you are interested in the detail of sending and receiving, check `snapRunner` and the reference https://pingcap.com/blog-cn/tikv-source-code-reading-10/

#### The Code
All you need to change is based on the code written in part A and part B.
#### Implement in Raft
Although we need some different handling for Snapshot messages, in the perspective of raft algorithm there should be no difference. See the definition of  `eraft.Snapshot` at proto file, the `data` field on `eraft.Snapshot` does not represent the actual state machine data but some metadata used for the upper application you can ignore it for now. When the leader needs to send a Snapshot message to a follower, it can call `Storage.Snapshot()` to get a `eraft.Snapshot`, then send the snapshot message like other raft messages. How the state machine data is actually built and sent are implemented by the raftstore, it will be introduced in the next step. You can assume that once `Storage.Snapshot()` returns successfully, it’s safe for Raft leader to the snapshot message to the follower, and follower should call `handleSnapshot` to handle it, which namely just restore the raft internal state like term, commit index and membership information, ect, from the `eraft.SnapshotMetadata` in the message, after that, the procedure of snapshot handling is finish.

#### Implement in raftstore
In this step, you need to learn two more workers of raftstore — raftlog-gc worker and region worker.
Raftstore checks whether it needs to gc log from time to time based on the config `RaftLogGcCountLimit`, see `onRaftGcLogTick()`. If yes, it will propose a raft admin command `CompactLogRequest`. You need to process the admin command in apply worker,  namely updating the `RaftTruncatedState` which is in the `RaftApplyState`, and return the apply res to raft worker to schedule a task to raftlog-gc worker located in `kv/runner/raftlog_gc.go`. Raftlog-gc worker will do the actual log deletion work asynchronously. 
Then due to the log compaction, Raft module maybe needs to send a snapshot. `PeerStorage` implements  `Storage.Snapshot()`. TinyKV generates snapshots and applies snapshots in region worker. When calling `Snapshot()`, it actually sends a task `TaskTypeRegionGen` to the region worker, see `ScheduleGenerateSnapshot()`. The message handler of region worker is located in `kv/runner/region_task.go`. It scans the underlying engines to generate a snapshot, and sends snapshot metadata by channel. At the next time of Raft calling `Snapshot`, it checks  whether the snapshot generating is finished. If yes, Raft should send the snapshot message to other peers, and the snapshot sending and receiving work is handled by `kv/storage/raft_storage/snap_runner.go`. You don’t need to dive into the details, only should know the snapshot message will be handled by `onRaftMsg` after the snapshot is received. Also, when you are sure to apply the snapshot, call `ScheduleApplyingSnapshot()` to send a task `TaskTypeRegionApply` to the region worker, and it will set the job status when finished.
Then the snapshot will reflect in the next Raft ready, so the task you should do is to modify the raft ready process to handle the case of snapshot. 
Here are some hints for this step:
If there are some committed entries to be executed in the apply worker, do not apply state, see `ReadyToHandlePendingSnap`.
Do not handle next Raft ready before finishing apply snapshot

#### Notes  

1. eraft.Snapshot doesn't exist
2. 提示要完成的部分是不是太少了。（2C）