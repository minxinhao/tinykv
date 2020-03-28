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
