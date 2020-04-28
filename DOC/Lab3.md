
### Part A
In this part you will implement membership change and leadership change to the basic raft algorithm, these features are required by the next two parts. Membership change, namely conf change, is used to add or remove peers to the raft group, which can change the quorum of the raft group, so be careful. Leadership change, namely leader transfer, is used to transfer the leadership to another peer, which is very useful for balance.

#### The Code
The code you need to modify is all about `raft/raft.go` and `raft/rawnode.go`, also see `proto/proto/eraft.proto` for new messages you need to handle. And both conf change and leader transfer are triggered by the upper application, so you may want to start at `raft/rawnode.go`.

#### Implement leader transfer
To implement leader transfer, let’s introduce two new message types: `MsgTransferLeader` and `MsgTimeoutNow`. To transfer leadership you need to first call `raft.Raft.Step` with `MsgTransferLeader` message on the current leader, and to ensure the success of transfer, the current leader should first check the qualification of the transferee (namely transfer target) like: is the transferee’s log up to date, etc. If the transferee is not qualified, the current leader can choose to abort the transfer or help the transferee, since abort is not helping, let’s choose to help the transferee. If the transferee’s log is not up to date, the current leader should send a `MsgAppend` message to the transferee and stop accepting new proposes in case we end up  cycling. So if the transferee is qualified (or after the current leader’s help), the leader should send a `MsgTimeoutNow` message to the transferee immediately, and after received a `MsgTimeoutNow` message the transferee should start a new election immediately regardless of its election timeout, with a higher term and up to date log, the transferee have great chance to step down the current leader and become the new leader.
#### Implement conf change
Conf change algorithm you will implement here is not the joint consensus algorithm mentioned in  the extended Raft paper that can add and/or remove arbitrary peers at once, instead it can only add or remove peers one by one, which is more simple and easy to reason about. Moreover, conf change start at calling leader’s  `raft.RawNode.ProposeConfChange` which will propose an entry with `pb.Entry.EntryType` set to `EntryConfChange` and `pb.Entry.Data` set to the input `pb.ConfChange`. When entries with type `EntryConfChange` are committed, you must apply it through `RawNode.ApplyConfChange` with the `pb.ConfChange` in the entry, only then you can add or remove peer to this raft node through `raft.Raft.addNode` and `raft.Raft.removeNode` according to the `pb.ConfChange`.

Hints:
`MsgTransferLeader` message is local message that not come from network
You set the `Message.from` of the `MsgTransferLeader` message to  the transferee (namely transfer target)
To start a new election immediately you can call `Raft.Step` with `MsgHup` message
Call `pb.ConfChange.Marshal` to get bytes represent of `pb.ConfChange` and  put it to `pb.Entry.Data`

#### Notes
