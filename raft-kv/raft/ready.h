/*
 * @Author: xirui kao 
 * @Date: 2024-12-12 21:52:32 
 * @Last Modified by:   xirui kao 
 * @Last Modified time: 2024-12-12 21:52:32 
 */
#pragma once
#include "proto.h"
#include "readonly.h"
namespace kv {
enum RaftState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3,
};

//节点运行状态，包括leader是谁，自己什么角色
struct SoftState {
    explicit SoftState(uint64_t lead, RaftState state) : lead(lead), state(state) {}
    bool equal(const SoftState &ss) const { return lead == ss.lead && state == ss.state; }
    uint64_t lead;
    RaftState state;
};
typedef std::shared_ptr<SoftState> SoftStatePtr;
class Raft;

struct Ready {
    Ready() : must_sync(false) {}
    explicit Ready(std::shared_ptr<Raft> raft, SoftStatePtr pre_soft_state,
                   const proto::HardState &pre_hard_state);
    bool contains_updates() const;
    bool equal(const Ready &rd) const;
    uint64_t applied_cursor() const;

    //节点运行状态，包括leader是谁，自己什么角色
    SoftStatePtr soft_state;

    //集群运行状态，包括term，提交索引和leader
    //硬状态需要使用者持久化，而软状态不需要
    proto::HardState hard_sate;

    //readstate是包含索引和rctx（就是readindex()函数的参数）的结构，意义是某一时刻的集群最大提交索引
    //至于这个时刻使用者用于实现linearizable read就是另一回事了，其中rctx就是某一时刻的唯一标识
    //这个参数就是Node.ReadIndex()的结果回调
    std::vector<ReadState> read_state;

    //需要存入可靠存储的日志，从unstable中获取
    std::vector<proto::EntryPtr> entries;

    //需要存入可靠存储的快照，同样来自unstable
    proto::Snapshot snapshot;

    // 已经提交的日志，用于使用者应用这些日志，需要注意的是，CommittedEntries可能与Entries有
    // 重叠的日志，这是因为Leader确认一半以上的节点接收就可以提交。而节点接收到新的提交索引的消息
    // 的时候，一些日志可能还存储在unstable中。
    std::vector<proto::EntryPtr> committed_entries;

    //需要发送给其他节点的消息，raft负责封装消息而不负责发送，发送需要使用者来实现
    std::vector<proto::MessagePtr> messages;

    //硬状态和不可靠日志是否必须同步写入磁盘还是异步写入，也就是使用者必须把数据同步到磁盘后才能调用Advance()
    bool must_sync;
};
typedef std::shared_ptr<Ready> ReadyPtr;

} // namespace kv