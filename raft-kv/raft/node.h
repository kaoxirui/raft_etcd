/*
 * @Author: xirui kao 
 * @Date: 2024-12-03 20:40:35 
 * @Last Modified by: xirui kao
 * @Last Modified time: 2024-12-03 22:11:25
 */
#pragma once

#include "config.h"
#include "proto.h"
#include "raft.h"
#include "raft_status.h"

namespace kv {

typedef uint8_t SnapshotStatus;

static const SnapshotStatus SnapshotFinish = 1;
static const SnapshotStatus SnapshotFailure = 2;

struct PeerContext {
    uint64_t id;
    std::vector<uint8_t> context;
};

class Node {
public:
    //!为什么不是虚析构函数
    ~Node() = default;

    // raft内部有两个计时：心跳和选举。raft内部没有设计定时器，计时就是由这个接口驱动的，每调用一次
    // 内部计数一次，这就是raft的计时原理。所以raft的计时粒度取决于调用者，这样的设计使得raft的
    // 适配能力更强。
    virtual void tick() = 0;

    //使node转换到候选状态并开始竞选成为leader。该函数的调用会触发节点进行选举
    virtual Status campaign() = 0;

    //?什么是提议：封装成一条日志然后广播到所有及节点，超过半数以上不拒绝就算提议通过
    //这是raft对外提供的非常核心的接口，该接口把使用者的data通过日志广播到所有节点，当然这个广播需要leader执行
    //如果当前节点不是leader，则会把日志转发给leader
    // 需要注意：该函数虽然返回错误代码，但是返回nil不代表data已经被超过半数节点接收了。因为提议
    // 是一个异步的过程，该接口的返回值只能表示提议这个操作是否被允许，比如在没有选出Leader的情况下
    // 是无法提议的。而提议的数据被超过半数的节点接受是通过下面的Ready()获取的。
    virtual Status propose(std::vector<uint8_t> data) = 0;

    //配置数据对于raft需要保证所有节点是相同的，所以raft也采用了提议的方法配置数据的一致性
    virtual Status propose_conf_change(const proto::ConfChange &cc) = 0;

    //raft本身作为一种算法并没实现网络相关的实现，需要使用者自己实现，也使得raft适配能力更强
    //message就是raft节点间通信的协议，所以使用者在实现网络部分会收到其他节点发来的消息
    //所以把其他节点发来的消息需要通过step()函数送到raft内部处理
    virtual Status step(proto::MessagePtr msg) = 0;

    //日志会从提交状态转移到应用状态，这个转换过程就是通过ready()函数实现的
    //使用者从该函数获取ReadyPtr然后从ReadyPtr获取ready数据，使用者再把数据应用到系统中
    //ready中不只有已提交的日志，还有其他内容
    virtual ReadyPtr ready() = 0;

    virtual bool has_ready() = 0;

    //advance函数和ready配合使用
    //Ready() <-chan Ready
    // Advance()函数是和Ready()函数配合使用的，当使用者处理完Ready数据后调用Advance()接口
    // 告知raft，这样raft才能向chan中推新的Ready数据。也就是使用者通过<-chan Ready获取的数据
    // 处理完后必须调用Advance()接口，否则将会阻塞在<-chan Ready上，因为raft此时也通过另一个
    // chan等待Advance()的信号。
    virtual void advance(ReadyPtr ready) = 0;

    //propose_conf_change()提议修改配置，当这个提议产生的日志被使用者应用时就需要通过调用这个接口
    //把配置应用到本地节点。常规日志被应用到修改使用者的状态，而配置修改日志更新的是raft自己的状态
    virtual proto::ConfStatePtr apply_conf_change(const proto::ConfChange &cc) = 0;

    //把leader转移到指定的peer
    virtual void transfer_leadership(uint64_t lead, ino64_t transferee) = 0;

    // 在了解该接口之前，读者需要了解linearizable read的概念，就是读请求需要读到最新的已经
    // commit的数据。我们知道etcd是可以在任何节点读取数据的，如果该节点不是leader，那么数据
    // 很有可能不是最新的，会造成stale read。即便是leader也有可能网络分区，leader还自认为
    // 自己是leader，这里就会涉及到一致性模型，感兴趣的读者可以阅读笔者的《一致性模型》。
    // 书归正传，ReadIndex就是使用者获取当前集群的最大的提交索引，此时使用者只要应用的最大
    // 索引大于该值，使用者就可以实现读取是最新的数据。说白了就是读之前获取集群的最大提交索引，
    // 然后等节点应用到该索引时再把系统中对应的值返回给用户。其中rctx唯一的标识了此次读操作，
    // 索引会通过Ready()返回给使用者，获取最大提交索引是一个异步过程。
    virtual Status read_index(std::vector<uint8_t> rctx) = 0;

    //获取raft的状态
    virtual RaftStatusPtr raft_status() = 0;

    //报告raft指定的节点上次发送没有成功
    virtual void report_unreachable(uint64_t id) = 0;

    virtual void report_snapshot(uint64_t id, SnapshotStatus status) = 0;
    //停止raft，相当于close的概念
    virtual void stop() = 0;

    static Node *start_node(const Config &conf, const std::vector<PeerContext> &peers);

    static Node *restart_node(const Config &conf);
};

class RawNode : public Node {
public:
    explicit RawNode(const Config &conf, const std::vector<PeerContext> &peers);
    explicit RawNode(const Config &conf);
    ~RawNode() = default;

    void tick() final;

    Status campaign() final;

    Status propose(std::vector<uint8_t> data) final;

    Status propose_conf_change(const proto::ConfChange &cc) final;

    Status step(proto::MessagePtr msg) final;

    ReadyPtr ready() final;

    bool has_ready() final;

    void advance(ReadyPtr rd) final;

    proto::ConfStatePtr apply_conf_change(const proto::ConfChange &cc) final;

    void transfer_leadership(uint64_t lead, ino64_t transferee) final;

    Status read_index(std::vector<uint8_t> rctx) final;

    RaftStatusPtr raft_status() final;

    void report_unreachable(uint64_t id) final;

    void report_snapshot(uint64_t id, SnapshotStatus status) final;

    void stop() final;

public:
    RaftPtr raft_;
    SoftStatePtr prev_soft_state_;
    proto::HardState prev_hard_state_;
};

} // namespace kv
