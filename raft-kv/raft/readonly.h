#pragma once
#include "config.h"
#include "proto.h"
#include <vector>

namespace kv {
//ReadState结构体用于管理只读请求的状态
//Raft协议允许客户端在不需要提交日志条目的情况下查询一致性数据，而ReadState用于跟踪那些只读请求的状态和相关信息
struct ReadState {
    bool equal(const ReadState &rs) const {
        if (index != rs.index) {
            return false;
        }
        return request_ctx == rs.request_ctx;
    }
    //处理只读请求时，raft节点的日志索引位置
    //该索引值确保在该索引之前的所有日志条目都已提交，从而保证了读取操作的一致性
    uint64_t index;
    //只读请求的上下文信息
    std::vector<uint8_t> request_ctx;
};

//管理读索引的状态
//ReadIndexStatus跟踪每个只读请求的相关信息，包括请求本身、对应的日志索引和确认请求的节点

struct ReadIndexStatus {
    proto::Message req;
    uint64_t index;
    //已确认此读索引请求的节点集合。集合中每个元素都是一个节点的ID
    //通过这种方式，raft可以跟踪哪些节点已经确认了该读索引请求
    std::unordered_set<uint64_t> acks;
};
typedef std::shared_ptr<ReadIndexStatus> ReadIndexStatusPtr;

struct ReadOnly {
    explicit ReadOnly(ReadOnlyOption option) : option(option) {}

    // last_pending_request_ctx returns the context of the last pending read only
    // request in readonly struct.
    void last_pending_request_ctx(std::vector<uint8_t> &ctx);

    uint32_t recv_ack(const proto::Message &msg);

    std::vector<ReadIndexStatusPtr> advance(const proto::Message &msg);

    void add_request(uint64_t index, proto::MessagePtr msg);

    ReadOnlyOption option;
    //在pendingReadIndex维护了消息ID与对应请求readIndexStatus实例的映射
    std::unordered_map<std::string, ReadIndexStatusPtr> pending_read_index;
    //记录了MsgReadIndex请求对应的消息ID，这样可以保证MsgReadIndex的顺序
    std::vector<std::string> read_index_queue;
};
typedef std::shared_ptr<ReadOnly> ReadOnlyPtr;

} // namespace kv
