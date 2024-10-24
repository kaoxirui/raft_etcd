#pragma once
#include <stdint.h>
#include <vector>
#include "status.h"
#include <limits>

namespace kv {
enum ReadOnlyOption {
    ReadOnlySafe = 0,
    ReadOnlyLeaseBased = 1,
};
struct Config {
    explicit Config()
        : id(0), election_tick(0), heartbeat_tick(0), applied(0), max_size_per_msg(0),
          max_committed_size_per_ready(0), max_uncommitted_entries_size(0), max_inflight_msgs(0),
          check_quorum(false), pre_vote(false), read_only_option(ReadOnlySafe),
          disable_proposal_forwarding(false) {}
    uint64_t id;
    //包含所有nodes的id
    std::vector<uint64_t> peers;
    std::vector<uint64_t> learners;
    //建议election_tick=10*heartbeat_tick
    uint32_t election_tick;
    uint32_t heartbeat_tick;
    //StoragePtr storage
    //最后应用的索引
    uint64_t applied;
    //限制每个附加消息的最大字节大小
    // max_size_per_msg limits the max byte size of each append message. Smaller
    // value lowers the raft recover cost(initial probing and message lost
    // during normal operation). On the other side, it might affect the
    // throughput during normal replication. Note: math.MaxUint64 for unlimited,
    // 0 for at most one entry per message.
    uint64_t max_size_per_msg;
    //限制可以应用的已提交条目大小
    // max_committed_size_per_ready limits the size of the committed entries which
    // can be applied.
    uint64_t max_committed_size_per_ready;
    // max_uncommitted_entries_size limits the aggregate byte size of the
    // uncommitted entries that may be appended to a leader's log. Once this
    // limit is exceeded, proposals will begin to return ErrProposalDropped
    // errors. Note: 0 for no limit.
    uint64_t max_uncommitted_entries_size;
    // max_inflight_msgs limits the max number of in-flight append messages during
    // optimistic replication phase. The application transportation layer usually
    // has its own sending buffer over TCP/UDP. Setting MaxInflightMsgs to avoid
    // overflowing that sending buffer.
    uint64_t max_inflight_msgs;
    bool check_quorum;
    bool pre_vote;
    ReadOnlyOption read_only_option;

    //?什么用
    bool disable_proposal_forwarding;

    Status validate();
};

} // namespace kv