#pragma once

#include <memory>
#include <vector>

namespace kv {
enum ProgressState {
    ProgressStateProbe = 0,
    ProgressStateReplicate = 1,
    ProgressStateSnapshot = 2
};

const char *progress_state_to_string(ProgressState state);

class InFlights {
public:
    /*
    并没有给buffer申请内存。size是容量，但是实际运行过程中对于buffer的使用量可能远远低于容量，此时申请size
    大小的内存明显是一种浪费。所以采用动态调整buffer大小的方法

    为什么实际运行过程中对于buffer的使用量可能远远低于容量？例如，容量是256，但是即使用的量可能只有16。

    首先，日志是以消息为粒度发送的，一个消息可以携带多个日志
    其次，inflights记录的是消息中最大日志的索引，所以它记录的是飞行中的消息的数量，折算成飞行中日志的数量就更多了
    第三，正常情况下日志发送到节点到接收节点的回复是非常快的，几毫秒到几十毫秒
    第四，使用者在不频繁执行写操作的情况下节点间的IO性能基本能够满足写IO，inflights缓冲效果就不明显了
*/
    explicit InFlights(uint64_t max_inflights_msgs)
        : start(0), count(0), size(static_cast<uint32_t>(max_inflights_msgs)) {}
    void reset() {
        start = 0;
        count = 0;
    }
    bool is_full() const { return size == count; }

    void add(uint64_t inflight);

    // 把小于等于to的日志全部释放，为什么不是把等于to的释放掉？这个很简单，如果节点回复的消息丢包了，那么
    // 就会造成部分日志无法释放。raft里日志是有序的，收到了节点回复消息的使用为n，那就说明节点已经收到了
    // n以前的全部日志，所以可以把之前的全部释放掉。
    void free_to(uint64_t to);

    void free_first_one();
    //the starting index of the buffer
    uint32_t start;
    //number of inflights in the buffer
    uint64_t count;
    //the size of the buffer
    uint32_t size;
    std::vector<uint64_t> buffer;
};
// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
class Progress {
public:
    explicit Progress(uint64_t max_inflights)
        : match(0), next(0), state(ProgressState::ProgressStateProbe), paused(false),
          pending_snapshot(0), recent_active(false), inflights(new InFlights(max_inflights)),
          is_learner(false) {}

    //进入复制状态
    void become_replicate();

    void become_probe();

    void become_snapshot(uint64_t snapshoti);

    void reset_state(ProgressState state);

    std::string string() const;

    bool is_paused() const;

    void set_pause() { this->paused = true; }

    void resume() { this->paused = false; }

    // maybe_update returns false if the given n index comes from an outdated message.
    // Otherwise it updates the progress and returns true.
    // 更新Progress的状态，为什么有个maybe呢？因为不确定是否更新，raft代码中有很多maybeXxx系列函数，
    // 大多是尝试性操作，毕竟是分布式系统，消息重发、网络分区可能会让某些操作失败。这个函数是在节点回复
    // 追加日志消息时被调用的，在反馈消息中节点告知Leader日志消息中最有一条日志的索引已经被接收，就是
    // 下面的参数n，Leader尝试更新节点的Progress的状态。
    bool maybe_update(uint64_t n);

    void optimistic_update(uint64_t n) { next = n + 1; }

    // maybe_decr_to returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
    bool maybe_decreases_to(uint64_t rejected, uint64_t last);

    // need_snapshot_abort returns true if snapshot progress's match
    // is equal or higher than the pending_snapshot.
    bool need_snapshot_abort() const;

    void snapshot_failure() { pending_snapshot = 0; }
    //follower确认接收的最大索引
    //（0，match】是节点的已经接收到的日志
    uint64_t match;
    //下一次发送日志起始索引
    //（0，next）的日志已经发送给节点了
    uint64_t next;
    // state defines how the leader should interact with the follower.
    //
    // When in ProgressStateProbe, leader sends at most one replication message
    // per heartbeat interval. It also probes actual progress of the follower.
    //
    // When in ProgressStateReplicate, leader optimistically increases next
    // to the latest entry sent after sending replication message. This is
    // an optimized state for fast replicating log entries to the follower.
    //
    // When in ProgressStateSnapshot, leader should have sent out snapshot
    // before and stops sending any replication message.
    ProgressState state;
    // paused is used in ProgressStateProbe.
    // When Paused is true, raft should pause sending replication message to this peer.
    bool paused;
    // pending_snapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    //快照状态时，快照索引值
    uint64_t pending_snapshot;
    // pending_snapshot is used in ProgressStateSnapshot.
    // If there is a pending snapshot, the pendingSnapshot will be set to the
    // index of the snapshot. If pendingSnapshot is set, the replication process of
    // this Progress will be paused. raft will not resend snapshot until the pending one
    // is reported to be failed.
    bool recent_active;
    // inflights is a sliding window for the inflight messages.
    // Each inflight message contains one or more log entries.
    // The max number of entries per message is defined in raft config as MaxSizePerMsg.
    // Thus inflight effectively limits both the number of inflight messages
    // and the bandwidth each Progress can use.
    // When inflights is full, no more message should be sent.
    // When a leader sends out a message, the index of the last
    // entry should be added to inflights. The index MUST be added
    // into inflights in order.
    // When a leader receives a reply, the previous inflights should
    // be freed by calling inflights.freeTo with the index of the last
    // received entry.
    //滑动窗口，控制在飞日志条目的数量和带宽
    /*
    用于控制在飞的日志条目的数量和带宽。
    他的主要作用是记录领导者发送给跟随着但尚未确认的日志条目索引，
    并限制在飞日志条目的数量，以避免网络用晒或跟随着处理不过来的情况
    */
    std::shared_ptr<InFlights> inflights;
    bool is_learner;
};
typedef std::shared_ptr<Progress> ProgressPtr;
}; // namespace kv