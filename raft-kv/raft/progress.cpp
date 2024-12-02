#include "log.h"
#include "progress.h"

namespace kv {
const char *progress_state_to_string(ProgressState state) {
    switch (state) {
        case ProgressStateProbe:
            return "ProgressStateProbe";
        case ProgressStateReplicate:
            return "ProgressStateReplicate";
        case ProgressStateSnapshot:
            return "ProgressStateSnapshot";
        default:
            LOG_FATAL("unkown state %d", state);
    }
}
//向缓冲区添加日志（向池子里放水）
void InFlights::add(uint64_t inflight) {
    if (is_full()) {
        LOG_FATAL("cannot add into a full inflights");
    }
    uint64_t next = start + count;
    if (next >= size) {
        next -= size;
    }
    //如果buffer大小不够，扩容
    if (next >= buffer.size()) {
        uint32_t new_size = buffer.size() * 2;
        if (new_size == 0) {
            new_size = 1;
        } else if (new_size > size) {
            size = new_size;
        }
        buffer.resize(new_size);
    }
    buffer[next] = inflight;
    count++;
}
void InFlights::free_to(uint64_t to) {
    if (count == 0 || to < buffer[start]) {
        //out of the left side of the window
        return;
    }
    //初始化索引和计数器
    uint32_t idx = start;
    size_t i;
    for (i = 0; i < count; i++) {
        //遍历缓冲区的条目，找到第一个大于to的条目，则停止
        if (to < buffer[idx]) {
            break;
        }
        idx++;

        if (idx >= size) {
            idx -= size;
        }
    }
    //减少i个条目并更新新的起始索引start
    count -= i;
    start = idx;
    if (count == 0) {
        //缓冲区为空，将起始索引置为0，避免不必要地增长缓冲区
        start = 0;
    }
}

void InFlights::free_first_one() {
    free_to(start);
}

void Progress::become_replicate() {
    reset_state(ProgressStateReplicate);
    //进入复制状态肯定是收到了探测消息的反馈，此时match会被更新，next从match+1
    next = match + 1;
}

void Progress::become_probe() {
    //如果原始状态是快照，说明快照已经被Peer接收了，那么Next=pendingSnapshot+1，
    // 意思就是从快照索引的下一个索引开始发送。
    if (state == pending_snapshot) {
        //使用临时变量，因为reset会重置pending_snapshot
        uint64_t pending = pending_snapshot;
        reset_state(ProgressStateProbe);
        next = std::max(match + 1, pending + 1);
    } else {
        // 上面的逻辑是Peer接收完快照后再探测一次才能继续发日志，而这里的逻辑是Peer从复制状态转
        // 到探测状态，这在Peer拒绝了日志、日志消息丢失的情况会发生，此时Leader不知道从哪里开始，
        // 倒不如从Match+1开始，因为Match是节点明确已经收到的。
        reset_state(ProgressStateProbe);
        next = match + 1;
    }
}

void Progress::become_snapshot(uint64_t snapshoti) {
    //此时不再发日志给peer，要等到快照结束才发送日志，所以不更新next
    reset_state(ProgressStateSnapshot);
    pending_snapshot = snapshoti;
}

void Progress::reset_state(ProgressState st) {
    paused = false;
    pending_snapshot = 0;
    this->state = st;
    this->inflights->reset();
}

//暂停表示leader不能再向peer发日志消息了，必须等待peer回复打破这个状态
bool Progress::is_paused() const {
    //不同状态下的暂停条件是不同的
    switch (state) {
        case ProgressStateProbe:
            //探测状态下如果已经发送了探测消息，progress就暂停了，一位则不能再发探测消息了
            //前一个消息还没回复，如果节点真的不活跃，发再多也没用
            return paused;
        case ProgressStateReplicate:
            //复制状态下inflights满了就是progress暂停
            return inflights->is_full();
        case ProgressStateSnapshot:
            /*
            快照状态progress就是暂停的，peer正在复制leader发送的块扎后，这个过程相对较大
            且重要，因为所有日志都是基于某一快照基础上的增量，所以快照不完成其他的就是徒劳
        */
            return true;
        default:
            LOG_FATAL("unexpected state");
    }
}

/*
更新match和next属性,更新progress状态，为什么有个maybe？因为不确定是否更新，分布式系统，消息重发，网络分区可能会让某些操作失败
这个函数是在节点回复追加日志消息时被调用的，在反馈消息中节点告知leader日志消息中有一条日志的索引已经被接收，即下面的n
leader尝试更新节点的progress状态
*/

bool Progress::maybe_update(uint64_t n) {
    bool update = false;
    //n比match大才更新
    if (match < n) {
        match = n;
        update = true;
        //这个函数就是把paused设置为false
        //raft可以把日志消息，心跳消息当作探测消息，此处是把日志消息当作探测消息的处理逻辑
        //新的日志肯定会让match更新，只有收到了比match更大的回复才能算是这个节点收到新日志消息
        //其他反馈都可以视为过时消息。
        //比如match=9，新的日志索引是10，只有收到了>=10的反馈才能确定节点收到了当作探测消息的日志
        resume();
    }
    //next是leader认为发送给peer最大的日志索引了，peer怎么可能回复一个比next更大的日志索引
    //这个其实是在系统初始化的是否亦或是每轮选举完成后，新的leader还不知道peer的接收到的最大日志索引
    //所以此时的next还是个初始值
    if (next < n + 1) {
        next = n + 1;
    }
    return update;
}

//当收到peer拒绝的消息的是否使用，参数rejected，last是peer拒绝的最后的日志的索引
//因为消息的无序和重复发送可能会造成peer的拒绝，因为progress通过match记录了先前已经确认收到的索引
//所以这些是不需要调整状态的，如果拒绝超出了progress预料，则名字地降低next0
bool Progress::maybe_decreases_to(uint64_t rejected, uint64_t last) {
    if (state == ProgressStateReplicate) {
        //复制状态下match是有效的，可以通过match判断拒绝的日志是否已经无效了
        if (rejected <= match) {
            //拒绝的日志索引比match小，可能是重复日志的回复，可以忽略
            return false;
        }
        // 源码注释：直接把Next调整到Match+1。源码注释还有一句是如果last更大为什么不用他？
        // last有可能比Match大么？让我们分析一下，因为在复制状态下Leader会发送多个日志信息
        // 给Peer再等待Peer的回复，例如：Match+1，Match+2，Match+3，Match+4，此时如果
        // Match+3丢了，那么Match+4肯定好会被拒绝，此时last应该是Match+2，Next=last+1
        // 应该更合理。但是从peer的角度看，如果收到了Match+2的日志就会给leader一次回复，这个
        // 回复理论上是早于当前这个拒绝消息的，所以当Leader收到Match+4拒绝消息，此时的Match
        // 已经更新到Match+2，如果Peer回复的消息也丢包了Match可能也没有更新。所以Match+1
        // 大概率和last相同，少数情况可能last更好，但是用Match+1做可能更保险一点。
        next = match + 1;
        return true;
    }
    // 源码注释翻译：如果拒绝日志索引不是Next-1，肯定是陈旧消息这是因为非复制状态探测消息一次只
    // 发送一条日志。这句话是什么意思呢，读者需要注意，代码执行到这里说明Progress不是复制状态，
    // 应该是探测状态。为了效率考虑，Leader向Peer发送日志消息一次会带多条日志，比如一个日志消息
    // 会带有10条日志。上面Match+1，Match+2，Match+3，Match+4的例子是为了理解方便假设每个
    // 日志消息一条日志。真实的情况是Message[Match,Match+9]，Message[Match+10,Match+15]，
    // 一个日志消息如果带有多条日志，Peer拒绝的是其中一条日志。此时用什么判断拒绝索引日志就在刚刚
    // 发送的探测消息中呢？所以探测消息一次只发送一条日志就能做到了，因为这个日志的索引肯定是Next-1。

    if (next - 1 != rejected) {
        return false;
    }
    next = std::min(rejected, last + 1);
    if (next < 1) {
        next = 1;
    }
    //因为节点拒绝了日志，如果这个日志是探测消息，那就再探测一次，paused=true的话，leader就不会再发消息了
    resume();
    return true;
}

bool Progress::need_snapshot_abort() const {
    return state == ProgressStateSnapshot && match >= pending_snapshot;
}

} // namespace kv