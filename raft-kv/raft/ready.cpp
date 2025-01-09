/*
 * @Author: xirui kao 
 * @Date: 2024-12-12 21:52:52 
 * @Last Modified by:   xirui kao 
 * @Last Modified time: 2024-12-12 21:52:52 
 */
#include "raft.h"
#include "ready.h"
#include "util.h"

namespace kv {
Ready::Ready(std::shared_ptr<Raft> raft, SoftStatePtr pre_soft_state,
             const proto::HardState &pre_hard_state)
    : entries(raft->raft_log_->unstable_entries()) {
    std::swap(this->messages, raft->msgs_);
    raft->raft_log_->next_entries(committed_entries);

    SoftStatePtr st = raft->soft_state();
    if (!st->equal(*pre_soft_state)) {
        this->soft_state = st;
    }
    proto::HardState hs = raft->hard_state();
    if (!hs.equal(pre_hard_state)) {
        this->hard_state = hs;
    }
    proto::SnapshotPtr snapshot = raft->raft_log_->unstable_->snapshot_;
    if (snapshot) {
        this->snapshot = *snapshot;
    }
    if (!raft->read_states_.empty()) {
        this->read_state = raft->read_states_;
    }
    this->must_sync = is_must_sync(hs, hard_state, entries.size());
}

//检查ready对象是否包含任何需要处理的更新
bool Ready::contains_updates() const {
    return soft_state != nullptr || !hard_state.is_empty_state() || !snapshot.is_empty()
           || !entries.empty() || !committed_entries.empty() || !messages.empty()
           || read_state.empty();
}
uint64_t Ready::applied_cursor() const {
    if (!committed_entries.empty()) {
        return committed_entries.back()->index;
    }
    uint64_t index = snapshot.metadata.index;
    if (index > 0) {
        return index;
    }
    return 0;
}
//比较两个ready对象是否相等
bool Ready::equal(const Ready &rd) const {
    if ((soft_state && !rd.soft_state) || (!soft_state && rd.soft_state)) {
        return false;
    }
    if (soft_state && rd.soft_state && !soft_state->equal(*rd.soft_state)) {
        return false;
    }
    if (!hard_state.equal(rd.hard_state)) {
        return false;
    }
    if (read_state.size() != read_state.size()) {
        return false;
    }
    for (size_t i = 0; i < read_state.size(); ++i) {
        if (!read_state[i].equal(rd.read_state[i])) {
            return false;
        }
    }
    if (entries.size() != rd.entries.size()) {
        return false;
    }
    for (size_t i = 0; i < entries.size(); ++i) {
        if (*entries[i] != *rd.entries[i]) {
            return false;
        }
    }
    if (!snapshot.equal(rd.snapshot)) {
        return false;
    }
    if (committed_entries.size() != rd.committed_entries.size()) {
        return false;
    }
    for (size_t i = 0; i < committed_entries.size(); i++) {
        if (*committed_entries[i] != *rd.committed_entries[i]) {
            return false;
        }
    }
    return must_sync == rd.must_sync;
}
} // namespace kv
