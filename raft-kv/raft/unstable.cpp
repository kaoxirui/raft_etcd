#include "../common/log.h"
#include "unstable.h"

namespace kv {
//从unstable类中获取可能的第一个和最后一个索引
void Unstable::maybe_first_index(uint64_t &index, bool &ok) {
    if (snapshot_) {
        ok = true;
        index = snapshot_->metadata.index + 1;
    } else {
        ok = false;
        index = 0;
    }
}

void Unstable::maybe_last_index(uint64_t &index, bool &ok) {
    if (!entries_.empty()) {
        ok = true;
        index = offset_ + entries_.size() - 1;
        return;
    }
    if (snapshot_) {
        ok = true;
        index = snapshot_->metadata.index;
        return;
    }
    index = 0;
    ok = false;
}
//获取给定索引的日志条目的任期
void Unstable::maybe_term(uint64_t index, uint64_t &term, bool &ok) {
    term = 0;
    ok = false;
    //索引小于偏移量，说明索引在快照范围内
    if (index < offset_) {
        if (!snapshot_) {
            return;
        }
        if (snapshot_->metadata.index == index) {
            term = snapshot_->metadata.term;
            ok = true;
            return;
        }
        return;
    }
    uint64_t last = 0;
    bool last_ok = false;
    maybe_first_index(last, last_ok);
    if (!last_ok) {
        return;
    }
    if (index > last) {
        return;
    }
    ok = true;
    term = entries_[index - offset_]->term;
}

//
void Unstable::stable_to(uint64_t index, uint64_t term) {
    uint64_t gt = 0;
    bool ok = false;
    maybe_term(index, gt, ok);
    if (!ok) {
        return;
    }
    if (gt == term && index >= offset_) {
        uint64_t n = index - offset_ + 1;
        entries_.erase(entries_.begin(), entries_.begin() + n);
        offset_ = index + 1;
    }
}

void Unstable::stable_snap_to(uint64_t index) {
    if (snapshot_ && snapshot_->metadata.index == index) {
        snapshot_ = nullptr;
    }
}

//收到新的快照后重复不稳定日志的状态
void Unstable::restore(proto::SnapshotPtr snapshot) {
    offset_ = snapshot->metadata.index + 1;
    //当前所有的不稳定日志条目被清空。
    //这是因为快照已经包含了该位置之前的所有日志状态，因此这些旧条目不再需要。
    entries_.clear();
    snapshot_ = snapshot;
}

void Unstable::truncate_add_append(std::vector<proto::EntryPtr> entries) {
    if (entries.empty()) {
        return;
    }
    //获取要追加的第一个条目的索引
    uint64_t after = entries[0]->index;
    if (after == entries_.size() + offset_) {
        //directly append
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else if (after <= offset_) {
        //afeter小于等于当前不稳定条目的偏移量，表示新的条目覆盖或替换了现有的所有条目
        //将偏移量设置为after并用新的条目替换现有条目
        LOG_INFO("replace the unstable entries from index %lu", after);
        offset_ = after;
        entries_ = std::move(entries);
    } else {
        //after比offset大，但不等于offset_+entries_.size()
        LOG_INFO("truncate the unstable entries before index %lu", after);
        std::vector<proto::EntryPtr> entries_slice;
        this->slice(offset_, after, entries_slice);
        entries_slice.insert(entries_slice.end(), entries_slice.begin(), entries_slice.end());
        entries_ = std::move(entries_slice);
    }
}

void Unstable::slice(uint64_t low, uint64_t high, std::vector<proto::EntryPtr> &entries) {
    assert(high > low);
    uint64_t upper = offset_ + entries_.size();
    if (low < offset_ || high > upper) {
        LOG_FATAL("unstable.slice[%lu,%lu) out of bound [%lu,%lu]", low, high, offset_, upper);
    }
    entries.insert(entries.end(), entries_.begin() + low - offset_, entries_.begin() + high - offset_);
}

} // namespace kv
