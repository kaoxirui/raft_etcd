#include "storage.h"
#include "../common/log.h"
#include "util.h"

namespace kv {
Status MemoryStorage::initial_state(proto::HardState &hard_state, proto::ConfState &conf_state) {
    hard_state = hard_state_;
    conf_state = snapshot_->metadata.conf_state;
    return Status::ok();
}
void MemoryStorage::set_hard_state(proto::HardState &hard_state) {
    std::lock_guard<std::mutex> guard(mutex_);
    hard_state_ = hard_state;
}
//返回指定范围的日志条目
Status MemoryStorage::entries(uint64_t low, uint64_t high, uint64_t max_size,
                              std::vector<proto::EntryPtr> &entries) {
    assert(low < high);
    std::lock_guard<std::mutex> guard(mutex_);
    //获取存储的第一个日志条目的索引，作为偏移量
    uint64_t offset = entries_[0]->index;
    if (low < offset) {
        return Status::invalid_argument("requested index is unavailable due to campaction");
    }
    uint64_t last = 0;
    this->last_index_impl(last);
    if (high > last + 1) {
        LOG_FATAL("entries' hi(%lu) is out of bound last_index(%lu)", high, last);
    }
    //只有一个日志条目，说明日志条目不可用，返回无效参数错误
    if (entries_.size() == 1) {
        return Status::invalid_argument("request entry at index is unavailable");
    }
    //将请求条目添加到entries中
    for (uint64_t i = low - offset; i < high - offset; ++i) {
        entries.push_back(entries_[i]);
    }
    entry_limit_size(max_size, entries);
    return Status::ok();
}

//获取指定索引i日志条目和任期
Status MemoryStorage::term(uint64_t i, uint64_t &term) {
    std::lock_guard<std::mutex> guard(mutex_);
    //旧日志被压缩，所以第一个日志的索引不一定是1或者0
    uint64_t offset = entries_[0]->index;
    if (i < offset) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }
    if (i - offset >= entries_.size()) {
        return Status::invalid_argument("requested entry at index is unavailable");
    }
    term = entries_[i - offset]->term;
    return Status::ok();
}

Status MemoryStorage::last_index(uint64_t &index) {
    std::lock_guard<std::mutex> guard(mutex_);
    return last_index_impl(index);
}
Status MemoryStorage::first_index(uint64_t &index) {
    std::lock_guard<std::mutex> guard(mutex_);
    return first_index_impl(index);
}
Status MemoryStorage::snapshot(proto::SnapshotPtr &snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    snapshot = snapshot_;
    return Status::ok();
}
//! erase的删除区间是【first，last）
Status MemoryStorage::compact(uint64_t compact_index) {
    std::lock_guard<std::mutex> guard(mutex_);
    uint64_t offset = entries_[0]->index;
    if (compact_index <= offset) {
        return Status::invalid_argument("requested idnex is unavailable due to compaction");
    }
    //获取最后一个条目索引
    uint64_t last_idx;
    this->last_index_impl(last_idx);
    if (compact_index > last_idx) {
        LOG_FATAL("compact %lu is out of lastindex(%lu)", compact_index, last_idx);
    }
    //compact_index=4,i=4-3=1
    //entries_[0]->index=4,term=4;
    //entries_.erase(1,2);
    //i表示在entries_中的位置
    //把4，5都删了，第0个的index就是4，第一个就是5
    uint64_t i = compact_index - offset;
    entries_[0]->index = entries_[i]->index;
    entries_[0]->term = entries_[i]->term;
    //删除到i的内容，到i的内容表示offset到compact_index之间的内容
    entries_.erase(entries_.begin() + 1, entries_.begin() + i + 1);
    return Status::ok();
}

Status MemoryStorage::append(std::vector<proto::EntryPtr> entries) {
    if (entries.empty()) {
        return Status::ok();
    }
    std::lock_guard<std::mutex> guard(mutex_);
    //获取第一个和最后一个条目的索引
    uint64_t first = 0;
    first_index_impl(first);
    //last是传入的entries的最后一个
    uint64_t last = 0;
    last = entries[0]->index + entries.size() - 1;
    //没有新条目，直接返回成功状态,新传入的last比旧的first还小
    if (last < first) {
        return Status::ok();
    }
    //截断已压缩条目
    //传入的entry的index小了，截断对应部分
    if (first > entries[0]->index) {
        uint64_t n = first - entries[0]->index;
        entries.erase(entries.begin(), entries.begin() + n);
    }
    //计算新条目相对于现有条目的偏移量
    //用于定位新条目在现有条目列表中的位置
    uint64_t offset = entries[0]->index - entries_[0]->index;
    //现有条目数大于偏移量，删除offset之后的现有条目，并将新条目插入到末尾
    if (entries_.size() > offset) {
        //offset之后的条目与新条目冲突，删除冲突条目
        entries_.erase(entries_.begin() + offset, entries_.end());
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else if (entries_.size() == offset) {
        entries_.insert(entries_.end(), entries.begin(), entries.end());
    } else {
        //现有条目数小于偏移量，说明存在丢失的日志条目
        uint64_t last_idx;
        last_index_impl(last_idx);
        LOG_FATAL("missing log entry [last:%lu,append at:%lu,", last_idx, entries[0]->index);
    }
    return Status::ok();
}

Status MemoryStorage::create_snapshot(uint64_t index, proto::ConfStatePtr cs,
                                      std::vector<uint8_t> data, proto::SnapshotPtr &snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    //小于已有快照索引
    if (index <= snapshot_->metadata.index) {
        snapshot = std::make_shared<proto::Snapshot>();
        return Status::invalid_argument("request index is older than the existing snapshot");
    }
    uint64_t offset = entries_[0]->index;
    uint64_t last = 0;
    last_index_impl(last);
    if (index > last) {
        LOG_FATAL("snaphot %lu is out of bound lastindex(%lu)", index, last);
    }
    //更新现有快照的元数据
    snapshot_->metadata.index = index;
    snapshot_->metadata.term = entries_[index - offset]->term;
    if (cs) {
        snapshot_->metadata.conf_state = *cs;
    }
    snapshot_->data = std::move(data);
    snapshot = snapshot_;
    return Status::ok();
}

Status MemoryStorage::apply_snapshot(const proto::Snapshot &snapshot) {
    std::lock_guard<std::mutex> guard(mutex_);
    uint64_t index = snapshot_->metadata.index;
    uint64_t snap_index = snapshot.metadata.index;
    if (index >= snap_index) {
        return Status::invalid_argument("requested index is older than the existing snapshot");
    }
    snapshot_ = std::make_shared<proto::Snapshot>(snapshot);
    entries_.resize(1);
    proto::EntryPtr entry(new proto::Entry());
    entry->term = snapshot_->metadata.term;
    entry->index = snapshot_->metadata.index;
    entries_[0] = std::move(entry);
    return Status::ok();
}

//返回当前日志的最后一个索引
Status MemoryStorage::last_index_impl(uint64_t &idnex) {
    idnex = entries_[0]->index + entries_.size() - 1;
    return Status::ok();
}
Status MemoryStorage::first_index_impl(uint64_t &index) {
    index = entries_[0]->index + 1;
    return Status::ok();
}

} // namespace kv
