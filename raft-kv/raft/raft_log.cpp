#include "log.h"
#include "raft_log.h"
#include "util.h"
namespace kv {
RaftLog::RaftLog(StoragePtr storage, uint64_t max_next_ents_size)
    : storage_(std::move(storage)), committed_(0), applied_(0), max_next_ents_size_(max_next_ents_size) {
    assert(storage_);
    uint64_t first;
    //storage类似于使用者持久化存储的cache
    auto status = storage_->first_index(first);
    assert(status.is_ok());

    uint64_t last;
    status = storage_->last_index(last);
    assert(status.is_ok());

    //当unstable没有不可靠日志时，unstable的offset的值就是未来的第一个不可靠日志的索引
    unstable_ = std::make_shared<Unstable>(last + 1);
    // 初始化提交索引和应用索引,切记只是初始化，raft在构造完raftLog后还会设置这两个值，所以下面
    // 赋值感觉奇怪的可以忽略它
    applied_ = committed_ = first - 1;
}
RaftLog::~RaftLog() {
}
// 追加日志，在收到leader追加日志的消息后被调用。为什么是maybe？更确切的说什么原因会失败？这就要
// 从index,logTerm这两个参数说起了。raft会把若干个日志条目(Entry)封装在一个消息(Message)中，
// 同时在消息中还有index和logTerm两个参数，就是下面函数传入的同名参数。这两个参数是entries前一条日
// 志的索引和届，笔者会在其他文章介绍leader向其他节点发送日志的方法，此处只需要知道一点，leader有
// 一个参数记录下一次将要发送给某个节点的索引起始值，也就是entries[0].Index，而index和logTerm值就是
// entries[-1].Index和entries[-1].Term。知道这两个参数再来看源码注释。
void RaftLog::maybe_append(uint64_t index, uint64_t log_term, uint64_t committed, std::vector<proto::EntryPtr> entries,
                           uint64_t &last_new_index, bool &ok) {
    if (match_term(index, log_term)) {
        uint64_t lastnewi = index + entries.size();
        uint64_t ci = find_conflict(entries);
        if (ci == 0) {
            //no conflict
        } else if (ci <= committed_) {
            LOG_FATAL("entry %lu conflict with committed entry [committed(%lu)]", ci, committed_);
        } else {
            //处理冲突情况
            assert(ci > 0);
            uint64_t offset = index + 1;
            uint64_t n = ci - offset;
            entries.erase(entries.begin(), entries.begin() + n);
            append(std::move(entries));
        }
        // 更新提交索引，为什么取了个最小？committed是leader发来的，是全局状态，但是当前节点
        // 可能落后于全局状态，所以取了最小值。这里读者可能有疑问，lastnewi是这个节点最新的索引，
        // 不是大的可靠索引，如果此时节点异常了，会不会出现提交索引以前的日志已经被应用，但是有些
        // 日志还没有被持久化？这里笔者需要解释一下，raft更新了提交索引，raft会把提交索引以前的
        // 日志交给使用者应用同时会把不可靠日志也交给使用者持久化，所以这要求使用者必须先持久化日志
        // 再应用日志，否则就会出现刚刚提到的问题。
        commit_to(std::min(committed, lastnewi));

        last_new_index = lastnewi;
        ok = true;
        return;
    } else {
        last_new_index = 0;
        ok = false;
    }
}
uint64_t RaftLog::append(std::vector<proto::EntryPtr> entries) {
    if (entries.empty()) {
        return last_index();
    }
    //获取要追加的第一个条目的前一个索引
    uint64_t after = entries[0]->index - 1;
    if (after < committed_) {
        LOG_FATAL("after(%lu) is out of range [committed(%lu)]\",after ,committed", after, committed_);
    }
    unstable_->truncate_add_append(std::move(entries));
    return last_index();
}

uint64_t RaftLog::find_conflict(const std::vector<proto::EntryPtr> &entries) {
    for (const proto::EntryPtr &entry : entries) {
        if (!match_term(entry->index, entry->term)) {
            if (entry->index < last_index()) {
                uint64_t t;
                Status status = this->term(entry->index, t);
                LOG_INFO("find conflict at index %lu [existing term: %lu,conflicting term:%lu],%s", entry->index, t, entry->term,
                         status.to_string().c_str());
            }
            return entry->index;
        }
    }
    //没冲突返回0
    return 0;
}

//获取应用索引到提交索引间的所有日志
void RaftLog::next_entries(std::vector<proto::EntryPtr> &entries) const {
    uint64_t off = std::max(applied_ + 1, first_index());
    //检查是否有未应用的条目，已提交的条目多余未应用的条目，说明有未应用的条目需要处理
    if (committed_ > off) {
        Status status = slice(off, committed_ + 1, max_next_ents_size_, entries);
        if (!status.is_ok()) {
            LOG_FATAL("unexpected error when getting unapplied entries");
        }
    }
}

//判断是否有可应用日志
bool RaftLog::has_next_entries() const {
    //applied_+1下一个要应用到状态机的日志
    uint64_t off = std::max(applied_ + 1, first_index());
    //off到committed+1还有未应用的日志
    return committed_ + 1 > off;
}

bool RaftLog::maybe_commit(uint64_t max_index, uint64_t term) {
    //大于表示有新的日志可以提交
    if (max_index > committed_) {
        uint64_t t;
        this->term(max_index, t);
        if (t == term) {
            commit_to(max_index);
            return true;
        }
    }
    return false;
}

void RaftLog::restore(proto::SnapshotPtr snapshot) {
    LOG_INFO("log starts to restore snapshot [index: %lu, term: %lu]", snapshot->metadata.index, snapshot->metadata.term);
    //更新提交索引committed_为快照索引
    committed_ = snapshot->metadata.index;
    unstable_->restore(std::move(snapshot));
}

Status RaftLog::snapshot(proto::SnapshotPtr &snap) const {
    //从unstable返回快照
    if (unstable_->snapshot_) {
        snap = unstable_->snapshot_;
        return Status::ok();
    }
    //从storage返回快照
    proto::SnapshotPtr s;
    Status status = storage_->snapshot(s);
    if (s) {
        snap = s;
    }
    return status;
}

//更新已应用日志条目索引
//committed_>=applied_.状态机只能应用已提交的日志
void RaftLog::applied_to(uint64_t index) {
    if (index == 0) {
        return;
    }
    if (committed_ < index || index < applied_) {
        LOG_ERROR("applied(%lu) is out of range [prevApplied(%lu), committed(%lu)]", index, applied_, committed_);
    }
    applied_ = index;
}

Status RaftLog::slice(uint64_t low, uint64_t high, uint64_t max_size, std::vector<proto::EntryPtr> &entries) const {
    Status status = must_check_out_of_bounds(low, high);
    if (!status.is_ok()) {
        return status;
    }
    if (low == high) {
        return Status::ok();
    }
    //slice from storage
    if (low < unstable_->offset_) {
        status = storage_->entries(low, std::min(high, unstable_->offset_), max_size, entries);
        if (!status.is_ok()) {
            return status;
        }

        if (entries.size() < std::min(high, unstable_->offset_) - low) {
            return Status::ok();
        }
    }
    //slice from unstable
    if (high > unstable_->offset_) {
        //? unstable有啥用
        std::vector<proto::EntryPtr> unstable;
        unstable_->slice(std::max(low, unstable_->offset_), high, entries);
        entries.insert(entries.end(), unstable.begin(), unstable.end());
    }
    entry_limit_size(max_size, entries);
    return Status::ok();
}

Status RaftLog::must_check_out_of_bounds(uint64_t low, uint64_t high) const {
    assert(high > low);
    uint64_t first = first_index();
    if (low < first) {
        return Status::invalid_argument("requested index is unavailable due to compaction");
    }

    uint64_t length = last_index() + 1 - first;
    if (low < first || high > first + length) {
        LOG_FATAL("slice[%lu,%lu) out of bound [%lu,%lu]", low, high, first, last_index());
    }
    return Status::ok();
}

bool RaftLog::match_term(uint64_t index, uint64_t t) {
    uint64_t term_out;
    Status status = term(index, term_out);
    if (!status.is_ok()) {
        return false;
    }
    return t == term_out;
}

uint64_t RaftLog::last_term() const {
    uint64_t t;
    Status status = this->term(last_index(), t);
    assert(status.is_ok());
    return t;
}

Status RaftLog::term(uint64_t index, uint64_t &t) const {
    //如果索引在raftlog记录的日志之外，返回0表示没找到
    uint64_t dummy_index = first_index() - 1;
    if (index < dummy_index || index > last_index()) {
        t = 0;
        return Status::ok();
    }

    uint64_t term_index;
    bool ok;

    unstable_->maybe_term(index, term_index, ok);
    if (ok) {
        t = term_index;
        return Status::ok();
    }

    //在unstable中没找到，就在storage的term方法中查找
    Status status = storage_->term(index, term_index);
    if (status.is_ok()) {
        t = term_index;
    }
    return status;
}
uint64_t RaftLog::first_index() const {
    uint64_t index;
    bool ok;
    unstable_->maybe_first_index(index, ok);
    if (ok) {
        return index;
    }

    //从storage中获取第一个索引
    Status status = storage_->first_index(index);
    assert(status.is_ok());
    return index;
}

uint64_t RaftLog::last_index() const {
    uint64_t index;
    bool ok;
    unstable_->maybe_last_index(index, ok);
    if (ok) {
        return index;
    }

    //从storage中获取第一个索引
    Status status = storage_->last_index(index);
    assert(status.is_ok());
    return index;
}

//获取所有日志
void RaftLog::all_entries(std::vector<proto::EntryPtr> &entries) {
    entries.clear();
    Status status = this->entries(first_index(), RaftLog::unlimited(), entries);
    if (status.is_ok()) {
        return;
    }
    if (status.to_string() == Status::invalid_argument("requested index is unavailable due to compaction").to_string()) {
        this->all_entries(entries);
    }
    LOG_FATAL("%s", status.to_string().c_str());
}

void RaftLog::commit_to(uint64_t to_commit) {
    //never decrease commit
    //确保新的提交索引to_commit仅在大于当前提交索引comitted_时更新
    if (committed_ < to_commit) {
        //新的提交索引超出当前日志的最后一个索引范围，记录错误并终止 程序
        if (last_index() < to_commit) {
            LOG_FATAL("to_commit(%lu) is out of range [lastIndex(%lu)]. was the raft log corrupted,truncated or lost?", to_commit,
                      last_index());
        }
        committed_ = to_commit;
    } else {
        //如果新的提交索引小于等于当前的提交索引，忽略
    }
}
} // namespace kv