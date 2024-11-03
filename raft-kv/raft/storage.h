#pragma once

#include <memory>
#include <mutex>

#include "../common/status.h"
#include "proto.h"

namespace kv {
class Storage {
public:
    ~Storage() = default;
    //returns the save hard_state and ConfState information
    virtual Status initial_state(proto::HardState &hard_state, proto::ConfState &conf_state) = 0;
    //returns a slice of log entries in the range [low,high)
    virtual Status entries(uint64_t low, uint64_t high, uint64_t max_size,
                           std::vector<proto::EntryPtr> &entries) = 0;
    //returns the term of entry i
    virtual Status term(uint64_t i, uint64_t &term) = 0;
    //returns the index of the last entry in the log
    virtual Status last_index(uint64_t &index) = 0;
    // firstIndex returns the index of the first log entry that is
    // possibly available via entries (older entries have been incorporated
    // into the latest Snapshot; if storage only contains the dummy entry the
    // first log entry is not available).
    virtual Status first_index(uint64_t &index) = 0;
    //returns the most recent snapshot
    virtual Status snapshot(proto::SnapshotPtr &snapshot) = 0;
};
typedef std::shared_ptr<Storage> StoragePtr;

class MemoryStorage : public Storage {
public:
    explicit MemoryStorage() : snapshot_(new proto::Snapshot()) {
        proto::EntryPtr entry(new proto::Entry());
        entries_.push_back(std::move(entry));
    }
    virtual Status initial_state(proto::HardState &hard_state, proto::ConfState &conf_state);
    void set_hard_state(proto::HardState &hard_state);
    virtual Status entries(uint64_t low, uint64_t high, uint64_t max_size,
                           std::vector<proto::EntryPtr> &entries);
    virtual Status term(uint64_t i, uint64_t &term);
    virtual Status last_index(uint64_t &index);
    virtual Status first_index(uint64_t &index);
    virtual Status snapshot(proto::SnapshotPtr &snapshot);
    // compact discards all log entries prior to compact_index.
    // It is the application's responsibility to not attempt to compact an index
    // greater than raftLog.applied.
    Status compact(uint64_t compact_index);
    // append the new entries to storage.
    Status append(std::vector<proto::EntryPtr> entries);
    // create_snapshot makes a snapshot which can be retrieved with Snapshot() and
    // can be used to reconstruct the state at that point.
    // If any configuration changes have been made since the last compaction,
    // the result of the last apply_conf_change must be passed in.
    Status create_snapshot(uint64_t index, proto::ConfStatePtr cs, std::vector<uint8_t> data,
                           proto::SnapshotPtr &snapshot);
    // ApplySnapshot overwrites the contents of this Storage object with
    // those of the given snapshot.
    Status apply_snapshot(const proto::Snapshot &snapshot);

public:
    Status last_index_impl(uint64_t &index);
    Status first_index_impl(uint64_t &index);
    std::mutex mutex_;
    proto::HardState hard_state_;
    proto::SnapshotPtr snapshot_;
    // entries_[i] has raft log position i+snapshot.Metadata.Index
    std::vector<proto::EntryPtr> entries_;
};
typedef std::shared_ptr<MemoryStorage> MemoryStoragePtr;
} // namespace kv
