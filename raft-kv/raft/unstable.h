#pragma once
#include "proto.h"

namespace kv {
class Unstable {
public:
    explicit Unstable(uint64_t offset) : offset_(offset) {}
    //maybe first index returns the index of the first possible entry in entries
    //if it has a snapshot
    void maybe_first_index(uint64_t &index, bool &ok);
    void maybe_last_index(uint64_t &index, bool &ok);
    //maybe_term returns the term of the entry at index i , if there is any
    void maybe_term(uint64_t index, uint64_t &term, bool &ok);
    void stable_to(uint64_t index, uint64_t term);
    void stable_snap_to(uint64_t index);
    void restore(proto::SnapshotPtr snapshot);
    void truncate_and_append(std::vector<proto::EntryPtr> entries);
    void slice(uint64_t low, uint64_t high, std::vector<proto::EntryPtr> &entries);

public:
    //the incoming unstable snapshot , if any
    proto::SnapshotPtr snapshot_;
    //all entries that have not yet been written to storage
    std::vector<proto::EntryPtr> entries_;
    //entries中第一条Entry记录的索引值
    uint64_t offset_;
};
typedef std::shared_ptr<Unstable> UnstablePtr;

} // namespace kv
