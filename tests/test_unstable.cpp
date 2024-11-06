#include "../raft-kv/raft/unstable.h"
#include <gtest/gtest.h>

using namespace kv;

static proto::SnapshotPtr newSnapshot(uint64_t index, uint64_t term) {
    proto::SnapshotPtr s(new proto::Snapshot());
    s->metadata.index = index;
    s->metadata.term = term;
    return s;
}

static bool snapshot_cmp(kv::proto::SnapshotPtr left, kv::proto::SnapshotPtr right) {
    if (left->data != right->data) {
        return false;
    }
    if (left->metadata.index != right->metadata.index) {
        return false;
    }
    if (left->metadata.term != right->metadata.term) {
        return false;
    }
    if (left->metadata.conf_state.nodes != right->metadata.conf_state.nodes) {
        return false;
    }
    if (left->metadata.conf_state.learners != right->metadata.conf_state.learners) {
        return false;
    }
    return true;
}

static proto::EntryPtr newEntry(uint64_t index, uint64_t term) {
    proto::EntryPtr entry(new proto::Entry());
    entry->index = index;
    entry->term = term;
    return entry;
}

TEST(unstable, first_index) {
    {
        uint64_t windex = 0;
        Unstable unstable(5);
        unstable.entries_.push_back(newEntry(5, 1));

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(index == windex);
    }
    {
        uint64_t windex = 0;
        Unstable unstable(5);
        unstable.snapshot_ = nullptr;
        unstable.entries_.clear();

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_FALSE(ok);
        ASSERT_TRUE(index == windex);
    }
    {
        uint64_t windex = 5;
        Unstable unstable(5);
        unstable.snapshot_ = newSnapshot(4, 1);
        unstable.entries_.push_back(newEntry(5, 1));

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }
    {
        uint64_t windex = 5;
        Unstable unstable(5);
        unstable.snapshot_ = newSnapshot(4, 1);
        unstable.entries_.clear();

        uint64_t index;
        bool ok;
        unstable.maybe_first_index(index, ok);
        ASSERT_TRUE(ok);
        ASSERT_TRUE(index == windex);
    }
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
