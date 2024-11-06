#include <gtest/gtest.h>
#include "../raft-kv/raft/storage.h"
#include <string>
#include <iostream>
using namespace std;

kv::proto::EntryPtr newMemoryStorage(uint64_t term, uint64_t index) {
    kv::proto::EntryPtr ptr(new kv::proto::Entry());
    ptr->term = term;
    ptr->index = index;
    return ptr;
}

bool entry_cmp(const std::vector<kv::proto::EntryPtr> &left,
               const std::vector<kv::proto::EntryPtr> &right) {
    if (left.size() != right.size()) {
        return false;
    }
    for (size_t i = 0; i < left.size(); ++i) {
        if (left[i]->index != right[i]->index) {
            return false;
        }
        if (left[i]->term != right[i]->term) {
            return false;
        }
    }
    return true;
}

TEST(storage, term) {
    {
        //* 指定要查询的日志条目索引
        uint64_t i = 2;
        //*预设了错误的状态信息，表明请求的索引由于日志压缩而不可用
        kv::Status status =
            kv::Status::invalid_argument("requested index is unavailable due to compaction");
        uint64_t wterm = 0;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        uint64_t term = 0;
        kv::Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
    {
        //* 指定要查询的日志条目索引
        uint64_t i = 3;
        kv::Status status = kv::Status::ok();
        uint64_t wterm = 3;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        uint64_t term;
        //获取索引为i的term
        kv::Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
    {
        //* 指定要查询的日志条目索引
        uint64_t i = 4;
        kv::Status status = kv::Status::ok();
        uint64_t wterm = 4;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        uint64_t term;
        //获取索引为i的term
        kv::Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
    {
        //* 指定要查询的日志条目索引
        uint64_t i = 5;
        kv::Status status = kv::Status::ok();
        uint64_t wterm = 5;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        uint64_t term;
        //获取索引为i的term
        kv::Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
    {
        //* 指定要查询的日志条目索引
        uint64_t i = 6;
        kv::Status status = kv::Status::invalid_argument("requested entry at index is unavailable");
        uint64_t wterm = 0;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        uint64_t term = 0;
        //获取索引为i的term
        kv::Status s = m.term(i, term);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(term == wterm);
    }
}

TEST(storage, find_first_of) {
    kv::MemoryStorage m;
    m.entries_.clear();
    m.entries_.push_back(newMemoryStorage(3, 3));
    m.entries_.push_back(newMemoryStorage(4, 4));
    m.entries_.push_back(newMemoryStorage(5, 5));

    uint64_t first = 0;
    kv::Status status = m.first_index(first);
    ASSERT_TRUE(first == 4);
    ASSERT_TRUE(status.is_ok());

    status = m.compact(4);
    ASSERT_TRUE(status.is_ok());
    m.first_index(first);
    ASSERT_TRUE(first == 5);

    status = m.compact(5);
    std::cerr << status.to_string() << std::endl;
    ASSERT_TRUE(m.entries_.size() == 1);
    m.first_index(first);
    ASSERT_TRUE(first == 6);
}

TEST(storage, last_index) {
    kv::MemoryStorage m;
    m.entries_.clear();
    m.entries_.push_back(newMemoryStorage(3, 3));
    m.entries_.push_back(newMemoryStorage(4, 4));
    m.entries_.push_back(newMemoryStorage(5, 5));

    uint64_t last;
    m.last_index(last);
    ASSERT_TRUE(last == 5);

    std::vector<kv::proto::EntryPtr> entries;
    entries.push_back(newMemoryStorage(5, 6));
    m.append(std::move(entries));
    last = 0;
    m.last_index(last);
    ASSERT_TRUE(last == 6);
}

TEST(storage, compact) {
    {
        uint64_t i = 2;
        kv::Status status =
            kv::Status::invalid_argument("requested idnex is unavailable due to compaction");
        uint64_t windex = 3;
        uint64_t wterm = 3;
        uint64_t wlen = 3;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.entries_[0]->index == windex);
        ASSERT_TRUE(m.entries_[0]->term == wterm);
        ASSERT_TRUE(m.entries_.size() == wlen);
    }
    {
        uint64_t i = 3;
        kv::Status status =
            kv::Status::invalid_argument("requested idnex is unavailable due to compaction");
        uint64_t windex = 3;
        uint64_t wterm = 3;
        uint64_t wlen = 3;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.to_string() == status.to_string());
        ASSERT_TRUE(m.entries_[0]->index == windex);
        ASSERT_TRUE(m.entries_[0]->term == wterm);
        ASSERT_TRUE(m.entries_.size() == wlen);
    }
    {
        uint64_t i = 4;
        kv::Status status = kv::Status::ok();
        uint64_t windex = 4;
        uint64_t wterm = 4;
        uint64_t wlen = 2;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.is_ok());
        ASSERT_TRUE(m.entries_[0]->index == windex);
        ASSERT_TRUE(m.entries_[0]->term == wterm);
        ASSERT_TRUE(m.entries_.size() == wlen);
    }
    {
        uint64_t i = 5;
        kv::Status status = kv::Status::ok();
        uint64_t windex = 5;
        uint64_t wterm = 5;
        uint64_t wlen = 1;

        kv::MemoryStorage m;
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        auto s = m.compact(i);
        ASSERT_TRUE(s.is_ok());
        ASSERT_TRUE(m.entries_[0]->index == windex);
        ASSERT_TRUE(m.entries_[0]->term == wterm);
        ASSERT_TRUE(m.entries_.size() == wlen);
    }
}

TEST(storage, append) {
    kv::MemoryStorage m;
    {
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        std::vector<kv::proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(1, 1));
        add_entries.push_back(newMemoryStorage(2, 2));

        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));

        m.append(std::move(add_entries));
        ASSERT_TRUE(entry_cmp(m.entries_, out_entries));
    }
    //truncate the existing entries and append
    { //term index
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        //first>add_entries[0]->index
        std::vector<kv::proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(3, 2));
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(5, 4));
        //offset=0；
        /*
        add_entries.push_back(newMemoryStorage(3, 3));
        add_entries.push_back(newMemoryStorage(5, 4));
        把之前的全部覆盖
        */
        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(5, 4));

        m.append(std::move(add_entries));
        ASSERT_TRUE(entry_cmp(m.entries_, out_entries));
    }
    //truncate the existing entries and append
    {
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        std::vector<kv::proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(5, 4));

        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(5, 4));
        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.entries_, out_entries));
    }
    //direct append
    {
        m.entries_.clear();
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(5, 5));

        std::vector<kv::proto::EntryPtr> add_entries;
        add_entries.push_back(newMemoryStorage(5, 6));

        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(3, 3));
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));
        out_entries.push_back(newMemoryStorage(5, 6));

        m.append(std::move(add_entries));

        ASSERT_TRUE(entry_cmp(m.entries_, out_entries));
    }
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

TEST(storage, create) {
    kv::proto::ConfStatePtr cs(new kv::proto::ConfState());
    cs->nodes.push_back(1);
    cs->learners.push_back(2);

    std::vector<uint8_t> data;
    data.push_back('d');
    data.push_back('a');
    data.push_back('t');
    data.push_back('a');

    {
        kv::MemoryStorage m;
        m.entries_.clear();
        //term index
        m.entries_.push_back(newMemoryStorage(3, 3));
        m.entries_.push_back(newMemoryStorage(4, 4));
        m.entries_.push_back(newMemoryStorage(4, 5));

        kv::proto::SnapshotPtr snapshot(new kv::proto::Snapshot());
        snapshot->data = data;
        snapshot->metadata.term = 4;
        snapshot->metadata.index = 5;
        snapshot->metadata.conf_state = *cs;

        kv::proto::SnapshotPtr snap;
        auto s = m.create_snapshot(5, cs, data, snap);
        ASSERT_TRUE(s.is_ok());
        ASSERT_TRUE(snapshot_cmp(snapshot, snap));
    }
}

TEST(storage, apply) {
    kv::proto::ConfState cs;
    cs.nodes.push_back(1);
    cs.nodes.push_back(2);
    cs.nodes.push_back(3);

    std::vector<uint8_t> data;
    data.push_back('d');
    data.push_back('a');
    data.push_back('t');
    data.push_back('a');

    kv::MemoryStorage m;

    kv::proto::SnapshotPtr snapshot(new kv::proto::Snapshot());
    snapshot->metadata.index = 4;
    snapshot->metadata.term = 4;
    snapshot->metadata.conf_state = cs;
    auto status = m.apply_snapshot(*snapshot);
    ASSERT_TRUE(status.is_ok());

    snapshot = std::make_shared<kv::proto::Snapshot>();
    snapshot->metadata.index = 3;
    snapshot->metadata.term = 3;
    snapshot->metadata.conf_state = cs;
    status = m.apply_snapshot(*snapshot);
    ASSERT_FALSE(status.is_ok());
}

TEST(storage, entry) {
    kv::MemoryStorage m;
    m.entries_.clear();
    m.entries_.push_back(newMemoryStorage(3, 3));
    m.entries_.push_back(newMemoryStorage(4, 4));
    m.entries_.push_back(newMemoryStorage(5, 5));
    m.entries_.push_back(newMemoryStorage(6, 6));
    {
        uint32_t low = 2;
        uint32_t high = 6;
        std::vector<kv::proto::EntryPtr> entries;
        auto s = m.entries(low, high, std::numeric_limits<uint64_t>::max(), entries);
        ASSERT_TRUE(entries.size() == 0);
        ASSERT_TRUE(!s.is_ok());
    }
    {
        uint32_t low = 3;
        uint32_t high = 4;
        std::vector<kv::proto::EntryPtr> entries;
        auto s = m.entries(low, high, std::numeric_limits<uint64_t>::max(), entries);
        ASSERT_TRUE(entries.size() != 0);
        ASSERT_TRUE(s.is_ok());
    }
    {
        uint32_t low = 4;
        uint32_t high = 5;

        std::vector<kv::proto::EntryPtr> entries;

        auto s = m.entries(low, high, std::numeric_limits<uint64_t>::max(), entries);
        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(4, 4));

        ASSERT_TRUE(entry_cmp(entries, out_entries));
        ASSERT_TRUE(s.is_ok());
    }

    {
        uint32_t low = 4;
        uint32_t high = 6;

        std::vector<kv::proto::EntryPtr> entries;

        auto s = m.entries(low, high, std::numeric_limits<uint64_t>::max(), entries);
        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));

        ASSERT_TRUE(entry_cmp(entries, out_entries));
        ASSERT_TRUE(s.is_ok());
    }
    {
        // even if maxsize is zero, the first entry should be returned
        uint32_t low = 4;
        uint32_t high = 7;

        std::vector<kv::proto::EntryPtr> entries;

        auto s = m.entries(low, high, 0, entries);
        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(4, 4));
        ASSERT_TRUE(entry_cmp(entries, out_entries));
        ASSERT_TRUE(s.is_ok());
    }
    {
        //limit to 2
        uint32_t low = 4;
        uint32_t high = 7;
        std::vector<kv::proto::EntryPtr> entries;
        uint64_t max_size = m.entries_[1]->serialize_size() + m.entries_[2]->serialize_size()
                            + m.entries_[3]->serialize_size();
        auto s = m.entries(low, high, max_size, entries);

        std::vector<kv::proto::EntryPtr> out_entries;
        out_entries.push_back(newMemoryStorage(4, 4));
        out_entries.push_back(newMemoryStorage(5, 5));
        out_entries.push_back(newMemoryStorage(6, 6));
        ASSERT_TRUE(s.is_ok());
        ASSERT_TRUE(entry_cmp(entries, out_entries));
    }
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
