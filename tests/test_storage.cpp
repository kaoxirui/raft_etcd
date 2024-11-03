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
    //阶段并append
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
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
