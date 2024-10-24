#pragma once
#include <stdint.h>
#include <vector>
#include <msgpack.hpp>
#include "log.h"

namespace kv {
namespace proto {
    typedef uint8_t MessageType;
    const MessageType MsgHup = 0;
    const MessageType MsgBeat = 1;
    const MessageType MsgProp = 2;
    const MessageType MsgApp = 3;
    const MessageType MsgAppResp = 4;
    const MessageType MsgVote = 5;
    const MessageType MsgVoteResp = 6;
    const MessageType MsgSnap = 7;
    const MessageType MsgHeartbeat = 8;
    const MessageType MsgHeartbeatResp = 9;
    const MessageType MsgUnreachable = 10;
    const MessageType MsgSnapStatus = 11;
    const MessageType MsgCheckQuorum = 12;
    const MessageType MsgTeansferLeader = 13;
    const MessageType MsgTimeoutNow = 14;
    const MessageType MsgReadIndex = 15;
    const MessageType MsgReadIndexResp = 16;
    const MessageType MsgPreVote = 17;
    const MessageType MsgPreVoteResp = 18;
    const MessageType MsgTypeSize = 19;

    const char *msg_type_to_string(MessageType type);

    typedef uint8_t EntryType;
    const EntryType EntryNormal = 0;
    //集群条目变更，如增加删除
    const EntryType EntryConfChange = 1;
    const char *entry_type_to_string(EntryType type);

    //日志
    struct Entry {
        Entry() : type(EntryNormal), term(0), index(0) {}

        explicit Entry(Entry &&entry)
            : type(entry.type), term(entry.term), index(entry.index), data(std::move(entry.data)) {}

        kv::proto::Entry &operator=(const kv::proto::Entry &entry) = default;

        Entry(const Entry &entry) = default;
        //有参构造
        explicit Entry(EntryType type, uint64_t term, uint64_t index, std::vector<uint8_t> data)
            : type(type), term(term), index(index), data(std::move(data)) {}

        uint32_t serialize_size() const;
        uint32_t payload_size() const { return static_cast<uint32_t>(data.size()); }

        bool operator==(const Entry &entry) const {
            return type == entry.type && term == entry.term && index == entry.index
                   && data == entry.data;
        }

        bool operator!=(const Entry &entry) const { return !(*this == entry); }

        EntryType type;
        uint64_t term;
        uint64_t index;
        std::vector<uint8_t> data;
        MSGPACK_DEFINE(type, term, index, data);
    };

    struct Message {
        Message()
            : type(MsgHup), to(0), from(0), term(0), log_term(0), index(0), commit(0),
              reject(false), reject_hint(0) {}
        bool operator=(const Message &msg) const {
            return type == msg.type && to == msg.to && from == msg.from && term == msg.term
                   && log_term == msg.log_term && index == msg.index && entries == msg.entries
                   && commit == msg.commit && reject == msg.reject && reject_hint == msg.reject_hint
                   && context == msg.context;
        }
        bool is_local_msg() const;
        bool is_response_msg() const;
        MessageType type;
        uint64_t to;
        uint64_t from;
        uint64_t term;
        uint64_t log_term;
        uint64_t index;
        std::vector<Entry> entries;
        uint64_t commit;
        //Snapshot snapshot;
        bool reject;
        uint64_t reject_hint;
        std::vector<uint8_t> context;
        MSGPACK_DEFINE(type, to, from, term, log_term, index, entries, commit, reject, reject_hint,
                       context);
    };
    typedef std::shared_ptr<Message> MessagePtr;

} // namespace proto
}; // namespace kv