#pragma once
#include <functional>
#include <stdint.h>
#include "config.h"
#include "ready.h"
#include "progress.h"
namespace kv {

class Raft {
public:
    explicit Raft(const Config &c) {}
    virtual ~Raft();
    void become_follower(uint64_t term, uint64_t lead);
    void become_candidate();
    void become_pre_candidate();
    void become_leader();
    void campaign(const std::string &campaign_type);
    void tick_election();
    bool past_election_timeout();
    void send(proto::MessagePtr msg);
    uint32_t quorum() const { return static_cast<uint32_t>(prs_.size() / 2 + 1); }
    uint32_t poll(uint64_t id, proto::MessageType type, bool v);
    virtual Status step(proto::MessagePtr msg);

public:
    uint32_t id_;
    uint32_t term_;
    uint32_t election_elapsed_;
    uint32_t randomized_election_timeout_;
    RaftState state_;
    std::unordered_map<uint64_t, bool> votes_;
    std::unordered_map<uint64_t, ProgressPtr> prs_;
    std::vector<proto::MessagePtr> msgs_;
    bool pre_vote_;
};
typedef std::shared_ptr<Raft> RaftPtr;

} // namespace kv