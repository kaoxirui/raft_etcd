#pragma once
#include <functional>
#include <stdint.h>
#include "config.h"
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
    virtual Status step(proto::MessagePtr msg);

public:
    uint32_t id_;
    uint32_t term_;
    uint32_t election_elapsed_;
    uint32_t randomized_election_timeout_;
};
} // namespace kv