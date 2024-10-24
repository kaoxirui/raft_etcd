#include <boost/algorithm/string.hpp>
#include "raft.h"
#include "proto.h"

namespace kv {
Status Raft::step(proto::MessagePtr msg) {
    if (msg->term == 0) {

    } else if (msg->term > term_) {
    }
}
bool Raft::past_election_timeout() {
    return election_elapsed_ >= randomized_election_timeout_;
}
void Raft::tick_election() {
    election_elapsed_++;
    //todo:promotable
    if (past_election_timeout) {
        election_elapsed_ = 0;
        proto::MessagePtr msg(new proto::Message());
        msg->from = id_;
        msg->type = proto::MsgHup;
        step(std::move(msg));
    }
}
}; // namespace kv