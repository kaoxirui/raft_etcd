#pragma once
#include "proto.h"

namespace kv {
//maps vote and prevote message types to their corresponding responses
proto::MessageType vote_resp_msg_type(proto::MessageType type);
void entry_limit_size(uint64_t max_size, std::vector<proto::EntryPtr> &entries);
} // namespace kv
