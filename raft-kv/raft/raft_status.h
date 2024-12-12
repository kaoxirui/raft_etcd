#pragma once
#include "proto.h"

namespace kv {
struct RaftStatus {
    uint64_t id;
};
typedef std::shared_ptr<RaftStatus> RaftStatusPtr;
} // namespace kv