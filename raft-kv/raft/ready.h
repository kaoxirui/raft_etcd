#pragma once
#include "proto.h"

namespace kv {
enum RaftState {
    Follower = 0,
    Candidate = 1,
    Leader = 2,
    PreCandidate = 3,
};
}