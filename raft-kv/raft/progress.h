#pragma once

#include <memory>
#include <vector>

namespace kv {
enum ProgressState { ProgressStateProbe = 0, ProgressStateReplicate = 1, ProgressStateSnapshot = 2 };
class Progress {};
typedef std::shared_ptr<Progress> ProgressPtr;
}; // namespace kv