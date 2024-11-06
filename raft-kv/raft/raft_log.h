#pragma once
#include "storage.h"
#include "unstable.h"
namespace kv {
class RaftLog {
public:
    explicit RaftLog(StoragePtr storage, uint64_t max_next_ents_size);

public:
    //storage contains all stable entries since the last snapshot
    StoragePtr storage_;
    //unstable contains all unstable entries and sanpshot.they will be saved into storage
    UnstablePtr unstable_;
    //committed is the highest log positon that is known to be in the stable storage on a quorum nodes
    uint64_t committed_;
    // applied is the highest log position that the application has
    // been instructed to apply to its state machine.
    // Invariant: applied <= committed
    uint64_t applied_;
    // max_next_ents_size is the maximum number aggregate byte size of the messages
    // returned from calls to nextEnts.
    uint64_t max_next_ents_size_;
};
typedef std::shared_ptr<RaftLog> RaftLogPtr;
} // namespace kv
