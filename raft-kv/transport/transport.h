#pragma once
#include "../common/status.h"
#include "../raft/node.h"
#include "../raft/proto.h"
#include "peer.h"
#include "raft_server.h"
#include "transport.h"
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace kv {
class Transport {
public:
    virtual ~Transport() = default;
    virtual void start(const std::string &host) = 0;
    virtual void stop() = 0;
    // sends out the given messages to the remote peers.
    // Each message has a To field, which is an id that maps
    // to an existing peer in the transport.
    // If the id cannot be found in the transport, the message
    // will be ignored.
    virtual void send(std::vector<proto::MessagePtr> msgs) = 0;
    virtual void add_peer(uint64_t id, const std::string &peer) = 0;
    virtual void remove_peer(uint64_t id) = 0;
    static std::shared_ptr<Transport> create(RaftServer *raft, uint64_t id);
};
typedef std::shared_ptr<Transport> TransporterPtr;

} // namespace kv