#include "log.h"
#include "readonly.h"

namespace kv {

//用于获取只读请求队列中最后一个待处理请求的ID。这个ID信息用于标志和跟踪每个只读请求
void ReadOnly::last_pending_request_ctx(std::vector<uint8_t> &ctx) {
    if (read_index_queue.empty()) {
        return;
    }
    ctx.insert(ctx.end(), read_index_queue.back().begin(), read_index_queue.back().end());
}
//用于接收并处理只读请求的确认消息
//更新对应请求的确认信息，并返回已收到的确认数量，包括本地节点的确认
uint32_t ReadOnly::recv_ack(const proto::Message &msg) {
    //消息的context即消息id，根据消息id获取对应的readIndexStatus
    std::string str(msg.context.begin(), msg.context.end());
    auto it = pending_read_index.find(str);
    if (it == pending_read_index.end()) {
        return 0;
    }
    it->second->acks.insert(msg.from);
    return it->second->acks.size() + 1;
}

std::vector<ReadIndexStatusPtr> ReadOnly::advance(const proto::Message &msg) {
    std::vector<ReadIndexStatusPtr> rss;
    std::string ctx(msg.context.begin(), msg.context.end());
    bool found = false;

    uint32_t i = 0;
    for (std::string &okctx : read_index_queue) {
        i++;
        auto it = pending_read_index.find(okctx);
        if (it == pending_read_index.end()) {
            LOG_FATAL("cannot find corresponding read state from pending map");
        }
        rss.push_back(it->second);
        if (okctx == ctx) {
            found = true;
            break;
        }
    }
    if (found) {
        read_index_queue.erase(read_index_queue.begin(), read_index_queue.begin() + i);
        for (ReadIndexStatusPtr &rs : rss) {
            std::string str(rs->req.entries[0].data.begin(), rs->req.entries[0].data.end());
            pending_read_index.erase(str);
        }
    }
    return rss;
}

//readonly对象添加一个新的只读请求
void ReadOnly::add_request(uint64_t index, proto::MessagePtr msg) {
    std::string ctx(msg->entries[0].data.begin(), msg->entries[0].data.end());
    auto it = pending_read_index.find(ctx);
    if (it != pending_read_index.end()) {
        return;
    }
    ReadIndexStatusPtr status(new ReadIndexStatus());
    status->index = index;
    status->req = *msg;
    pending_read_index[ctx] = status;
    read_index_queue.push_back(ctx);
}

} // namespace kv
