#include "log.h"
#include "node.h"

namespace kv {
Node *Node::start_node(const Config &conf, const std::vector<PeerContext> &peers) {
    return new RawNode(conf, peers);
}

Node *Node::restart_node(const Config &conf) {
    return new RawNode(conf);
}

//?这是新建集群吗？
RawNode::RawNode(const Config &conf, const std::vector<PeerContext> &peers) {
    raft_ = std::make_shared<Raft>(conf);
    uint64_t last_index;
    Status status = conf.storage->last_index(last_index);
    if (!status.is_ok()) {
        LOG_FATAL("%s", status.to_string().c_str());
    }
    //if the log is empty,this is a new Rawnode(like startNode);
    //otherwise it`s restoring an existing RawNode(like restartnode)
    //?为什么要把peer的都加进去
    if (last_index == 0) {
        //1. 创建并序列化配置变更条目（ConfChange），这些条目用于添加节点到集群
        //2. 将这些条目追加到raft日志中，并更新日志的提交位置
        //3. 为每个节点调用raft->add_node方法将其添加到集群中
        raft_->become_follower(1, 0);
        std::vector<proto::EntryPtr> entries;
        for (size_t i = 0; i < peers.size(); ++i) {
            auto &peer = peers[i];
            proto::ConfChange cs = proto::ConfChange{
                .id = 0,
                .conf_change_type = proto::ConfChangeAddNode,
                .node_id = peer.id,
                .context = peer.context,
            };
            std::vector<uint8_t> data = cs.serialize();
            proto::EntryPtr entry(new proto::Entry());
            entry->type = proto::EntryConfChange;
            entry->term = 1;
            entry->index = i + 1;
            entry->data = std::move(data);
            entries.push_back(entry);
        }
        raft_->raft_log_->append(entries);
        raft_->raft_log_->committed_ = entries.size();
        for (auto &peer : peers) {
            raft_->add_node(peer.id);
        }
    }
    prev_soft_state_ = raft_->soft_state();
    if (last_index == 0) {
        prev_hard_state_ = proto::HardState();
    } else {
        prev_hard_state_ = raft_->hard_state();
    }
}

RawNode::RawNode(const Config &conf) {
    uint64_t last_index = 0;
    Status status = conf.storage->last_index(last_index);
    if (!status.is_ok()) {
        LOG_FATAL("%s", status.to_string().c_str());
    }
    raft_ = std::make_shared<Raft>(conf);
    prev_soft_state_ = raft_->soft_state();
    if (last_index == 0) {
        prev_hard_state_ = proto::HardState();
    } else {
        prev_hard_state_ = raft_->hard_state();
    }
}

void RawNode::tick() {
    raft_->tick();
}

//竞选
Status RawNode::campaign() {
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgHup;
    return raft_->step(std::move(msg));
}

//提出新的日志条目
Status RawNode::propose(std::vector<uint8_t> data) {
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgProp;
    msg->from = raft_->id_; //消息来源为当前节点id
    msg->entries.emplace_back(proto::EntryNormal, 0, 0, std::move(data));
    return raft_->step(std::move(msg));
}

Status RawNode::propose_conf_change(const proto::ConfChange &cc) {
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgProp;
    msg->entries.emplace_back(proto::EntryConfChange, 0, 0, cc.serialize());
    return raft_->step(std::move(msg));
}

Status RawNode::step(proto::MessagePtr msg) {
    //节点内部的消息不处理
    if (msg->is_local_msg()) {
        return Status::invalid_argument("raft:cannot step raft local message");
    }
    //获取消息发送者的进度
    ProgressPtr progress = raft_->get_progress(msg->from);
    //检查进度指针是否有效（即发送消息的节点是否存在进度信息）或者消息是否不是响应消息（如投票请求）
    if (progress || !msg->is_response_msg()) {
        return raft_->step(msg);
    }
    return Status::invalid_argument("raft:cannot step as peer not found");
}

//生成当前节点的ready对象
ReadyPtr RawNode::ready() {
    ReadyPtr rd = std::make_shared<Ready>(raft_, prev_soft_state_, prev_hard_state_);
    //返回ready之前，清除之前收集的未处理消息
    raft_->msgs_.clear();
    raft_->reduce_uncommitted_size(rd->committed_entries);
    return rd;
}

//检查当前节点是否有准备好的状态可以处理或发送
//?
bool RawNode::has_ready() {
    assert(prev_soft_state_);
    //当前节点的软状态和之前保存的软状态不相等，表示节点有新的状态变化
    if (!raft_->soft_state()->equal(*prev_soft_state_)) {
        return true;
    }
    proto::HardState hs = raft_->hard_state();
    if (!hs.is_empty_state() && !hs.equal(prev_hard_state_)) {
        return true;
    }
    proto::SnapshotPtr snapshot = raft_->raft_log_->unstable_->snapshot_;
    if (snapshot && !snapshot->is_empty()) {
        return true;
    }
    if (!raft_->msgs_.empty() || !raft_->raft_log_->unstable_entries().empty()
        || raft_->raft_log_->has_next_entries()) {
        return true;
    }
    return !raft_->read_states_.empty();
}

//更新raft节点在处理完一个ready状态后的状态信息
//todo
void RawNode::advance(ReadyPtr rd) {
    if (rd->soft_state) {
        prev_soft_state_ = rd->soft_state;
    }
    if (!rd->hard_sate.is_empty_state()) {
        prev_hard_state_ = rd->hard_sate;
    }
    // If entries were applied (or a snapshot), update our cursor for
    // the next Ready. Note that if the current HardState contains a
    // new Commit index, this does not mean that we're also applying
    // all of the new entries due to commit pagination by size.
    //获取ready对象应用到的索引
    uint64_t index = rd->applied_cursor();
    if (index > 0) {
        raft_->raft_log_->applied_to(index);
    }
    if (!rd->entries.empty()) {
        auto &entry = rd->entries.back();
        raft_->raft_log_->stable_to(entry->index, entry->term);
    }
    if (!rd->snapshot.is_empty()) {
        raft_->raft_log_->stable_snap_to(rd->snapshot.metadata.index);
    }
    if (!rd->read_state.empty()) {
        raft_->read_states_.clear();
    }
}
//处理配置变更操作
//todo
proto::ConfStatePtr RawNode::apply_conf_change(const proto::ConfChange &&cc) {
    //创建新的ConfState对象，用于记录配置变更后的节点状态
    proto::ConfStatePtr state(new proto::ConfState());
    if (cc.node_id == 0) {
        raft_->nodes(state->nodes);
        raft_->learner_nodes(state->learners);
        return state;
    }
    //根据类型进行相应的配置变更操作
    switch (cc.conf_change_type) {
        case proto::ConfChangeAddNode: {
            raft_->add_node_or_learner(cc.node_id, false);
            break;
        }
        case proto::ConfChangeAddLearnerNode: {
            raft_->add_node_or_learner(cc.node_id, true);
            break;
        }
        case proto::ConfChangeRemoveNode: {
            raft_->remove_node(cc.node_id);
            break;
        }
        case proto::ConfChangeUpdateNode: {
            LOG_DEBUG("ConfChangeUpdate");
            break;
        }
        default: {
            LOG_FATAL("unexpected conf type");
        }
    }
    //更新raft节点和学习者节点信息到ConfState对象中
    raft_->nodes(state->nodes);
    raft_->learner_nodes(state->learners);
    return state;
}
//领导者转移
void RawNode::transfer_leadership(uint64_t lead, ino64_t transferee) {
    // manually set 'from' and 'to', so that leader can voluntarily transfers its leadership
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgTransferLeader;
    msg->from = transferee;
    msg->to = lead;

    Status status = raft_->step(std::move(msg));
    if (!status.is_ok()) {
        LOG_WARN("transfer_leadership %s", status.to_string().c_str());
    }
}

//请求读取状态
Status RawNode::read_index(std::vector<uint8_t> rctx) {
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgReadIndex;
    msg->entries.emplace_back(proto::MsgReadIndex, 0, 0, std::move(rctx));
    return raft_->step(std::move(msg));
}
RaftStatusPtr RawNode::raft_status() {
    LOG_DEBUG("no impl yet");
    return nullptr;
}
//报告某个节点不可达
void RawNode::report_unreachable(uint64_t id) {
    proto::MessagePtr msg(new proto::Message());
    msg->type = proto::MsgUnreachable;
    msg->from = id; //from设置为不可达节点的id

    Status status = raft_->step(std::move(msg));
    if (!status.is_ok()) {
        LOG_WARN("report_unreachable %s", status.to_string().c_str());
    }
}

void RawNode::stop() {
}

} // namespace kv
