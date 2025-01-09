#include "../common/slice.h"
#include "proto.h"
#include "raft.h"
#include "util.h"
#include <boost/algorithm/string.hpp>

namespace kv {
//用于计算给定日志条目列表中配置变更条目的数量
static uint32_t num_of_pending_conf(const std::vector<proto::EntryPtr> &entries) {
    uint32_t n = 0;
    for (const proto::EntryPtr &entry : entries) {
        if (entry->type == proto::EntryConfChange) {
            n++;
        }
    }
    return n;
}
static const std::string kCampaignPreElection = "kCampaignPreElection";
static const std::string kCampaignElection = "kCampaignElection";
static const std::string kCampaignTransfer = "kCampaignTransfer";

//用config对象来初始化raft对象
Raft::Raft(const Config &c)
    : id_(c.id), term_(0), vote_(0), max_msg_size_(c.max_size_per_msg),
      max_uncommitted_size_(c.max_uncommitted_entries_size), max_inflight_(c.max_inflight_msgs),
      state_(RaftState::Follower), is_learner_(false), lead_(0), lead_transferee_(0),
      pending_conf_index_(0), uncommitted_size_(0), read_only_(new ReadOnly(c.read_only_option)),
      election_elapsed_(0), heartbeat_elapsed_(0), check_quorum_(c.check_quorum),
      pre_vote_(c.pre_vote), heartbeat_timeout_(c.heartbeat_tick),
      election_timeout_(c.election_tick), randomized_election_timeout_(0),
      disable_proposal_forwarding_(c.disable_proposal_forwarding),
      random_device_(0, c.election_tick) {
    raft_log_ = std::make_shared<RaftLog>(c.storage, c.max_committed_size_per_ready);
    proto::HardState hs;
    proto::ConfState cs;
    Status status = c.storage->initial_state(hs, cs);
    if (!status.is_ok()) {
        LOG_FATAL("%s", status.to_string().c_str());
    }
    std::vector<uint64_t> peers = c.peers;
    std::vector<uint64_t> learners = c.learners;

    if (!cs.nodes.empty() || !cs.learners.empty()) {
        if (!peers.empty() || !learners.empty()) {
            // tests; the argument should be removed and these tests should be
            // updated to specify their nodes through a snapshot.
            LOG_FATAL(
                "cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
        }
        peers = cs.nodes;
        learners = cs.learners;
    }
    //为每个节点和学习者创建progress对象
    for (uint64_t peer : peers) {
        ProgressPtr p(new Progress(max_inflight_));
        p->next = 1; //next是下一次发送日志
        prs_[peer] = p;
    }
    for (uint64_t learner : learners) {
        //如果学习者节点已经在普通节点的列表中，则会记录一个致命错误并终止程序运行
        auto it = prs_.find(learner);
        if (it != prs_.end()) {
            LOG_FATAL("node %lu is in both learner and peer list", learner);
        }
        ProgressPtr p(new Progress(max_inflight_));
        p->next = 1; //next是下一次发送日志
        p->is_learner = true;

        learner_prs_[learner] = p;
        if (id_ == learner) {
            is_learner_ = true;
        }
    }

    if (!hs.is_empty_state()) {
        load_state(hs);
    }
    if (c.applied > 0) {
        raft_log_->applied_to(c.applied);
    }

    become_follower(term_, 0);
    std::string node_str;
    {
        std::vector<std::string> node_strs;
        std::vector<uint64_t> node;
        this->nodes(node);
        for (uint64_t n : node) {
            node_strs.push_back(std::to_string(n));
        }
        node_str = boost::join(node_strs, ",");
    }
    LOG_INFO("raft %lu [peers: [%s], term: %lu, commit: %lu, applied: %lu, last_index: %lu, "
             "last_term: %lu]",
             id_, node_str.c_str(), term_, raft_log_->committed_, raft_log_->applied_,
             raft_log_->last_index(), raft_log_->last_term());
}

Raft::~Raft() {
}

void Raft::become_leader() {
    //follower不能直接转变为leader
    if (state_ == RaftState::Follower) {
        LOG_FATAL("invalid transition [follower -> leader]");
    }
    step_ = std::bind(&Raft::step_leader, this, std::placeholders::_1);
    reset(term_); //保持当前任期
    tick_ = std::bind(&Raft::tick_heartbeat, this);
    lead_ = id_; //设置为自己id，表示成为了leader
    state_ = RaftState::Leader;
    // Followers enter replicate mode when they've been successfully probed
    // (perhaps after having received a snapshot as a result). The leader is
    // trivially in this state. Note that r.reset() has initialized this
    // progress with the last index already.
    //查找并获取自己的Progress对象，并将状态设置为ProgressStateReplicate，表示进入复制模式
    auto it = prs_.find(id_);
    assert(it != prs_.end());
    it->second->become_replicate();

    // Conservatively set the pendingConfIndex to the last index in the
    // log. There may or may not be a pending config change, but it's
    // safe to delay any future proposals until we commit all our
    // pending log entries, and scanning the entire tail of the log
    // could be expensive.
    //保守地将pending_conf_index_设置为日志中的最后一个索引。这样可以安全地延迟任何未来的提议，直到提交所有待处理的日志条目
    pending_conf_index_ = raft_log_->last_index();
    auto empty_ent = std::make_shared<proto::Entry>();
    if (!append_entry(std::vector<proto::Entry>{*empty_ent})) {
        // This won't happen because we just called reset() above.
        LOG_FATAL("empty entry was dropped");
    }
    // As a special case, don't count the initial empty entry towards the
    // uncommitted log quota. This is because we want to preserve the
    // behavior of allowing one entry larger than quota if the current
    // usage is zero.
    //将空条目添加到entries中，并调用reduce_uncommitted_size减少未提交日志配额
    std::vector<proto::EntryPtr> entries{empty_ent};
    reduce_uncommitted_size(entries);
    LOG_INFO("%lu became leader at term %lu", id_, term_);
}
void Raft::become_follower(uint64_t term, uint64_t lead) {
    step_ = std::bind(&Raft::step_follower, this, std::placeholders::_1);
    reset(term);
    tick_ = std::bind(&Raft::tick_election, this);

    lead_ = lead;
    state_ = RaftState::Follower;

    LOG_INFO("%lu become follower at term %lu", id_, term_);
} // namespace kv

void Raft::become_candidate() {
    if (state_ == RaftState::Leader) {
        LOG_FATAL("invalid transition [leader->candidate]");
    }
    step_ = std::bind(&Raft::step_candidate, this, std::placeholders::_1);
    reset(term_ + 1);
    tick_ = std::bind(&Raft::tick_election, this);
    vote_ = id_;
    state_ = RaftState::Candidate;

    LOG_INFO("%lu become candidate at term %lu", id_, term_);
}

void Raft::become_pre_candidate() {
    if (state_ == RaftState::Leader) {
        LOG_FATAL("invalid transition [leader->pre_candidate]");
    }
    // Becoming a pre-candidate changes our step functions and state,
    // but doesn't change anything else. In particular it does not increase
    // r.Term or change r.Vote.
    step_ = std::bind(&Raft::step_candidate, this, std::placeholders::_1);
    votes_.clear();
    tick_ = std::bind(&Raft::tick_election, this);
    lead_ = 0;
    state_ = RaftState::PreCandidate;
    LOG_INFO("%lu become pre-candidate at term %lu", id_, term_);
}

Status Raft::step(proto::MessagePtr msg) {
    if (msg->term == 0) {
        //term为0 的消息
    } else if (msg->term > term_) { //传入消息term大于当前term
        if (msg->type == proto::MsgVote || msg->type == proto::MsgPreVote) {
            bool force = (Slice((const char *)msg->context.data(), msg->context.size())
                          == Slice(kCampaignTransfer));
            bool in_lease = (check_quorum_ && lead_ != 0 && election_elapsed_ < election_timeout_);
            //忽略非强制转移领导权请求且在租约期内的投票请求
            if (!force && in_lease) {
                // If a server receives a RequestVote request within the minimum election timeout
                // of hearing from a current leader, it does not update its term or grant its vote
                LOG_INFO(
                    "%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu [log_term: "
                    "%lu, index: %lu] at term %lu: lease is not expired (remaining ticks: %d)",
                    id_, raft_log_->last_term(), raft_log_->last_index(), vote_,
                    proto::msg_type_to_string(msg->type), msg->from, msg->log_term, msg->index,
                    term_, election_timeout_ - election_elapsed_);
                return Status::ok();
            }
        }
        switch (msg->type) {
            case proto::MsgPreVote:
                // Never change our term in response to a PreVote
                break;
            case proto::MsgPreVoteResp:
                if (!msg->reject) {
                    // We send pre-vote requests with a term in our future. If the
                    // pre-vote is granted, we will increment our term when we get a
                    // quorum. If it is not, the term comes from the node that
                    // rejected our vote so we should become a follower at the new
                    // term.
                    break;
                }
            default:
                LOG_INFO(
                    "%lu [term: %lu] received a %s message with higher term from %lu [term: %lu]",
                    id_, term_, proto::msg_type_to_string(msg->type), msg->from, msg->term);
                if (msg->type == proto::MsgApp || msg->type == proto::MsgHeartbeat
                    || msg->type == proto::MsgSnap) {
                    become_follower(msg->term, msg->from);
                } else {
                    become_follower(msg->term, 0);
                }
        }
    } else if (msg->term < term_) {
        //!
        if ((check_quorum_ || pre_vote_)
            && (msg->type == proto::MsgHeartbeat || msg->type == proto::MsgApp)) {
            // 生成一个MsgAppResp消息，并发送给消息的发送者。确保通知leader当前节点是活跃的，但不会扰乱当前的term
            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->type = proto::MsgAppResp;
            send(std::move(m));
        } else if (msg->type == proto::MsgPreVote) {
            LOG_INFO("%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu [log_term: "
                     "%lu, index: %lu] at term %lu",
                     id_, raft_log_->last_term(), raft_log_->last_index(), vote_,
                     proto::msg_type_to_string(msg->type), msg->from, msg->log_term, msg->index,
                     term_);
            //消息类型是MsgPreVote，生成一个拒绝的MsgPreVoteResp消息，并发送给消息的发送者。这确保了在启用预投票后，不会丢弃低term的消息而导致集群死锁
            proto::MessagePtr m(new proto::Message());
            m->to = msg->from;
            m->type = proto::MsgPreVoteResp;
            m->reject = true;
            m->term = term_;
            send(std::move(m));
        } else {
            //ignore other cases
            LOG_INFO("%lu [term: %lu] ignored a %s message with lower term from %lu [term: %lu]",
                     id_, term_, proto::msg_type_to_string(msg->type), msg->from, msg->term);
        }
        return Status::ok();
    }
    switch (msg->type) {
        case proto::MsgHup: {
            if (state_ != RaftState::Leader) {
                std::vector<proto::EntryPtr> entries;
                //获取未应用日志条目
                Status status = raft_log_->slice(raft_log_->applied_ + 1, raft_log_->committed_ + 1,
                                                 RaftLog::unlimited(), entries);
                if (!status.is_ok()) {
                    LOG_FATAL("unexpected error getting unapplied entries (%s)",
                              status.to_string().c_str());
                }
                //检查是否有待处理的配置更改
                uint32_t pending = num_of_pending_conf(entries);
                if (pending > 0 && raft_log_->committed_ > raft_log_->applied_) {
                    LOG_WARN("%lu cannot campaign at term %lu since there are still %u pending "
                             "configuration changes to apply",
                             id_, term_, pending);
                    return Status::ok();
                }
                LOG_INFO("%lu is starting a new election at term %lu", id_, term_);
                if (pre_vote_) {
                    campaign(kCampaignPreElection);
                } else {
                    campaign(kCampaignElection);
                }
            } else {
                LOG_DEBUG("%lu ignoring MsgHup because already leader", id_);
            }
            break;
        }
        case proto::MsgVote:
        case proto::MsgPreVote: {
            if (is_learner_) {
                LOG_INFO("%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu "
                         "[log_term: %lu, index: %lu] at term %lu: learner can not vote",
                         id_, raft_log_->last_term(), raft_log_->last_index(), vote_,
                         proto::msg_type_to_string(msg->type), msg->from, msg->log_term, msg->index,
                         msg->term);
                return Status::ok();
            }
            // We can vote if this is a repeat of a vote we've already cast...
            bool can_vote =
                //当前节点已经为请求的节点投过票
                vote_ == msg->from ||
                // ...we haven't voted and we don't think there's a leader yet in this term...
                //当前节点没投票且当前任期内没有leader
                (vote_ == 0 && lead_ == 0) ||
                // ...or this is a PreVote for a future term...
                //当前请求是预投票请求，且请求的任期大于当前任期
                (msg->type == proto::MsgPreVote && msg->term > term_);
            // ...and we believe the candidate is up to date.
            //如果可以投票且候选人日志是最新的，则投票，记录日志并发送投票响应消息
            if (can_vote && this->raft_log_->is_up_to_date(msg->index, msg->log_term)) {
                LOG_INFO("%lu [log_term: %lu, index: %lu, vote: %lu] cast %s for %lu [log_term: "
                         "%lu, index: %lu] at term %lu",
                         id_, raft_log_->last_term(), raft_log_->last_index(), vote_,
                         proto::msg_type_to_string(msg->type), msg->from, msg->log_term, msg->index,
                         term_);
                // When responding to Msg{Pre,}Vote messages we include the term
                // from the message, not the local term. To see why consider the
                // case where a single node was previously partitioned away and
                // it's local term is now of date. If we include the local term
                // (recall that for pre-votes we don't update the local term), the
                // (pre-)campaigning node on the other end will proceed to ignore
                // the message (it ignores all out of date messages).
                // The term in the original message and current local term are the
                // same in the case of regular votes, but different for pre-votes.
                proto::MessagePtr m(new proto::Message());
                m->to = msg->from;
                m->term = msg->term;
                m->type = vote_resp_msg_type(msg->type);
                send(std::move(m));
                if (msg->type == proto::MsgVote) {
                    //only record real votes
                    election_elapsed_ = 0;
                    vote_ = msg->from;
                }
            } else {
                LOG_INFO("%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu "
                         "[log_term: %lu, index: %lu] at term %lu",
                         id_, raft_log_->last_term(), raft_log_->last_index(), vote_,
                         proto::msg_type_to_string(msg->type), msg->from, msg->log_term, msg->index,
                         term_);
                //拒绝投票
                proto::MessagePtr m(new proto::Message());
                m->to = msg->from;
                m->term = term_;
                m->type = vote_resp_msg_type(msg->type);
                m->reject = true;
                send(std::move(m));
            }
            break;
        }
        default:
            return step_(msg);
    }
    return Status::ok();
}

Status Raft::step_leader(proto::MessagePtr msg) {
    // These message types do not require any progress for m.From.
    switch (msg->type) {
        case proto::MsgBeat: {
            bcast_heartbeat();
            return Status::ok();
        }
        case proto::MsgCheckQuorum:
            if (!check_quorum_active()) {
                LOG_WARN("%lu stepped down to follower since quorum is not active", id_);
                become_follower(term_, 0);
            }
            return Status::ok();
        case proto::MsgProp: {
            if (msg->entries.empty()) {
                LOG_FATAL("%lu stepped empty MsgProp", id_);
            }
            //如果当前节点不在配置中，丢弃提案
            auto it = prs_.find(id_);
            if (it == prs_.end()) {
                // If we are not currently a member of the range (i.e. this node
                // was removed from the configuration while serving as leader),
                // drop any new proposals.
                return Status::invalid_argument("raft proposal dropped");
            }
            //如果正在进行领导者迁移，丢弃提案
            if (lead_transferee_ != 0) {
                LOG_DEBUG(
                    "%lu [term %lu] transfer leadership to %lu is in progress; dropping proposal",
                    id_, term_, lead_transferee_);
                return Status::invalid_argument("raft proposal dropped");
            }
            //对于每一个提案条目，检查其类型是否为配置变更
            for (size_t i = 0; i < msg->entries.size(); ++i) {
                proto::Entry &e = msg->entries[i];
                if (e.type == proto::EntryConfChange) {
                    //!
                    if (pending_conf_index_ > raft_log_->applied_) {
                        //如果有未应用的配置变更，则将当前的配置变更条目忽略，重置为普通条目类型
                        LOG_INFO("propose conf %s ignored since pending unapplied configuration "
                                 "[index %lu, applied %lu]",
                                 proto::entry_type_to_string(e.type), pending_conf_index_,
                                 raft_log_->applied_);
                        e.type = proto::EntryNormal;
                        e.index = 0;
                        e.term = 0;
                        e.data.clear();
                    } else {
                        //如果没有未应用的配置变更，则更新pending_conf_index_为当前条目的索引
                        pending_conf_index_ = raft_log_->last_index() + i + 1;
                    }
                }
            }
            if (!append_entry(msg->entries)) {
                return Status::invalid_argument("raft proposal dropped");
            }
            bcast_append();
            return Status::ok();
        }
        //处理客户端读请求
        case proto::MsgReadIndex: {
            if (quorum() > 1) {
                uint64_t term = 0;
                raft_log_->term(raft_log_->committed_, term);
                if (term != term_) {
                    return Status::ok();
                }

                // thinking: use an interally defined context instead of the user given context.
                // We can express this in terms of the term and index instead of a user-supplied value.
                // This would allow multiple reads to piggyback on the same message.
                switch (read_only_->option) {
                    case ReadOnlySafe:
                        read_only_->add_request(raft_log_->committed_, msg);
                        bcast_heartbeat_with_ctx(msg->entries[0].data);
                        break;
                    case ReadOnlyLeaseBased:
                        if (msg->from == 0 || msg->from == id_) {
                            read_states_.push_back(ReadState{.index = raft_log_->committed_,
                                                             .request_ctx = msg->entries[0].data});
                        } else {
                            proto::MessagePtr m(new proto::Message());
                            m->to = m->from;
                            m->type = proto::MsgReadIndexResp;
                            m->index = raft_log_->committed_;
                            m->entries = msg->entries;
                            send(std::move(m));
                        }
                        break;
                }
            } else {
                read_states_.push_back(
                    ReadState{.index = raft_log_->committed_, .request_ctx = msg->entries[0].data});
            }
            return Status::ok();
        }
    }
    // All other message types require a progress for m.From (pr).
    auto pr = get_progress(msg->from);
    if (pr == nullptr) {
        LOG_DEBUG("%lu no progress available for %lu", id_, msg->from);
        return Status::ok();
    }
    switch (msg->type) {
        case proto::MsgAppResp: {
            pr->recent_active = true;
            if (msg->reject) {
                //记录日志表明收到拒绝消息
                LOG_DEBUG("%lu received msgApp rejection(last_index: %lu) from %lu for index %lu",
                          id_, msg->reject_hint, msg->from, msg->index);
                //尝试减少进度到提示位置
                if (pr->maybe_decreases_to(msg->index, msg->reject_hint)) {
                    LOG_DEBUG("%lu decreased progress of %lu to [%s]", id_, msg->from,
                              pr->string().c_str());
                    if (pr->state == ProgressStateReplicate) {
                        pr->become_probe();
                    }
                    send_append(msg->from);
                }
            } else {
                //检查进度是否暂停，并尝试更新进度到消息的索引位置
                bool old_paused = pr->is_paused();
                if (pr->maybe_update(msg->index)) {
                    if (pr->state == ProgressStateProbe) {
                        pr->become_replicate();
                        //如果节点状态是快照状态且需要终止快照，记录日志并转换为探测状态
                    } else if (pr->state == ProgressStateSnapshot && pr->need_snapshot_abort()) {
                        LOG_DEBUG("%lu snapshot aborted, resumed sending replication messages to "
                                  "%lu [%s]",
                                  id_, msg->from, pr->string().c_str());
                        // Transition back to replicating state via probing state
                        // (which takes the snapshot into account). If we didn't
                        // move to replicating state, that would only happen with
                        // the next round of appends (but there may not be a next
                        // round for a while, exposing an inconsistent RaftStatus).
                        pr->become_probe();
                        //如果节点状态是复制状态，释放到消息索引位置的inflights
                    } else if (pr->state == ProgressStateReplicate) {
                        pr->inflights->free_to(msg->index);
                    }
                    if (maybe_commit()) {
                        bcast_append();
                    } else if (old_paused) {
                        // If we were paused before, this node may be missing the
                        // latest commit index, so send it.
                        send_append(msg->from);
                    }
                    // We've updated flow control information above, which may
                    // allow us to send multiple (size-limited) in-flight messages
                    // at once (such as when transitioning from probe to
                    // replicate, or when freeTo() covers multiple messages). If
                    // we have more entries to send, send as many messages as we
                    // can (without sending empty messages for the commit index)
                    while (maybe_send_append(msg->from, false)) {
                    }
                    // Transfer leadership is in progress.
                    //如果领导者转移正在进行中且日志已经同步，发送MsgTimeoutNow消息以触发选举
                    if (msg->from == lead_transferee_ && pr->match == raft_log_->last_index()) {
                        LOG_INFO("%lu sent MsgTimeoutNow to %lu after received MsgAppResp", id_,
                                 msg->from);
                        send_timeout_now(msg->from);
                    }
                }
            }
        } break;
        case proto::MsgHeartbeatResp: {
            pr->recent_active = true;
            pr->resume();
            // free one slot for the full inflights window to allow progress.
            if (pr->state == ProgressStateReplicate && pr->inflights->is_full()) {
                pr->inflights->free_first_one();
            }
            if (pr->match < raft_log_->last_index()) {
                send_append(msg->from);
            }
            if (read_only_->option != ReadOnlySafe || msg->context.empty()) {
                return Status::ok();
            }
            uint32_t ack_count = read_only_->recv_ack(*msg);
            if (ack_count < quorum()) {
                return Status::ok();
            }

            auto rss = read_only_->advance(*msg);
            for (auto &rs : rss) {
                auto &req = rs->req;
                if (req.from == 0 || req.from == id_) {
                    ReadState read_state =
                        ReadState{.index = rs->index, .request_ctx = req.entries[0].data};
                    read_states_.push_back(std::move(read_state));
                } else {
                    proto::MessagePtr m(new proto::Message());
                    m->to = req.from;
                    m->type = proto::MsgReadIndexResp;
                    m->index = rs->index;
                    m->entries = req.entries;
                    send(std::move(m));
                }
            }
        } break;
        case proto::MsgSnapStatus: {
            if (pr->state != ProgressStateSnapshot) {
                return Status::ok();
            }
            if (!msg->reject) {
                pr->become_probe();
                LOG_DEBUG(
                    "%lu snapshot succeeded, resumed sending replication messages to %lu [%s]", id_,
                    msg->from, pr->string().c_str());
            } else {
                pr->snapshot_failure();
                pr->become_probe();
                LOG_DEBUG("%lu snapshot failed, resumed sending replication messages to %lu [%s]",
                          id_, msg->from, pr->string().c_str());
            }
            // If snapshot finish, wait for the msgAppResp from the remote node before sending
            // out the next msgApp.
            // If snapshot failure, wait for a heartbeat interval before next try
            pr->set_pause();
            break;
        }
        case proto::MsgUnreachable: {
            // During optimistic replication, if the remote becomes unreachable,
            // there is huge probability that a MsgApp is lost.
            if (pr->state == ProgressStateReplicate) {
                pr->become_probe();
            }
            LOG_DEBUG("%lu failed to send message to %lu because it is unreachable [%s]", id_,
                      msg->from, pr->string().c_str());
            break;
        }
        case proto::MsgTransferLeader: {
            if (pr->is_learner) {
                LOG_DEBUG("%lu is learner. Ignored transferring leadership", id_);
                return Status::ok();
            }
            //从消息中获取领导者转移的目标节点，并保存上次转移的目标节点
            uint64_t lead_transferee = msg->from;
            uint64_t last_lead_transferee = lead_transferee_;
            //处理上次转移未完成情况
            if (last_lead_transferee != 0) {
                //如果上一次转移尚未完成且目标节点与本次相同，忽略本次请求。如果不同，终止上次转移
                if (last_lead_transferee == lead_transferee) {
                    LOG_INFO("%lu [term %lu] transfer leadership to %lu is in progress, ignores "
                             "request to same node %lu",
                             id_, term_, lead_transferee, lead_transferee);
                    return Status::ok();
                }
                abort_leader_transfer();
                LOG_INFO("%lu [term %lu] abort previous transferring leadership to %lu", id_, term_,
                         last_lead_transferee);
            }
            //转移给自身
            if (lead_transferee == id_) {
                LOG_DEBUG("%lu is already leader. Ignored transferring leadership to self", id_);
                return Status::ok();
            }
            // Transfer leadership to third party.
            LOG_INFO("%lu [term %lu] starts to transfer leadership to %lu", id_, term_,
                     lead_transferee);
            // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
            election_elapsed_ = 0;
            lead_transferee_ = lead_transferee;
            //如果目标节点已经与领导者的日志同步，立即发送MsgTimeoutNow触发选举，否则发送AppendEntries同步日志
            if (pr->match == raft_log_->last_index()) {
                send_timeout_now(lead_transferee);
                LOG_INFO(
                    "%lu sends MsgTimeoutNow to %lu immediately as %lu already has up-to-date log",
                    id_, lead_transferee, lead_transferee);
            } else {
                send_append(lead_transferee);
            }
            break;
        }
    }
    return Status::ok();
}

Status Raft::step_candidate(proto::MessagePtr msg) {
    // Only handle vote responses corresponding to our candidacy (while in
    // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
    // our pre-candidate state).
    switch (msg->type) {
        case proto::MsgProp:
            LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
            return Status::invalid_argument("raft proposal dropped");
            //MsgApp，MsgHeartbeat，MsgSnap表明集群中已经存在领导者了，因此候选者此时需要变成跟随着并处理响应的消息
        case proto::MsgApp:
            become_follower(msg->term, msg->from);
            handle_append_entries(std::move(msg));
            break;
        case proto::MsgHeartbeat:
            become_follower(msg->term, msg->from);
            handle_heartbeat(std::move(msg));
            break;

        case proto::MsgSnap:
            become_follower(msg->term, msg->from);
            handle_snapshot(std::move(msg));
            break;
        case proto::MsgPreVoteResp:
        case proto::MsgVoteResp: {
            uint64_t gr = poll(msg->from, msg->type, !msg->reject);
            LOG_INFO("%lu [quorum:%u] has received %lu %s votes and %lu vote rejections", id_,
                     quorum(), gr, proto::msg_type_to_string(msg->type), votes_.size() - gr);
            if (quorum() == gr) {
                if (state_ == RaftState::PreCandidate) {
                    campaign(kCampaignElection);
                } else {
                    assert(state_ == RaftState::Candidate);
                    become_leader();
                    bcast_append();
                }
            } else if (quorum() == votes_.size() - gr) {
                // pb.MsgPreVoteResp contains future term of pre-candidate
                // m.Term > r.Term; reuse r.Term
                become_follower(term_, 0);
            }
            break;
        }
            //该消息被忽略，因为它通常在候选者状态下没意义
        case proto::MsgTimeoutNow: {
            LOG_DEBUG("%lu [term %lu state %d] ignored MsgTimeoutNow from %lu", id_, term_, state_,
                      msg->from);
        }
    }
    return Status::ok();
}

Status Raft::step_follower(proto::MessagePtr msg) {
    switch (msg->type) {
        case proto::MsgProp:
            if (lead_ == 0) {
                LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
                return Status::invalid_argument("raft proposal dropped");
            } else if (disable_proposal_forwarding_) {
                //禁用了提案转发
                LOG_INFO("%lu not forwarding to leader %lu at term %lu; dropping proposal", id_,
                         lead_, term_);
                return Status::invalid_argument("raft proposal dropped");
            }
            msg->to = lead_;
            send(msg);
            break;
        case proto::MsgApp: {
            election_elapsed_ = 0;
            lead_ = msg->from;
            handle_append_entries(msg);
            break;
        }
        case proto::MsgHeartbeat: {
            election_elapsed_ = 0;
            lead_ = msg->from;
            handle_heartbeat(msg);
            break;
        }
        case proto::MsgSnap: {
            election_elapsed_ = 0;
            lead_ = msg->from;
            handle_snapshot(msg);
            break;
        }
        case proto::MsgTransferLeader: {
            if (lead_ == 0) {
                LOG_INFO("%lu no leader at term %lu; dropping leader transfer msg", id_, term_);
                return Status::ok();
            }
            msg->to = lead_;
            send(msg);
            break;
        }
        case proto::MsgTimeoutNow: {
            if (promotable()) {
                LOG_INFO("%lu [term %lu] received MsgTimeoutNow from %lu and starts an election to "
                         "get leadership.",
                         id_, term_, msg->from);
                // Leadership transfers never use pre-vote even if r.preVote is true; we
                // know we are not recovering from a partition so there is no need for the
                // extra round trip.
                campaign(kCampaignTransfer);
            } else {
                LOG_INFO("%lu received MsgTimeoutNow from %lu but is not promotable", id_,
                         msg->from);
            }
            break;
        }
        case proto::MsgReadIndex:
            if (lead_ == 0) {
                LOG_INFO("%lu no leader at term %lu; dropping index reading msg", id_, term_);
                return Status::ok();
            }
            msg->to = lead_;
            send(msg);
            break;
        case proto::MsgReadIndexResp:
            if (msg->entries.size() != 1) {
                LOG_ERROR("%lu invalid format of MsgReadIndexResp from %lu, entries count: %lu",
                          id_, msg->from, msg->entries.size());
                return Status::ok();
            }
            ReadState rs;
            rs.index = msg->index;
            rs.request_ctx = std::move(msg->entries[0].data);
            read_states_.push_back(std::move(rs));
            break;
    }
    return Status::ok();
}

void Raft::handle_append_entries(proto::MessagePtr msg) {
    //如果消息中的日志索引小于当前节点已提交的日志索引（说明消息中的日志被处理过或是过期的）
    if (msg->index < raft_log_->committed_) {
        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->type = proto::MsgAppResp;
        m->index = raft_log_->committed_;
        send(std::move(m));
        return;
    }
    //将消息中的日志条目转换为指针类型
    std::vector<proto::EntryPtr> entries;
    for (proto::Entry &entry : msg->entries) {
        entries.push_back(std::make_shared<proto::Entry>(std::move(entry)));
    }
    bool ok = false;
    uint64_t last_index = 0;

    //尝试追加日志条目
    raft_log_->maybe_append(msg->index, msg->log_term, msg->commit, std::move(entries), last_index,
                            ok);
    if (ok) {
        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->type = proto::MsgAppResp;
        m->index = last_index;
        send(std::move(m));
    } else {
        uint64_t term = 0;
        raft_log_->term(msg->index, term);
        LOG_DEBUG(
            "%lu [log_term: %lu, index: %lu] rejected msgApp [log_term: %lu, index: %lu] from %lu",
            id_, term, msg->index, msg->log_term, msg->index, msg->from)
        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->type = proto::MsgAppResp;
        m->index = msg->index;
        m->reject = true;
        m->reject_hint = raft_log_->last_index();
        send(std::move(m));
    }
}

void Raft::handle_heartbeat(proto::MessagePtr msg) {
    //将本地日志的提交索引更新为领导者发送的提交索引
    raft_log_->commit_to(msg->commit);
    proto::MessagePtr m(new proto::Message());
    m->to = msg->from; //心跳响应消息的目标节点m->to为心跳消息的发送者，即领导者节点
    m->type = proto::MsgHeartbeatResp;
    //将心跳信息的上下文信息传递给响应消息，上下文信息可以携带一些额外的数据，在某些情况下对领导者有用
    msg->context = std::move(msg->context);
    //发送心跳响应信息
    send(std::move(m));
}

void Raft::handle_snapshot(proto::MessagePtr msg) {
    uint64_t sindex = msg->snapshot.metadata.index; //获取快照索引
    uint64_t sterm = msg->snapshot.metadata.term;   //获取快照的任期

    if (restore(msg->snapshot)) { //尝试恢复快照数据，判断恢复是否成功
        LOG_INFO("%lu [commit: %lu] restored snapshot [index: %lu, term: %lu]", id_,
                 raft_log_->committed_, sindex, sterm);
        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->type = proto::MsgAppResp;
        msg->index = raft_log_->last_index();
        send(std::move(m));
    } else {
        LOG_INFO("%lu [commit: %lu] ignored snapshot [index: %lu, term: %lu]", id_,
                 raft_log_->committed_, sindex, sterm);
        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->type = proto::MsgAppResp;
        msg->index = raft_log_->committed_;
        send(std::move(m));
    }
}

bool Raft::restore(const proto::Snapshot &s) {
    //如果快照的索引小于或等于当前已提交的索引，说明快照已经过期，无需恢复，直接返回false
    if (s.metadata.index <= raft_log_->committed_) {
        return false;
    }
    //如果当前日志匹配快照的索引和任期，则说明已经包含了快照的数据，只需要快速提交到快照的索引，无需完全恢复
    if (raft_log_->match_term(s.metadata.index, s.metadata.term)) {
        LOG_INFO("%lu [commit: %lu, last_index: %lu, last_term: %lu] fast-forwarded commit to "
                 "snapshot [index: %lu, term: %lu]",
                 id_, raft_log_->committed_, raft_log_->last_index(), raft_log_->last_term(),
                 s.metadata.index, s.metadata.term);
        raft_log_->commit_to(s.metadata.index);
        return false;
    }
    // The normal peer can't become learner.
    //正常节点不能变成学习者。如果当前节点在快照中的学习者列表里，则记录错误日志并返回false
    if (!is_learner_) {
        for (uint64_t id : s.metadata.conf_state.learners) {
            if (id == id_) {
                LOG_ERROR("%lu can't become learner when restores snapshot [index: %lu, term: %lu]",
                          id_, s.metadata.index, s.metadata.term);
                return false;
            }
        }
    }
    LOG_INFO("%lu [commit: %lu, last_index: %lu, last_term: %lu] starts to restore snapshot "
             "[index: %lu, term: %lu]",
             id_, raft_log_->committed_, raft_log_->last_index(), raft_log_->last_term(),
             s.metadata.index, s.metadata.term);
    proto::SnapshotPtr snap(new proto::Snapshot(s));
    raft_log_->restore(snap);
    //清空当前的进度列表
    prs_.clear();
    learner_prs_.clear();
    restore_node(s.metadata.conf_state.nodes, false);
    restore_node(s.metadata.conf_state.learners, true);
    return true;
}

void Raft::tick() {
    if (tick_) {
        tick_();
    } else {
        LOG_WARN("tick function is not set");
    }
}

void Raft::campaign(const std::string &campaign_type) {
    uint64_t term = 0;
    proto::MessageType vote_msg = 0;
    if (campaign_type == kCampaignPreElection) {
        become_pre_candidate();
        vote_msg = proto::MsgPreVote;
        term = term_ + 1;
    } else {
        become_candidate();
        vote_msg = proto::MsgVote;
        term = term_;
    }
    if (quorum() == poll(id_, vote_resp_msg_type(vote_msg), true)) {
        if (campaign_type == kCampaignPreElection) {
            campaign(kCampaignElection);
        } else {
            become_leader();
        }
        return;
    }
    //向其他节点发送投票请求
    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        if (it->first == id_) {
            continue;
        }

        LOG_INFO("%lu [log_term: %lu, index: %lu] sent %s request to %lu at term %lu", id_,
                 raft_log_->last_term(), raft_log_->last_index(),
                 proto::msg_type_to_string(vote_msg), it->first, term_);

        std::vector<uint8_t> ctx;
        //领导权转移
        if (campaign_type == kCampaignTransfer) {
            ctx = std::vector<uint8_t>(kCampaignTransfer.begin(), kCampaignTransfer.end());
        }
        proto::MessagePtr msg(new proto::Message());
        msg->term = term;
        msg->to = it->first;
        msg->type = vote_msg;
        msg->index = raft_log_->last_index();
        msg->log_term = raft_log_->last_term();
        send(std::move(msg));
    }
}

void Raft::send(proto::MessagePtr msg) {
    msg->from = id_;
    if (msg->type == proto::MsgVote || msg->type == proto::MsgVoteResp
        || msg->type == proto::MsgPreVote || msg->type == proto::MsgPreVoteResp) {
        //竞选的term必须被设置
        if (msg->term == 0) {
            LOG_FATAL("term should be set when sending %s", proto::msg_type_to_string(msg->type));
        }
    } else {
        //其他消息类型的term不应该被设置
        if (msg->term != 0) {
            LOG_FATAL("term should not be set when sending %d (was %lu)", msg->type, msg->term);
        }
        //提案消息和读取索引消息不需要附加term，因为他们会被转发给当前领导者进行处理
        if (msg->term != proto::MsgProp && msg->type != proto::MsgReadIndex) {
            msg->term = term_;
        }
    }
    msgs_.push_back(std::move(msg));
}

void Raft::restore_node(const std::vector<uint64_t> &nodes, bool is_learner) {
    for (uint64_t node : nodes) {
        uint64_t match = 0;
        uint64_t next = raft_log_->last_index() + 1;
        //node是本地节点
        if (node == id_) {
            match = next - 1;
            is_learner_ = is_learner;
        }
        //设置节点node的进度信息
        set_progress(node, match, next, is_learner);
        LOG_INFO("%lu restored progress of %lu [%s]", id_, node,
                 get_progress(id_)->string().c_str());
    }
}

uint32_t Raft::poll(uint64_t id, proto::MessageType type, bool v) {
    uint32_t granted = 0; //赞成票计数器
    if (v) {
        LOG_INFO("%lu received %s from %lu at term %lu", id_, proto::msg_type_to_string(type), id,
                 term_);
    } else {
        LOG_INFO("%lu received %s rejection from %lu at term %lu", id_,
                 proto::msg_type_to_string(type), id, term_);
    }
    auto it = votes_.find(id);
    if (it == votes_.end()) {
        votes_[id] = v; //如果该节点之前没投票记录，则将其投票结果v记录到votes_中
    }
    //统计赞成票数
    for (auto it = votes_.begin(); it != votes_.end(); ++it) {
        if (it->second) {
            granted++;
        }
    }
    return granted;
}

bool Raft::past_election_timeout() {
    return election_elapsed_ >= randomized_election_timeout_;
}

bool Raft::promotable() const {
    auto it = prs_.find(id_);
    return it != prs_.end();
}

bool Raft::append_entry(const std::vector<proto::Entry> &entries) {
    uint64_t li = raft_log_->last_index();
    std::vector<proto::EntryPtr> ents(entries.size(), nullptr);
    for (size_t i = 0; i < entries.size(); ++i) {
        proto::EntryPtr ent(new proto::Entry());
        ent->term = term_;
        ent->index = li + i + 1;
        ent->data = entries[i].data;
        ent->type = entries[i].type;
        ents[i] = ent;
    }

    if (!increase_uncommitted_size(ents)) {
        LOG_DEBUG("%lu appending new entries to log would exceed uncommitted entry size limit; "
                  "dropping proposal",
                  id_);
        return false;
    }
    // use latest "last" index after truncate/append
    //将新日志条目追加到当前节点的日志中
    li = raft_log_->append(ents);
    get_progress(id_)->maybe_update(li);
    // Regardless of maybeCommit's return, our caller will call bcastAppend.
    maybe_commit();
    return true;
}

void Raft::tick_election() {
    election_elapsed_++;
    if (promotable() && past_election_timeout()) {
        election_elapsed_ = 0;
        proto::MessagePtr msg(new proto::Message());
        msg->from = id_;
        msg->type = proto::MsgHup;
        step(std::move(msg));
    }
}

void Raft::tick_heartbeat() {
    heartbeat_elapsed_++;
    election_elapsed_++;
    if (election_elapsed_ >= election_timeout_) {
        election_elapsed_ = 0;
        if (check_quorum_) {
            proto::MessagePtr msg(new proto::Message());
            msg->from = id_;
            msg->type = proto::MsgCheckQuorum;
            step(std::move(msg));
        }
        // If current leader cannot transfer leadership in electionTimeout, it becomes leader again.
        if (state_ == RaftState::Leader && lead_transferee_ != 0) {
            abort_leader_transfer();
        }
    }
    if (state_ != RaftState::Leader) {
        return;
    }
    //发送心跳消息
    if (heartbeat_elapsed_ >= heartbeat_timeout_) {
        heartbeat_elapsed_ = 0;
        proto::MessagePtr msg(new proto::Message());
        msg->from = id_;
        msg->type = proto::MsgBeat;
        step(std::move(msg));
    }
}

void Raft::send_heartbeat(uint64_t to, std::vector<uint8_t> ctx) {
    // Attach the commit as min(to.matched, r.committed).
    // When the leader sends out heartbeat message,
    // the receiver(follower) might not be matched with the leader
    // or it might not have all the committed entries.
    // The leader MUST NOT forward the follower's commit to
    // an unmatched index.
    /*
    表明了领导者已经提交的最新日志条目，同时也表明了follower与leader之间的匹配情况
    */
    uint64_t commit = std::min(get_progress(to)->match, raft_log_->committed_);
    proto::MessagePtr msg(new proto::Message());
    msg->to = to;
    msg->type = proto::MsgHeartbeat;
    msg->commit = commit;
    msg->context = std::move(ctx); //附带上下文信息
    send(std::move(msg));
}

void Raft::send_append(uint64_t to) {
    maybe_send_append(to, true);
}
bool Raft::maybe_send_append(uint64_t to, bool send_if_empty) {
    ProgressPtr pr = get_progress(to);
    if (pr->is_paused()) {
        return false;
    }
    //创建一个新的消息对象并设置目标节点
    proto::MessagePtr msg(new proto::Message());
    msg->to = to;
    uint64_t term = 0;
    Status status_term = raft_log_->term(pr->next - 1, term);
    std::vector<proto::EntryPtr> entries;
    Status status_entries = raft_log_->entries(pr->next, max_msg_size_, entries);
    if (entries.empty() && !send_if_empty) {
        return false;
    }
    //获取任期或日志条目失败的情况，需要发送快照消息
    //如果获取日志条目或任期失败，检查目标节点最近是否活跃
    //如果目标节点最近不活跃，跳过发送快照消息
    //创建并发送快照消息
    //更新目标节点的进度为快照状态
    if (!status_term.is_ok()
        || !status_entries.is_ok()) { // send snapshot if we failed to get term or entries
        if (!pr->recent_active) {
            LOG_DEBUG("ignore sending snapshot to %lu since it is not recently active", to)
            return false;
        }
        msg->type = proto::MsgSnap;

        proto::SnapshotPtr snap;
        Status status = raft_log_->snapshot(snap);
        if (!status.is_ok()) {
            LOG_FATAL("snapshot error %s", status.to_string().c_str());
        }
        if (snap->is_empty()) {
            LOG_FATAL("need non-empty snapshot");
        }
        uint64_t sindex = snap->metadata.index;
        uint64_t sterm = snap->metadata.term;
        LOG_DEBUG(
            "%lu [first_index: %lu, commit: %lu] sent snapshot[index: %lu, term: %lu] to %lu [%s]",
            id_, raft_log_->first_index(), raft_log_->committed_, sindex, sterm, to,
            pr->string().c_str());
        pr->become_snapshot(sindex);
        msg->snapshot = *snap;
        LOG_DEBUG("%lu paused sending replication messages to %lu [%s]", id_, to,
                  pr->string().c_str());
    } else {
        //成功获取日志条目和任期，创建日志追加消息
        msg->type = proto::MsgApp;
        msg->index = pr->next - 1;
        msg->log_term = term;
        //遍历从raft_log_获取的日志条目entries
        for (proto::EntryPtr &entry : entries) {
            //copy
            //将每个日志条目的副本添加到消息的entries数组中
            msg->entries.emplace_back(*entry);
        }
        msg->commit = raft_log_->committed_;
        if (!msg->entries.empty()) {
            switch (pr->state) {
                    // optimistically increase the next when in ProgressStateReplicate
                    //处于乐观复制状态
                case ProgressStateReplicate: {
                    uint64_t last = msg->entries.back().index;
                    pr->optimistic_update(last);
                    //将这个索引添加到正在进行的飞行消息集合中
                    pr->inflights->add(last);
                    break;
                }
                case ProgressStateProbe: {
                    pr->set_pause();
                    break;
                }
                default: {
                    LOG_FATAL("%lu is sending append in unhandled state %s", id_,
                              progress_state_to_string(pr->state));
                }
            }
        }
    }
    send(std::move(msg));
    return true;
}

void Raft::for_each_progress(const std::function<void(uint64_t, ProgressPtr &)> &callback) {
    //std::unordered_map<uint64_t, ProgressPtr> prs_;

    for (auto it = prs_.begin(); it != prs_.end(); ++it) {
        callback(it->first, it->second);
    }

    for (auto it = learner_prs_.begin(); it != learner_prs_.end(); ++it) {
        callback(it->first, it->second);
    }
}

void Raft::bcast_append() {
    for_each_progress([this](uint64_t id, ProgressPtr &progress) {
        if (id == id_) {
            return;
        }
        this->send_append(id);
    });
}

void Raft::bcast_heartbeat() {
    std::vector<uint8_t> ctx;
    read_only_->last_pending_request_ctx(ctx);
    bcast_heartbeat_with_ctx(std::move(ctx));
}

void Raft::bcast_heartbeat_with_ctx(const std::vector<uint8_t> &ctx) {
    for_each_progress([this, ctx](uint64_t id, ProgressPtr &progress) {
        if (id == id_) {
            return;
        }
        this->send_heartbeat(id, std::move(ctx));
    });
}

bool Raft::maybe_commit() {
    // Preserving matchBuf across calls is an optimization
    // used to avoid allocating a new slice on each call.
    //match_buf是一个临时缓冲区，用来存储所有节点的match值（即每个节点已经复制的最新日志条目的索引
    //每次调用maybe_commit时先清空这个缓冲区
    match_buf_.clear();
    for (auto it = prs_.begin(); it != prs_.end(); it++) {
        match_buf_.push_back(it->second->match);
    }
    std::sort(match_buf_.begin(), match_buf_.end());
    //获取当前日志的提交点mci：排序后match_buf_中第quorum个元素的值。表示多数节点已经复制的日志索引
    //mci是排序后match_buf_第quorum个元素的值
    auto mci = match_buf_[match_buf_.size() - quorum()];
    //检查并更新日志提交点。
    //mci大于当前的提交点，且当前任期内，更新提交点并返回true
    return raft_log_->maybe_commit(mci, term_);
}

void Raft::reset(uint64_t term) {
    if (term_ != term) {
        term_ = term;
        vote_ = 0;
    }
    lead_ = 0;

    election_elapsed_ = 0;
    heartbeat_elapsed_ = 0;

    reset_randomized_election_timeout();

    abort_leader_transfer();

    votes_.clear();

    for_each_progress([this](uint64_t id, ProgressPtr &progress) {
        bool is_learner = progress->is_learner; //保留学习者属性
        progress = std::make_shared<Progress>(max_inflight_);
        progress->next = raft_log_->last_index() + 1;
        progress->is_learner = is_learner;

        if (id == id_) {
            progress->match = raft_log_->last_index();
        }
    });

    pending_conf_index_ = 0;
    uncommitted_size_ = 0;
    read_only_->pending_read_index.clear();
    read_only_->read_index_queue.clear();
}

SoftStatePtr Raft::soft_state() const {
    return std::make_shared<SoftState>(lead_, state_);
}

proto::HardState Raft::hard_state() const {
    proto::HardState hs;
    hs.term = term_;
    hs.vote = vote_;
    hs.commit = raft_log_->committed_;
    return hs;
}

void Raft::load_state(const proto::HardState &state) {
    //检查state.commit是否在raft_log_的已提交索引和最后一个日志索引范围内
    if (state.commit < raft_log_->committed_ || state.commit > raft_log_->last_index()) {
        LOG_FATAL("%lu state.commit %lu is out of range [%lu, %lu]", id_, state.commit,
                  raft_log_->committed_, raft_log_->last_index());
    } // namespace kv
    raft_log_->committed_ = state.commit;
    term_ = state.term;
    vote_ = state.vote;
}

void Raft::reset_randomized_election_timeout() {
    //固定选举超时时间+随机时间偏移量
    randomized_election_timeout_ = election_timeout_ + random_device_.gen();
    assert(randomized_election_timeout_ <= 2 * election_timeout_);
}

bool Raft::check_quorum_active() {
    size_t act = 0;
    for_each_progress([&act, this](uint64_t id, ProgressPtr &pr) {
        if (id == this->id_) {
            act++;
            return;
        }
        if (pr->recent_active && !pr->is_learner) {
            act++;
        }
    });
    return act >= quorum();
}

void Raft::abort_leader_transfer() {
    lead_transferee_ = 0;
}

void Raft::nodes(std::vector<uint64_t> &node) const {
    for (auto it = prs_.begin(); it != prs_.end(); it++) {
        node.push_back(it->first);
    }
    std::sort(node.begin(), node.end());
}

void Raft::learner_nodes(std::vector<uint64_t> &learner) const {
    for (auto it = learner_prs_.begin(); it != learner_prs_.end(); it++) {
        learner.push_back(it->first);
    }
    std::sort(learner.begin(), learner.end());
}

ProgressPtr Raft::get_progress(uint64_t id) {
    auto it = prs_.find(id);
    if (it != prs_.end()) {
        return it->second;
    }
    it = learner_prs_.find(id);
    if (it != learner_prs_.end()) {
        return it->second;
    }
    return nullptr;
}

void Raft::set_progress(uint64_t id, uint64_t match, uint64_t next, bool is_learner) {
    if (!is_learner) {
        learner_prs_.erase(id); //节点不是学习者，从学习者集合中移除该节点的进度信息
        ProgressPtr progress(new Progress(max_inflight_)); //创建新的进度对象
        progress->next = next;
        progress->match = match;
        prs_[id] = progress; //将新的进度对象添加到进度集合中
        return;
    }
    auto it = prs_.find(id);
    if (it != prs_.end()) {
        LOG_FATAL("%lu unexpected changing from voter to learner for %lu", id_, id);
    }

    ProgressPtr progress(new Progress(max_inflight_));
    progress->next = next;
    progress->match = match;
    progress->is_learner = true; //设置为学习者

    learner_prs_[id] = progress;
}

void Raft::del_progress(uint64_t id) {
    prs_.erase(id);
    learner_prs_.erase(id);
}

void Raft::send_timeout_now(uint64_t to) {
    proto::MessagePtr msg(new proto::Message());
    msg->to = to;
    msg->type = proto::MsgTimeoutNow;
    send(std::move(msg));
}

bool Raft::increase_uncommitted_size(const std::vector<proto::EntryPtr> &entries) {
    uint32_t s = 0;
    for (auto &entry : entries) {
        s += entry->payload_size();
    }
    /*
        如果raft日志未提交部分为空，即所有日志条目都已提交，系统允许任何大小的新日志
        因为当前日志已经处于稳定状态，添加新日志不会引发冲突或影响稳定性

        如果非空，则限制，防止未提交日志部分过大
    */
    if (uncommitted_size_ > 0 && uncommitted_size_ + s > max_uncommitted_size_) {
        // If the uncommitted tail of the Raft log is empty, allow any size
        // proposal. Otherwise, limit the size of the uncommitted tail of the
        // log and drop any proposal that would push the size over the limit.
        return false;
    }
    uncommitted_size_ += s;
    return true;
}

void Raft::reduce_uncommitted_size(const std::vector<proto::EntryPtr> &entries) {
    if (uncommitted_size_ == 0) {
        // Fast-path for followers, who do not track or enforce the limit.
        return;
    }

    uint32_t size = 0;

    for (const proto::EntryPtr &e : entries) {
        //遍历每个日志条目，计算总大小
        size += e->payload_size();
    }
    if (size > uncommitted_size_) {
        // uncommittedSize may underestimate the size of the uncommitted Raft
        // log tail but will never overestimate it. Saturate at 0 instead of
        // allowing overflow.
        //如果计算的总大小超过了uncommitted_size_的值，则将uncommitted_size_设为0
        uncommitted_size_ = 0;
    } else {
        //否则从uncommitted_size_中减去中大小
        uncommitted_size_ -= size;
    }
}

void Raft::add_node_or_learner(uint64_t id, bool is_learner) {
    ProgressPtr pr = get_progress(id);
    if (pr == nullptr) { //节点不存在，设置初始进度
        set_progress(id, 0, raft_log_->last_index() + 1, is_learner);
    } else {
        //尝试将普通节点改为学习者，日志记录错误信息
        if (is_learner && !pr->is_learner) {
            // can only change Learner to Voter
            LOG_INFO(
                "%lu ignored addLearner: do not support changing %lu from raft peer to learner.",
                id_, id);
            return;
        }
        //当前角色与目标角色相同，忽略重复操作
        if (is_learner == pr->is_learner) {
            // Ignore any redundant addNode calls (which can happen because the
            // initial bootstrapping entries are applied twice).
            return;
        }
        //学习者转变为普通节点
        // change Learner to Voter, use origin Learner progress
        learner_prs_.erase(id);
        pr->is_learner = false;
        prs_[id] = pr;
    }

    if (id_ == id) {
        is_learner_ = is_learner;
    }

    // When a node is first added, we should mark it as recently active.
    // Otherwise, CheckQuorum may cause us to step down if it is invoked
    // before the added node has a chance to communicate with us.
    get_progress(id)->recent_active = true;
}

void Raft::add_node(uint64_t id) {
    add_node_or_learner(id, false);
}
void Raft::remove_node(uint64_t id) {
    del_progress(id);

    // do not try to commit or abort transferring if there is no nodes in the cluster.
    if (prs_.empty() && learner_prs_.empty()) {
        return;
    }

    // The quorum size is now smaller, so see if any pending entries can
    // be committed.campaign
    //节点移除后，集群的法定人数可能会发生变化，调用maybe_commit检查是否有可以提交的日志条目
    if (maybe_commit()) {
        bcast_append();
    }
    // If the removed node is the leadTransferee, then abort the leadership transferring.
    if (state_ == RaftState::Leader && lead_transferee_ == id) {
        abort_leader_transfer();
    }
}
}; // namespace kv