#include <boost/algorithm/string.hpp>
#include "raft.h"
#include "proto.h"
#include "util.h"

namespace kv {

static const std::string kCampaignPreElection = "kCampaignPreElection";
static const std::string kCampaignElection = "kCampaignElection";
static const std::string kCampaignTransfer = "kCampaignTransfer";

Status Raft::step(proto::MessagePtr msg) {
    if (msg->term == 0) {
    } else if (msg->term > term_) {
        switch (msg->type) {}
    } else if (msg->term < term_) {
    }
    switch (msg->type) {
        case proto::MsgHup: {
            if (state_ != RaftState::Leader) {
                //TODO：获取未应用的日志条目
                /*code*/
                std::vector<proto::EntryPtr> entries;
                //TODO：检查是否有待处理的配置更改
                /*code*/
                if (pre_vote_) {
                    campaign(kCampaignPreElection);
                } else {
                    campaign(kCampaignElection);
                }
            }
        }
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
        proto::MessagePtr msg(new proto::Message());
        msg->term = term;
        msg->to = it->first;
        msg->type = vote_msg;
        //todo:raft log;
        // msg->index=
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