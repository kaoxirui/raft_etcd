#include "proto.h"
#include <msgpack.hpp>
namespace kv {
namespace proto {
    /*
        在8位无符号整数uint8_t中，数的取值范围是0-255，00000000-11111111
        128的二进制是10000000
        小于128的数，可以用1字节的7位存储，d=5，00000101
        当一个数值大于或等于128时（即二进制需要8位或更多位来表示），一个字节的7位不再足够表示。举个例子：
        在紧凑型编码中，由于要保留1位来作为标志位，只有7位可以用于存储数值
    */
    static uint32_t u8_serialize_size(uint8_t d) {
        //如果d小于128，则可以编码为一个fixnum，占用一个字节
        if (d < (1 << 7)) {
            return 1;
            //否则d是一个普通的无符号8位整数，占用两个字节
        } else {
            return 2;
        }
    }

    uint32_t Entry::serialize_size() const {}

    const char *msg_type_to_string(MessageType type) {
        switch (type) {
            case MsgHup:
                return "MsgHup";

            case MsgBeat:
                return "MsgBeat";

            case MsgProp:
                return "MsgProp";

            case MsgApp:
                return "MsgApp";

            case MsgAppResp:
                return "MsgAppResp";

            case MsgVote:
                return "MsgVote";

            case MsgVoteResp:
                return "MsgVoteResp";

            case MsgSnap:
                return "MsgSnap";

            case MsgHeartbeat:
                return "MsgHeartbeat";

            case MsgHeartbeatResp:
                return "MsgHeartbeatResp";

            case MsgUnreachable:
                return "MsgUnreachable";

            case MsgSnapStatus:
                return "MsgSnapStatus";

            case MsgCheckQuorum:
                return "MsgCheckQuorum";

            case MsgTeansferLeader:
                return "MsgTeansferLeader";

            case MsgTimeoutNow:
                return "MsgTimeoutNow";

            case MsgReadIndex:
                return "MsgReadIndex";

            case MsgReadIndexResp:
                return "MsgReadIndexResp";

            case MsgPreVote:
                return "MsgPreVote";

            case MsgPreVoteResp:
                return "MsgPreVoteResp";

            case MsgTypeSize:
                return "MsgTypeSize";

            default:
                LOG_FATAL("invalid msg type %d", type);
        }
    }

    const char *entry_type_to_string(EntryType type) {
        switch (type) {
            case EntryNormal:
                return "EntryNormal";
            case EntryConfChange:
                return "EntryConfChange";
            default:
                LOG_FATAL("invalid entry type %d", type);
        }
    }

    bool Message::is_local_msg() const {
        return type == MsgHup || type == MsgBeat || type == MsgUnreachable || type == MsgSnapStatus
               || type == MsgCheckQuorum;
    }
    bool Message::is_response_msg() const {
        return type == MsgAppResp || type == MsgVoteResp || type == MsgHeartbeatResp
               || type == MsgUnreachable || type == MsgPreVoteResp;
    }
} // namespace proto
} // namespace kv