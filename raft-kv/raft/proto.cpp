#include "../common/log.h"
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
    //1ULL:int 1 with type unsigned long long
    //<< is bitshift operator. 1ULL<<i is equal to 2^i,or in binary:1000..00 with i zeros
    //左移理解：二进制左移几位就是后面加几个0，前面去掉几位
    //右移：在二进制前面加几位，正加0，负加1，后面去掉几位
    //左移一位相当于乘以2，右移一位相当于除2（不完全等同）
    static uint32_t u64_serialize_size(uint64_t d) {
        if (d < (1ULL << 8)) {
            if (d < (1ULL << 7)) {
                return 1;
            } else {
                return 2;
            }
        } else {
            if (d < (1ULL << 16)) {
                return 3;
            } else if (d < (1ULL << 32)) {
                return 5;
            } else {
                return 9;
            }
        }
    }
    //计算数据长度为len的序列化大小
    static uint32_t data_serialize_size(uint32_t len) {
        if (len <= std::numeric_limits<uint8_t>::max()) {
            //2^8-1
            //2表示与序列后所需的额外的字节，其中一个存储len大小类型标志，另一个存储实际长度
            return 2 + len;
        }
        if (len <= std::numeric_limits<uint16_t>::max()) {
            return 3 + len;
        }
        if (len <= std::numeric_limits<uint32_t>::max()) {
            return 5 + len;
        }
        assert(false);
    }
    uint32_t Entry::serialize_size() const {
        return 1 + u8_serialize_size(type) + u64_serialize_size(term) + u64_serialize_size(index)
               + data_serialize_size(static_cast<uint32_t>(data.size()));
    }

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

            case MsgTransferLeader:
                return "MsgTransferLeader";

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

    /*
    从data解析出一个ConfChange对象
    通过 oh.get() 获取解包的 msgpack::object，然后使用 convert 方法将其内容转换并赋值给传入的 ConfChange &cc。
    msgpack 会自动匹配并填充 cc 的字段（前提是 ConfChange 定义了 MSGPACK_DEFINE 宏）。
    */
    void ConfChange::from_data(const std::vector<uint8_t> &data, ConfChange &cc) {
        msgpack::object_handle oh = msgpack::unpack((const char *)data.data(), data.size());
        oh.get().convert(cc);
    }
    std::vector<uint8_t> ConfChange::serialize() const {
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, *this);
        return std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size());
    }
    bool Snapshot::equal(const Snapshot &snap) const {
        //如果operator==没有声明为const，不能比较const对象
        return data == snap.data && metadata == snap.metadata;
    }
} // namespace proto
} // namespace kv