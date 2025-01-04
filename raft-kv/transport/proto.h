#pragma once
#include <stdint.h>

namespace kv {
const uint8_t TransportTypeStream = 1;
const uint8_t TransportTypePipeline = 3;
const uint8_t TransportTypeDebug = 5;

//#pragma pack(1)确保结构体被紧密打包，没有字段之间的填充
#pragma pack(1)
struct TransportMeta {
    uint8_t type;
    uint32_t len;
    uint8_t data[0]; //可变数组长度
};
#pragma pack()
//恢复默认打包方式
#pragma pack(1)
struct DebugMessage {
    uint32_t a;
    uint32_t b;
};
#pragma pack()
} // namespace kv
