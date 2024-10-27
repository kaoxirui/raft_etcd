#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <iostream>
#include "status.h"

namespace kv {
Status::Status(const Status &s) {
    status_ = copy(s);
}
Status::~Status() {
}

Status &Status::operator=(const Status &s) {
    if (this != &s) {
        status_ = copy(s);
    }
    return *this;
}
/*
this是一个指向当前对象的指针，类型为classname*，表示当前对象在内存中的地址
*this表示当前对象本身，但是是以引用的形式返回
支持链式操作
*/

std::unique_ptr<char[]> Status::copy(const Status &s) {
    if (s.status_ == nullptr) {
        return nullptr;
    } else {
        uint32_t len;
        memcpy(&len, s.status_.get(), sizeof(uint32_t));
        std::unique_ptr<char[]> status(new char[len + 5]);
        memcpy(status.get(), s.status_.get(), len + 5);
        return status;
    }
}

std::string Status::to_string() const {
    if (is_ok())
        return "ok";

    const char *str;
    char tmp[30];
    Code c = code();
    switch (c) {
        case Code::OK:
            str = "ok";
            break;
        case Code::NotFound:
            str = "not found";
            break;
        case Code::NotSupported:
            str = "not supported";
            break;
        case Code::InvalidArgument:
            str = "invalid argument";
            break;
        case Code::IOError:
            str = "io error";
            break;
        default:
            snprintf(tmp, sizeof(tmp), "Unknown code(%d):", c);
            str = tmp;
    }
    std::string ret(str);
    uint32_t length;
    //length从status_.get()中提取前四个字节的消息长度
    memcpy(&length, status_.get(), sizeof(length));
    if (length > 0) {
        ret.append(status_.get() + 5, length);
    } else {
        ret.pop_back();
    }
    return ret;
}

//根据code和msg初始化一个状态对象
Status::Status(Code code, const char *msg) {
    uint32_t len;
    if (msg == nullptr) {
        len = 0;
    } else {
        len = strlen(msg);
    }
    status_ = std::make_unique<char[]>(len + 5);
    //前四个直接存储消息的长度
    memcpy(status_.get(), &len, sizeof(uint32_t));
    //第五个字节存储状态码
    status_[4] = code;
    //第六个字节存储消息内容
    memcpy(status_.get() + 5, msg, len);
}
}; // namespace kv

/*
void *memcpy(void *dest, const void *src, size_t n);
dest：指向目标内存块的指针，数据将被复制到这里
src：指向源内存块的指针，数据从这里复制
n：要复制的字节数
*/