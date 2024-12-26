
#include "/home/kao/cpp/raft-cpp/raft-kv/common/bytebuffer.h"
#include <string.h>

namespace kv {
static uint32_t MIN_BUFFERING = 4096;

ByteBuffer::ByteBuffer() : reader_(0), writer_(0), buff_(MIN_BUFFERING) {
}

//将数据放入缓冲区
void ByteBuffer::put(const uint8_t *data, uint32_t len) {
    //buff.size()返回缓冲区当前的总大小
    //writer_当前写指针的位置，表示已经写入的数据量
    //相减得到缓冲区剩余的可用空间
    uint32_t left = static_cast<uint32_t>(buff_.size()) - writer_;
    if (left < len) {
        buff_.resize(buff_.size() * 2 + len, 0);
    }
    //buff_.data() + writer_写入数据的起始位置
    //data要写入数据源指针
    memcpy(buff_.data() + writer_, data, len);
    writer_ += len;
}

uint32_t ByteBuffer::readable_bytes() const {
    //写指针不小于读指针位置
    assert(writer_ >= reader_);
    //返回可读数据的字节数
    return writer_ - reader_;
}

void ByteBuffer::read_bytes(uint32_t bytes) {
    //确保读取的字节数不超过缓冲区中可读的数据量
    assert(readable_bytes() >= bytes);
    //移动读指针，表示已读bytes个字节
    reader_ += bytes;
    //检查是否需要调整缓冲区
    may_shrink_to_fit();
}

void ByteBuffer::may_shrink_to_fit() {
    if (reader_ == writer_) {
        //缓冲区被完全读取，将缓冲区置为空
        reader_ = 0;
        writer_ = 0;
    }
}

void ByteBuffer::reset() {
    reader_ = writer_ = 0;
    //缓冲区大小调整为最小缓冲区大小
    buff_.resize(MIN_BUFFERING);
    //释放多余的内存，使缓冲区容量与实际使用大小一致
    buff_.shrink_to_fit();
}

} // namespace kv
