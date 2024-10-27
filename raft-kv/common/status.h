#pragma once
#include <string>
#include <memory>

namespace kv {
class Status {
public:
    Status(/* args */) : status_(nullptr) {}
    Status(const Status &s);
    Status &operator=(const Status &s);
    ~Status();
    static Status ok() { return Status(); }
    static Status not_found(const char *msg) { return Status(NotFound, msg); }
    static Status not_supported(const char *msg) { return Status(NotSupported, msg); }
    static Status invalid_argument(const char *msg) { return Status(InvalidArgument, msg); }
    static Status io_error(const char *msg) { return Status(IOError, msg); }
    bool is_ok() const { return status_ == nullptr; }
    bool is_not_found() const { return code() == Code::NotFound; }
    bool is_io_error() const { return code() == Code::IOError; }
    bool is_invalid_argument() const { return code() == Code::InvalidArgument; }
    bool is_not_supported() const { return code() == Code::NotSupported; }
    std::string to_string() const;

private:
    enum Code {
        OK = 0,
        NotFound = 1,
        NotSupported = 2,
        InvalidArgument = 3,
        IOError = 4,
    };
    inline static std::unique_ptr<char[]> copy(const Status &s);
    Status(Code code, const char *msg);
    Code code() const { return status_ == nullptr ? Code::OK : static_cast<Code>(status_[4]); }

private:
    std::unique_ptr<char[]> status_;
};

} // namespace kv