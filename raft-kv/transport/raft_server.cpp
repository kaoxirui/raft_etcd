#include "../common/log.h"
#include "proto.h"
#include "raft_server.h"
#include "transport.h"
#include <boost/algorithm/string.hpp>
#include <boost/asio.hpp>

namespace kv {
class AsioServer;
class ServerSession : public std::enable_shared_from_this<ServerSession> {
public:
    //构造函数初始化套接字和服务器指针
    explicit ServerSession(boost::asio::io_service &io_service, AsioServer *server)
        : socket(io_service), server_(server) {}

    void start_read_meta() {
        assert(sizeof(meta_) == 5);
        meta_.type = 0;
        meta_.len = 0;
        //调用shared_from_this()，可以在类的成员函数中安全地创建指向自身的共享指针
        auto self = shared_from_this();
        auto buffer = boost::asio::buffer(&meta_, sizeof(meta_));
        auto handler = [self](const boost::system::error_code &error, std::size_t bytes) {
            if (bytes == 0) {
                return;
            }
            if (error) {
                LOG_DEBUG("read error %s", error.message().c_str());
                return;
            }
            //读取的字节数不等于元数据的大小，记录错误并返回
            if (bytes != sizeof(meta_)) {
                LOG_DEBUG("invalid data len %lu", bytes);
                return;
            }
            self->start_read_message();
        };
        //从socket读取sizeof(meta_)字节数据到buffer并在完成时调用handler处理
        boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(sizeof(meta_)),
                                handler);
    }
    void start_read_message() {
        uint32_t len = ntohl(meta_.len);
        if (buffer_.capacity() < len) {
            buffer_.resize(len);
        }
        auto self = shared_from_this();
        //将buffer_的地址和长度封装成一个Boost.Asio缓冲区对象
        auto buffer = boost::asio::buffer(buffer_.data(), len);
        auto handler = [self, len](const boost::system::error_code &error, std::size_t bytes) {
            assert(len == ntohl(self->meta_.len));
            //读取错误或读取字节数为0
            if (error || bytes == 0) {
                LOG_DEBUG("read error %s", error.message().c_str());
                return;
            }
            //读取的字节数不等于消息长度
            if (bytes != len) {
                LOG_DEBUG("invalid data len %lu, %u", bytes, len);
                return;
            }
            self->decode_message(len);
        };
        boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(len), handler);
    }
    void decode_message(uint32_t len) {
        switch (meta_.type) {
            case TransportTypeDebug: {
                assert(len == sizeof(DebugMessage));
                DebugMessage *dbg = (DebugMessage *)buffer_.data();
                assert(dbg->a + 1 == dbg->b);
                break;
            }
            case TransportTypeStream: {
                proto::MessagePtr msg(new proto::Message());
                try {
                    //msgpack::unpack将缓冲区的二进制数据解码为msgpack::object_handle对象
                    //并转换为proto::Message类型
                    msgpack::object_handle oh = msgpack::unpack((const char *)buffer_.size(), len);
                    oh.get().convert(*msg);
                } catch (std::exception &e) {
                    LOG_ERROR("bad message %s, size = %lu, type %s", e.what(), buffer_.size(),
                              proto::msg_type_to_string(msg->type));
                    return;
                }
                on_receive_stream_message(std::move(msg));
                break;
            }
            default: {
                LOG_DEBUG("unknown msg type %d, len = %d", meta_.type, ntohl(meta_.len));
                return;
            }
                start_read_meta();
        }
    }
    void on_receive_stream_message(proto::MessagePtr);
    boost::asio::ip::tcp::socket socket;

private:
    AsioServer *server_;          //存储服务器的指针，便于会话与服务器交互
    TransportMeta meta_;          //存储消息的元数据
    std::vector<uint8_t> buffer_; //一个字节缓冲区，用于存储从客户端读取的数据
};
typedef std::shared_ptr<ServerSession> ServerSessionPtr;

class AsioServer : public IoServer {
public:
    explicit AsioServer(boost::asio::io_service &io_service, const std::string &host,
                        RaftServer *raft)
        : io_service_(io_service), acceptor_(io_service), raft_(raft) {
        std::vector<std::string> strs;
        //使用boost::split函数将host字符串按“：”分割成IP地址和端口号
        //如果分割后得到的字符串数量不是2，则认为host无效
        boost::split(strs, host, boost::is_any_of(":"));
        if (strs.size() != 2) {
            LOG_DEBUG("invalid host %s", host.c_str());
            exit(0);
        }
        //将IP地址字符串转换为boost::asio::ip::address对象
        auto address = boost::asio::ip::address::from_string(strs[0]);
        //将端口号转换为整数
        int port = std::atoi(strs[1].c_str());
        //表示一个TCP连接的端点
        auto endpoint = boost::asio::ip::tcp::endpoint(address, port);
        //打开接收器并指定协议（TCP）
        acceptor_.open(endpoint.protocol());
        //设置接收器选项，允许地址重用。对于快速重启服务器非常有用
        acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
        //将接收器绑定到指定的端口
        acceptor_.bind(endpoint);
        //使接收器进入监听模式，准备接受传入的连接
        acceptor_.listen();
        LOG_DEBUG("listen at %s:%d", address.to_string().c_str(), port);
    }
    ~AsioServer() {}
    void start() final {
        ServerSessionPtr session(new ServerSession(io_service_, this));
        acceptor_.async_accept(session->socket,
                               [this, session](const boost::system::error_code &error) {
                                   if (error) {
                                       LOG_DEBUG("accept error %s", error.message().c_str());
                                       return;
                                   }
                                   this->start();
                                   session->start_read_meta();
                               });
    }
    void stop() final {}
    void on_message(proto::MessagePtr msg) {
        raft_->process(std::move(msg), [](const Status &status) {
            if (!status.is_ok()) {
                LOG_ERROR("process error %s", status.to_string().c_str());
            }
        });
    }

private:
    boost::asio::io_service &io_service_;
    boost::asio::ip::tcp::acceptor acceptor_;
    RaftServer *raft_;
};

void ServerSession::on_receive_stream_message(proto::MessagePtr msg) {
    server_->on_message(std::move(msg));
}

//该函数首先创建一个 AsioServer 的实例，然后将其转换为 IoServer 类型的共享指针并返回
std::shared_ptr<IoServer> IoServer::create(void *io_service, const std::string &host,
                                           RaftServer *raft) {
    std::shared_ptr<AsioServer> server(
        new AsioServer(*(boost::asio::io_service *)io_service, host, raft));
    return server;
}
} // namespace kv
