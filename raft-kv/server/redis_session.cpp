#include "../common/log.h"
#include "redis_session.h"
#include "redis_store.h"
#include <glib.h>
#include <unordered_map>

namespace kv {
#define RECEIVE_BUFFER_SIZE(1024 * 512) ;
namespace shared {
    static const char *ok = "+OK\r\n";
    static const char *err = "-ERR %s\r\n";
    static const char *wrong_type =
        "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
    static const char *unknown_command = "-ERR unknown command `%s`\r\n";
    static const char *wrong_number_arguments =
        "-ERR wrong number of arguments for '%s' command\r\n";
    static const char *pong = "+PONG\r\n";
    static const char *null = "$-1\r\n";

    typedef std::function<void(RedisSessionPtr, struct redisReply *reply)> CommandCallback;
    static std::unordered_map<std::string, CommandCallback> command_table = {
        {"ping", RedisSession::ping_command}, {"PING", RedisSession::ping_command},
        {"get", RedisSession::get_command},   {"Get", RedisSession::get_command},
        {"set", RedisSession::set_command},   {"SET", RedisSession::set_command},
        {"del", RedisSession::del_command},   {"DEL", RedisSession::del_command},
        {"keys", RedisSession::keys_command}, {"KEYS", RedisSession::keys_command},
    };
} // namespace shared

/// @brief 构造redis协议格式的多字符串组回复
/// @param strs 包含需要构造为redis格式回复的字符串列表
/// @param reply 保存构造后的redis协议格式字符串
/*
redis协议格式
数组的起始部分以 * 开头，后接数组的元素个数和回车换行符（\r\n）。
每个字符串的起始部分以 $ 开头，后接字符串的长度和回车换行符。
字符串的内容紧随其后，最后加上回车换行符。
*/
//*2\r\n$4\r\nkey1\r\n$4key2\r\n

static void build_redis_string_array_reply(const std::vector<std::string> &strs,
                                           std::string &reply) {
    char buffer[64];
    //将数组的大小格式化为redis协议的头部，如strs有3个字符串，则头部为 *3\r\n
    snprintf(buffer, sizeof(buffer), "*%lu\r\n", strs.size());
    //snprintf主要用于格式化字符串并将结果存储到字符数组中
    //与sprintf类似，但前者限制了可以写入的最大字符数
    reply.append(buffer);
    /*
    snprintf的示例
    int main() {
    char buffer[50];
    int value = 42;
    snprintf(buffer, sizeof(buffer), "Value: %d", value);
    printf("%s\n", buffer);  // 输出：Value: 42
    return 0;
}
    */
    for (const std::string &str : strs) {
        //为每个字符串构造长度头部，如hello的头部为$5\r\n
        snprintf(buffer, sizeof(buffer), "$%lu\r\n", str.size());
        reply.append(buffer);

        if (!str.empty()) {
            reply.append(str);
            reply.append("\r\n");
        }
    }
}

/*
    quit_:标志会话是否已经结束
    server_:指向redis服务器的指针，用于调用服务器功能
    socket_:用于异步通信
    reader_:使用hiredis库的redis协议解析器，用于解析收到的数据
*/
RedisSession::RedisSession(RedisStore *server, boost::asio::io_service &io_service)
    : quit_(false), server_(server), socket_(io_service), read_buffer_(RECEIVE_BUFFER_SIZE),
      reader_(redisReaderCreate()) {
}

void RedisSession::start() {
    if (quit_) {
        return;
    }
    auto self = shared_from_this();
    //通过boost:asio::buffer将缓冲区read_buffer_绑定为数据写入目标
    auto buffer = boost::asio::buffer(read_buffer_.data(), read_buffer_.size());
    auto handler = [self](const boost::system::error_code &error, size_t bytes) {
        if (bytes == 0) {
            return;
        }
        if (error) {
            LOG_DEBUG("read error %s", error.message().c_str());
            return;
        }
        self->handle_read(bytes);
    };
    //异步读取数据到缓冲区，读取完成后执行handler
    socket_.async_read_some(buffer, std::move(handler));
}

/// @brief 从接收到的数据中解析redis协议消息，调用on_redis_reply处理解析得到的每个Redis消息
/// @param bytes
void RedisSession::handle_read(size_t bytes) {
    uint8_t *start = read_buffer_.data();
    uint8_t *end = read_buffer_.data() + bytes;
    int error = REDIS_OK;
    std::vector<struct redisReply *> replies;

    while (!quit_ && start < end) {
        //memchr查找缓冲区中的换行符，将消息按行拆分
        uint8_t *p = (uint8_t *)memchr(start, '\n', bytes);
        //未找到完整的消息，调用start继续异步读取数据
        if (!p) {
            this->start();
            break;
        }

        size_t n = p + 1 - start;
        //将消息数据输入redis解析器
        err = redisReaderFeed(reader_, (const char *)start, n);
        if (err != REDIS_OK) {
            LOG_DEBUG("redis protocal error %d,%s", err, reader_->errstr);
            quit_ = true;
            break;
        }
        struct redisReply *reply = NULL;
        //获取解析后的redis回复对象
        err = redisReaderGetReply(reader_, (void **)&reply);
        if (err != REDIS_OK) {
            LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
            quit_ = true;
            break;
        }
        if (reply) {
            replies.push_back(reply);
        }

        start += n;
        bytes -= n;
    }
    if (err == REDIS_OK) {
        //对每个解析结果执行具体的业务逻辑
        for (struct redisReply *reply : replies) {
            on_redis_reply(reply);
        }
        this->start();
    }

    for (struct redisReply *reply : replies) {
        freeReplyObject(reply);
    }
}
void RedisSession::on_redis_reply(struct redisReply *reply) {
    char buffer[256];
    //确保redisReply是redis协议中的数组类型
    if (reply->type != REDIS_REPLY_ARRAY) {
        LOG_WARN("wrong type %d", reply->type);
        send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    if (reply->elements < 1) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "");
        send_reply(buffer, n);
        return;
    }
    //确保数组的第一个元素是字符串类型（通常为redis命令名）
    if (reply->element[0]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[0]->type);
        send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    //提取数组的第一个元素作为命令名在command_table中查找对应的回调函数
    std::string command(reply->element[0]->str, reply->element[0]->len);
    auto it = shared::command_table.find(command);
    if (it == shared::command_table.end()) {
        int n = snprintf(buffer, sizeof(buffer), shared::unknown_command, command.c_str());
        send_reply(buffer, n);
        return;
    }
    shared::CommandCallback &cb = it->second;
    cb(shared_from_this(), reply);
}

void RedisSession::send_reply(const char *data, uint32_t len) {
    uint32_t bytes = send_buffer_.readable_bytes();
    send_buffer_.put((uint8_t *)data, len);
    if (bytes == 0) {
        start_send();
    }
}

void RedisSession::start_send() {
    if (!send_buffer_.readable()) {
        return;
    }
    auto self = shared_from_this();
    //获取发送数据
    uint32_t remaining = send_buffer_.readable_bytes();
    auto buffer = boost::asio::buffer(send_buffer_.reader().remaining);
    //异步发送数据
    auto handler = [self](const boost::system::error_code &error, std::size_t bytes) {
        if (bytes == 0) {
            return;
        }
        if (error) {
            LOG_DEBUG("send error %s", error.message().c_str());
            return;
        }
        std::string str((const char *)self->send_buffer_.reader(), bytes);
        //移动缓冲区读取位置
        self->send_buffer_.read_bytes(bytes);
        self->start_send();
    };
    // 异步写入数据到套接字，并在完成时调用回调函数。
    boost::asio::async_write(socket_, buffer, std::move(handler));
}

void RedisSession::ping_command(std::shared_ptr<RedisSession> self, struct redisReply *reply) {
    self->send_reply(shared::pong, strlen(shared::pong));
}

//处理客户端发送的redis GET命令
void RedisSession::get_command(std::shared_ptr<RedisSession> self, struct redisReply *reply) {
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements != 2) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "get");
        self->send_reply(buffer, n);
        return;
    }

    if (reply->element[1]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[1]->type);
        self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    //查找键对应的值
    std::string value;
    std::string key(reply->element[1]->str, reply->element[1]->len);
    bool get = self->server_->get(key, value);
    //键不存在
    if (!get) {
        self->send_reply(shared::null, strlen(shared::null));
    } else {
        //键存抓，使用redis协议的批量字符串格式返回值
        //$<字符串长度>\r\n<字符串值>\r\n  $6\r\nfoobar\r\n
        char *str = g_strdup_printf("$%lu\r\n%s\r\n", value.size(), value.c_str());
        self->send_reply(str, strlen(str));
        g_free(str);
    }
}

void RedisSession::set_command(std::shared_ptr<RedisSession> self, struct redisReply *reply) {
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements != 3) {
        LOG_WARN("wrong elements %lu", reply->elements);
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "set");
        self->send_reply(buffer, n);
        return;
    }

    if (reply->element[1]->type != REDIS_REPLY_STRING
        || reply->element[2]->type != REDIS_REPLY_STRING) {
        LOG_WARN("wrong type %d", reply->element[1]->type);
        self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }
    std::string key(reply->element[1]->str, reply->element[1]->len);
    std::string value(reply->element[2]->str, reply->element[2]->len);
    self->server_->set(std::move(key), std::move(value), [self](const Status &status) {
        if (status.is_ok()) {
            self->send_reply(shared::ok, strlen(shared::ok));
        } else {
            char buff[256];
            int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
            self->send_reply(buff, n);
        }
    });
}

void RedisSession::del_command(std::shared_ptr<RedisSession> self, struct redisReply *reply) {
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements <= 1) {
        ;
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "del");
        self->send_reply(buffer, n);
        return;
    }

    std::vector<std::string> keys;
    for (size_t i = 1; i < reply->elements; ++i) {
        redisReply *element = reply->element[i];
        if (element->type != REDIS_REPLY_STRING) {
            self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
            return;
        }

        keys.emplace_back(element->str, element->len);
    }

    self->server_->del(std::move(keys), [self](const Status &status) {
        if (status.is_ok()) {
            self->send_reply(shared::ok, strlen(shared::ok));
        } else {
            char buff[256];
            int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
            self->send_reply(buff, n);
        }
    });
}

//匹配所有符合指定模式的键
void RedisSession::keys_command(std::shared_ptr<RedisSession> self, struct redisReply *reply) {
    assert(reply->type = REDIS_REPLY_ARRAY);
    assert(reply->elements > 0);
    char buffer[256];

    if (reply->elements != 2) {
        ;
        int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "keys");
        self->send_reply(buffer, n);
        return;
    }

    redisReply *element = reply->element[1];

    if (element->type != REDIS_REPLY_STRING) {
        self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
        return;
    }

    //构造redis响应
    std::vector<std::string> keys;
    self->server_->keys(element->str, element->len, keys);
    std::string str;
    build_redis_string_array_reply(keys, str);
    self->send_reply(str.data(), str.size());
}

} // namespace kv
  // namespace kv
