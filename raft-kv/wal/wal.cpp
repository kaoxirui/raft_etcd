#include "log.h"
#include "util.h"
#include "wal.h"
#include <boost/filesystem.hpp>
#include <fcntl.h>
#include <inttypes.h>
#include <sstream>

namespace kv {

static const WAL_type wal_InvalidType = 0;
static const WAL_type wal_EntryType = 1;
static const WAL_type wal_StateType = 2;
static const WAL_type wal_CrcType = 3;
static const WAL_type wal_snapshot_Type = 4;
static const int SegmentSizeBytes = 64 * 1000 * 1000; // 64MB

static std::string wal_name(uint64_t seq, uint64_t index) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".wal", seq, index);
    return buffer;
}

class WAL_File {
public:
    WAL_File(const char *path, int64_t seq) : seq(seq), file_size(0) {
        fp = fopen(path, "a+");
        if (!fp) {
            LOG_FATAL("fopen error %s", strerror(errno));
        }

        file_size = ftell(fp);
        if (file_size == -1) {
            LOG_FATAL("ftell error %s", strerror(errno));
        }

        if (fseek(fp, 0L, SEEK_SET) == -1) {
            LOG_FATAL("fseek error %s", strerror(errno));
        }
    }

    ~WAL_File() { fclose(fp); }
    //用于截断文件至指定的偏移量，并将文件指针重置到该偏移量。
    //主要用于清理文件的无效数据，确保文件只保留有效内容
    void truncate(size_t offset) {
        if (ftruncate(fileno(fp), offset) != 0) {
            LOG_FATAL("ftruncate error %s", strerror(errno));
        }
        //重定位文件指针
        if (fseek(fp, offset, SEEK_SET) == -1) {
            LOG_FATAL("fseek error %s", strerror(errno));
        }
        //更新文件大小和清空缓冲区
        file_size = offset;
        data_buffer.clear();
    }

    void append(WAL_type type, const uint8_t *data, size_t len) {
        //创建并初始化WAL记录
        WAL_Record record;
        record.type = type;
        record.crc = compute_crc32((char *)data, len);
        //设置WAL记录的长度
        set_WAL_Record_len(record, len);
        uint8_t *ptr = (uint8_t *)&record;
        //将WAL_Record结构体的字节数和传入的数据追加到data_buffer中
        data_buffer.insert(data_buffer.end(), ptr, ptr + sizeof(record));
        data_buffer.insert(data_buffer.end(), data, data + len);
    }

    //将内存中的缓冲数据写入到文件中，并确保数据持久化
    void sync() {
        if (data_buffer.empty()) {
            //没有要写入文件，直接返回
            return;
        }
        //将缓冲区的数据写入文件
        size_t bytes = fwrite(data_buffer.data(), 1, data_buffer.size(), fp);
        if (bytes != data_buffer.size()) {
            LOG_FATAL("fwrite error %s", strerror(errno));
        }

        file_size += data_buffer.size();
        data_buffer.clear();
    }

    //从一个文件中读取所有数据并存储在传入的out容器中。它使用缓冲区逐块读取文件中的数据，知道文件尾
    void read_all(std::vector<char> &out) {
        char buffer[1024];
        while (true) {
            //从文件指针fp读取数据到缓冲区buffer中
            size_t bytes = fread(buffer, 1, sizeof(buffer), fp);
            if (bytes <= 0) {
                break;
            }
            //将读取的数据插入到out向量的末尾
            out.insert(out.end(), buffer, buffer + bytes);
        }
        //重定位文件指针到文件末尾
        fseek(fp, 0L, SEEK_END);
    }

    std::vector<uint8_t> data_buffer;
    int64_t seq;
    long file_size;
    FILE *fp;
};

void WAL::create(const std::string &dir) {
    using namespace boost;
    //创建WAL文件的路径和临时文件的路径
    filesystem::path walFile = filesystem::path(dir) / wal_name(0, 0);
    std::string tmpPath = walFile.string() + ".tmp";
    //删除已有的临时文件
    if (filesystem::exists(tmpPath)) {
        filesystem::remove(tmpPath);
    }
    //创建新的临时WAL文件
    {
        std::shared_ptr<WAL_File> wal(new WAL_File(tmpPath.c_str(), 0));
        WAL_Snapshot snap;
        snap.term = 0;
        snap.index = 0;
        msgpack::sbuffer sbuf;
        msgpack::pack(sbuf, snap);
        wal->append(wal_snapshot_Type, (uint8_t *)sbuf.data(), sbuf.size());
        wal->sync();
    }

    filesystem::rename(tmpPath, walFile);
}

WAL_ptr WAL::open(const std::string &dir, const WAL_Snapshot &snap) {
    //创建一个WAL对象
    WAL_ptr w(new WAL(dir));

    //获取WAL文件名列表
    std::vector<std::string> names;
    w->get_wal_names(dir, names);
    if (names.empty()) {
        LOG_FATAL("wal not found");
    }
    //在names列表中查找与给定WAL_Snapshot对应的索引，并获取其索引位置
    uint64_t nameIndex;
    if (!WAL::search_index(names, snap.index, &nameIndex)) {
        LOG_FATAL("wal not found");
    }
    //获取从找到的nameIndex开始的WAL文件名子序列check_names
    std::vector<std::string> check_names(names.begin() + nameIndex, names.end());
    //检查文件名顺序是否有效
    if (!WAL::is_valid_seq(check_names)) {
        LOG_FATAL("invalid wal seq");
    }
    //解析每个文件名以获取其序列号和索引
    for (const std::string &name : check_names) {
        uint64_t seq;
        uint64_t index;
        if (!parse_wal_name(name, &seq, &index)) {
            LOG_FATAL("invalid wal name %s", name.c_str());
        }

        boost::filesystem::path path = boost::filesystem::path(w->dir_) / name;
        std::shared_ptr<WAL_File> file(new WAL_File(path.string().c_str(), seq));
        w->files_.push_back(file);
    }
    //将snap数据复制到w->start_字段中
    memcpy(&w->start_, &snap, sizeof(snap));
    return w;
}

//用于从WAL文件中读取所有记录，并根据记录类型处理数据
Status WAL::read_all(proto::HardState &hs, std::vector<proto::EntryPtr> &ents) {
    //存储读取的数据
    std::vector<char> data;
    //遍历所有WAL文件
    for (auto file : files_) {
        data.clear();
        file->read_all(data);
        //offset跟踪数据的读取位置
        //matchsnap标志用于标记是否找到快照记录
        size_t offset = 0;
        bool matchsnap = false;

        while (offset < data.size()) {
            //确保剩余数据足够长以包含一个WAL_record
            size_t left = data.size() - offset;
            size_t record_begin_offset = offset;

            if (left < sizeof(WAL_Record)) {
                file->truncate(record_begin_offset);
                LOG_WARN("invalid record len %lu", left);
                break;
            }
            //将记录头复制到WAL_Record结构中，并更新偏移量
            WAL_Record record;
            memcpy(&record, data.data() + offset, sizeof(record));

            left -= sizeof(record);
            offset += sizeof(record);
            //记录类型无效，跳出循环
            if (record.type == wal_InvalidType) {
                break;
            }
            //检查记录数据长度
            uint32_t record_data_len = WAL_Record_len(record);
            if (left < record_data_len) {
                file->truncate(record_begin_offset);
                LOG_WARN("invalid record data len %lu, %u", left, record_data_len);
                break;
            }
            //计算记录数据的CRC校验和，并与记录中的CRC进行比较
            char *data_ptr = data.data() + offset;
            uint32_t crc = compute_crc32(data_ptr, record_data_len);

            left -= record_data_len;
            offset += record_data_len;

            if (record.crc != 0 && crc != record.crc) {
                file->truncate(record_begin_offset);
                LOG_WARN("invalid record crc %u, %u", record.crc, crc);
                break;
            }
            //处理记录数据
            //如果记录类型为wal_snapshot_Type，设置matchsnap
            handle_record_wal_record(record.type, data_ptr, record_data_len, matchsnap, hs, ents);

            if (record.type == wal_snapshot_Type) {
                matchsnap = true;
            }
        }

        if (!matchsnap) {
            LOG_FATAL("wal: snapshot not found");
        }
    }

    return Status::ok();
}
/// @brief 处理不同类型的WAL记录，根据记录的类型执行相应的操作，以更新内部的硬状态，日志条目等
/// @param type 记录的类型
/// @param data 记录数据的指针
/// @param data_len 记录数据的长度
/// @param matchsnap 标记是否找到匹配的快照
/// @param hs 硬状态对象的引用
/// @param ents 日志条目指针的向量
void WAL::handle_record_wal_record(WAL_type type, const char *data, size_t data_len,
                                   bool &matchsnap, proto::HardState &hs,
                                   std::vector<proto::EntryPtr> &ents) {

    switch (type) {
        case wal_EntryType: {
            //反序列化为entry
            proto::EntryPtr entry(new proto::Entry());
            msgpack::object_handle oh = msgpack::unpack(data, data_len);
            oh.get().convert(*entry);
            //大于start_的索引
            if (entry->index > start_.index) {
                ents.resize(entry->index - start_.index - 1);
                ents.push_back(entry);
            }

            enti_ = entry->index;
            break;
        }

        case wal_StateType: {
            //反序列化将其转换为hs
            msgpack::object_handle oh = msgpack::unpack(data, data_len);
            oh.get().convert(hs);
            break;
        }

        case wal_snapshot_Type: {
            WAL_Snapshot snap;
            msgpack::object_handle oh = msgpack::unpack(data, data_len);
            oh.get().convert(snap);
            //匹配
            if (snap.index == start_.index) {
                if (snap.term != start_.term) {
                    LOG_FATAL("wal: snapshot mismatch");
                }
                matchsnap = true;
            }
            break;
        }

        case wal_CrcType: {
            LOG_FATAL("wal crc type");
            break;
        }
        //无效记录类型
        default: {
            LOG_FATAL("invalid record type %d", type);
        }
    }
}
//保存raft的硬状态和日志条目到write-ahead log中
Status WAL::save(proto::HardState hs, const std::vector<proto::EntryPtr> &ents) {
    // short cut, do not call sync
    //检查硬状态和日志条目是否为空
    if (hs.is_empty_state() && ents.empty()) {
        //为空，无操作
        return Status::ok();
    }
    //判断是否需要将数据同步到磁盘
    bool mustSync = is_must_sync(hs, state_, ents.size());
    Status status;

    //保存日志条目
    for (const proto::EntryPtr &entry : ents) {
        status = save_entry(*entry);
        if (!status.is_ok()) {
            return status;
        }
    }
    //保存硬状态
    status = save_hard_state(hs);
    if (!status.is_ok()) {
        return status;
    }
    //文件大小小于预定义的SegmentSizeBytes且需要同步
    if (files_.back()->file_size < SegmentSizeBytes) {
        if (mustSync) {
            //将数据同步到磁盘
            files_.back()->sync();
        }
        return Status::ok();
    }
    return cut();
}

Status WAL::cut() {
    files_.back()->sync();
    return Status::ok();
}
//将快照元素保存到WAL
Status WAL::save_snapshot(const WAL_Snapshot &snap) {
    //序列化
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, snap);
    //追加快照元素到WAL文件
    files_.back()->append(wal_snapshot_Type, (uint8_t *)sbuf.data(), sbuf.size());
    if (enti_ < snap.index) {
        enti_ = snap.index;
    }
    files_.back()->sync();
    return Status::ok();
}
//将raft日志条目保存到WAL文件中
Status WAL::save_entry(const proto::Entry &entry) {
    //序列化entry到sbuf中
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, entry);
    //调用files_容器中最后一个文件的append方法，将序列化的日志条目追加到WAL文件中
    files_.back()->append(wal_EntryType, (uint8_t *)sbuf.data(), sbuf.size());
    //记录最后一个日志条目的索引
    enti_ = entry.index;
    return Status::ok();
}

Status WAL::save_hard_state(const proto::HardState &hs) {
    if (hs.is_empty_state()) {
        return Status::ok();
    }
    state_ = hs;

    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, hs);
    files_.back()->append(wal_StateType, (uint8_t *)sbuf.data(), sbuf.size());
    return Status::ok();
}

void WAL::get_wal_names(const std::string &dir, std::vector<std::string> &names) {
    using namespace boost;

    filesystem::directory_iterator end;
    for (boost::filesystem::directory_iterator it(dir); it != end; it++) {
        filesystem::path filename = (*it).path().filename();
        filesystem::path extension = filename.extension();
        if (extension != ".wal") {
            continue;
        }
        names.push_back(filename.string());
    }
    std::sort(names.begin(), names.end(), std::less<std::string>());
}

Status WAL::release_to(uint64_t index) {
    return Status::ok();
}

bool WAL::parse_wal_name(const std::string &name, uint64_t *seq, uint64_t *index) {
    //初始化参数为0
    *seq = 0;
    *index = 0;
    //检查文件拓展名
    boost::filesystem::path path(name);
    if (path.extension() != ".wal") {
        return false;
    }
    //去除文件拓展名
    std::string filename = name.substr(0, name.size() - 4); // trim ".wal"
    //查找分隔符
    size_t pos = filename.find('-');
    if (pos == std::string::npos) {
        return false;
    }
    //解析序列号
    //提取分隔符之前的部分并将其解释为十六进制数，转换为seq
    try {
        {
            std::string str = filename.substr(0, pos);
            std::stringstream ss;
            ss << std::hex << str;
            ss >> *seq;
        }
        //解析索引
        //分隔符之后的部分，并解释为十六进制数，转换为index
        {
            if (pos == filename.size() - 1) {
                return false;
            }
            std::string str = filename.substr(pos + 1, filename.size() - pos - 1);
            std::stringstream ss;
            ss << std::hex << str;
            ss >> *index;
        }
    } catch (...) {
        return false;
    }
    return true;
}
//检查一组WAL文件名的序列是否连续。如果文件名解析成功且序列连续，返回true
bool WAL::is_valid_seq(const std::vector<std::string> &names) {
    uint64_t lastSeq = 0;
    //遍历文件名
    for (const std::string &name : names) {
        uint64_t curSeq;
        uint64_t i;
        //解析
        if (!WAL::parse_wal_name(name, &curSeq, &i)) {
            LOG_FATAL("parse correct name should never fail %s", name.c_str());
        }
        //检查序列连续性
        if (lastSeq != 0 && lastSeq != curSeq - 1) {
            return false;
        }
        lastSeq = curSeq;
    }
    return true;
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
bool WAL::search_index(const std::vector<std::string> &names, uint64_t index,
                       uint64_t *name_index) {

    for (size_t i = names.size() - 1; i >= 0; --i) {
        const std::string &name = names[i];
        uint64_t seq;
        uint64_t curIndex;
        //对每个文件名提取序列号和当前索引
        if (!parse_wal_name(name, &seq, &curIndex)) {
            LOG_FATAL("invalid wal name %s", name.c_str());
        }
        //给定的index大于等于当前文件的索引
        if (index >= curIndex) {
            *name_index = i;
            return true;
        }
        if (i == 0) {
            break;
        }
    }
    //未找到匹配文件名
    *name_index = -1;
    return false;
}

} // namespace kv
