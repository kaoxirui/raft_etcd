#include "../common/log.h"
#include "../raft/util.h"
#include "snapshotter.h"
#include <boost/filesystem.hpp>
#include <inttypes.h>
#include <msgpack.hpp>

namespace kv {

struct SnapshotRecord {
    //表示data字段中存储的长度，使用32位无符号整数类型
    uint32_t data_len;
    //存储了对data字段中数据计算得到的32为CRC（循环冗余校验）值
    uint32_t crc32;
    //C语言的可变数组成员，用于存储实际的数据
    char data[0];
};

Status Snapshotter::load(proto::Snapshot &snapshot) {
    //获取快照文件名列表
    std::vector<std::string> names;
    get_snap_names(names);
    //遍历快照文件并加载
    for (std::string &filename : names) {
        Status status = load_snap(filename, snapshot);
        if (status.is_ok()) {
            return Status::ok();
        }
    }

    return Status::not_found("snap not found");
}
//生成一个表示快照文件名的字符串。包含任期号和索引，采用十六进制格式
std::string Snapshotter::snap_name(uint64_t term, uint64_t index) {
    char buffer[64];
    snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".snap", term, index);
    return buffer;
}

Status Snapshotter::save_snap(const proto::Snapshot &snapshot) {
    Status status;
    //用于序列化snapshot对象的缓冲区
    msgpack::sbuffer sbuf;
    //将snapshot序列化到sbuf缓冲区
    msgpack::pack(sbuf, snapshot);

    //动态分配内存，用于存储SnapshotRecord结构体和序列化的数据
    SnapshotRecord *record = (SnapshotRecord *)malloc(sbuf.size() + sizeof(SnapshotRecord));
    //设置record的数据长度和CRC32校验码
    record->data_len = sbuf.size();
    record->crc32 = compute_crc32(sbuf.data(), sbuf.size());
    //将序列化的数据复制到record中
    memcpy(record->data, sbuf.data(), sbuf.size());

    //字符数组，用于存储保存路径
    char save_path[128];
    snprintf(save_path, sizeof(save_path), "%s/%s", dir_.c_str(),
             snap_name(snapshot.metadata.term, snapshot.metadata.index).c_str());

    FILE *fp = fopen(save_path, "w");
    //打开失败，释放动态分配的内存
    if (!fp) {
        free(record);
        return Status::io_error(strerror(errno));
    }

    //要写入的字节数
    size_t bytes = sizeof(SnapshotRecord) + record->data_len;
    if (fwrite((void *)record, 1, bytes, fp) != bytes) {
        status = Status::io_error(strerror(errno));
    }
    free(record);
    fclose(fp);

    return status;
}

void Snapshotter::get_snap_names(std::vector<std::string> &names) {
    using namespace boost;
    //目录迭代器end，表示目录迭代的终止条件
    filesystem::directory_iterator end;
    //遍历dir_目录中的每个文件
    for (boost::filesystem::directory_iterator it(dir_); it != end; it++) {
        filesystem::path filename = (*it).path().filename();
        filesystem::path extension = filename.extension();
        //如果扩展名不是.snap，则跳过该文件
        if (extension != ".snap") {
            continue;
        }
        //将符合条件的快照文件名调价到names变量中
        names.push_back(filename.string());
    }
    std::sort(names.begin(), names.end(), std::greater<std::string>());
}
/// @brief 用于从文件加载快照数据并进行校验和反序列化。以下是对代码的详细解释
/// @param filename 要加载的快照文件名
/// @param snapshot 存储加载的快照数据
/// @return
Status Snapshotter::load_snap(const std::string &filename, proto::Snapshot &snapshot) {
    using namespace boost;
    //SnapshotRecord结构体用于存储快照头信息
    SnapshotRecord snap_hdr;
    //存储快照数据
    std::vector<char> data;
    //构建文件路径
    filesystem::path path = filesystem::path(dir_) / filename;
    //打开文件，获取文件指针fp
    FILE *fp = fopen(path.c_str(), "r");

    if (!fp) {
        //文件打开失败
        goto invalid_snap;
    }
    //从文件中读取快照信息到snap_hdr
    //读取失败
    //size_t fread(void *ptr, size_t size, size_t count, FILE *stream);
    //指向存储数据的缓冲区的指针，每个元素的大小，要读取的元素个数，指向文件的指针
    if (fread(&snap_hdr, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord)) {
        goto invalid_snap;
    }
    //快照头信息无效（数据长度或CRC32校验码为0）
    if (snap_hdr.data_len == 0 || snap_hdr.crc32 == 0) {
        goto invalid_snap;
    }
    //调整data大小以适应快照数据
    data.resize(snap_hdr.data_len);
    //读取快照数据到data
    if (fread(data.data(), 1, snap_hdr.data_len, fp) != snap_hdr.data_len) {
        goto invalid_snap;
    }
    //关闭文件
    fclose(fp);
    fp = NULL;
    //计算校验位并与快照信息中的CRC32校验码进行比较
    if (compute_crc32(data.data(), data.size()) != snap_hdr.crc32) {
        goto invalid_snap;
    }

    try {
        //msgpack解包数据并将其转换为snapshot对象
        msgpack::object_handle oh = msgpack::unpack((const char *)data.data(), data.size());
        oh.get().convert(snapshot);
        return Status::ok();

    } catch (std::exception &e) {
        goto invalid_snap;
    }

invalid_snap:
    if (fp) {
        //非空关闭文件
        fclose(fp);
    }
    //记录日志，指示快照文件损坏
    LOG_INFO("broken snapshot %s", path.string().c_str());
    filesystem::rename(path, path.string() + ".broken");
    return Status::io_error("unexpected empty snapshot");
}

} // namespace kv
