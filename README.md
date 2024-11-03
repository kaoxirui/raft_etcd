2024_10_27

# 完成proto部分

其中定义了日志结构体`Entry`，集群节点配置状态结构体`ConfState`，快照元信息`SnapshotMetadata`，快照结构体`Snapshot`，消息结构体`Message`，要持久化的内容`HardState`，变更结构体`ConfState`

## Entry日志

日志中的信息

* 状态机指令
* leader的任期号
* 日志号（索引）

## HardState持久化状态

每个服务需要持久化当前任期和投票的选择，防止服务器在相同任期内投票两次

```C++
uint64_t term;
uint64_t vote;
uint64_t commit;
```



2024_10_31

# Storage类

MemoryStorage实现storage接口的类，用于在内存中存储raft日志条目

实现方法有

初始化状态，设置硬状态，截取日志，获取任期，压缩日志，添加日志，创建快照，应用快照



成员变量包括硬状态，快照和日志数组

## 日志压缩compact

将`entries_[0]`到conpact_index的日志删除，并更新`entries_[0]`的索引和任期

## 日志添加append

计算偏移量：`entries`中第一个条目索引与`entries_`中第一个条目索引差值计算为偏移量，用于定位新条目在现有条目列表中的位置

* 如果`entries_`的大小大于偏移量，说明现有条目中在offset位置之后的条目与新条目冲突，删除冲突条目，并在末尾插入新条目
  * 在第3.5节，leader处理不一致是通过强制跟随着直接复制自己的日志来解决
* 若`entries_`的大小等于偏移量，则无冲突情况，将新条目直接插入到现有条目的末尾
* 若`entries_`大小小于偏移量，则说明日志缺少某些条目，直接输出错误日志并返回

## 创建快照create_snapshot

https://steinslab.io/archives/2686

将`snapshot_`元数据metadata更新为当前index和term

snapshot=snapshot_
