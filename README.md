https://blog.csdn.net/cyq6239075/article/details/105326576

节点之间传递的是消息（Message），每条消息中可以携带多条Entry记录，每条记录对应一个独立的操作

Message是所有消息的抽象，包括了各种类型消息所需要的字段

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

MemoryStorage实现storage接口的类，用于在内存中存储raft日志条目。MemoryStorage在内存中维护上述状态信息（hardstate）、快照数据（snapshot）及所有Entry记录

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

如果传入的index小于等于已有快照的索引，返回一个空快照

计算offset为已有快照的第一个日志的索引，将快照成员变量的index设置为传入index，任期为日志对应索引的term，最后将confstate和data都传给新的快照变量

## 应用快照apply_snapshot

`    entries_.resize(1); //!这里应该就是日志缩减了`

并把`entries_`相关参数设置为快照内容

并将传入的snapshot设置为MemoryStorage中`snapshot_`

如果传入的快照的inedx小于已有快照的index，则说明传入的是旧快照了



# unstable类

unstable使用内存数组维护其中所有的Entry记录，对于leader而言，它维护了客户端请求对应的Entry记录；对于follower节点而言，维护的是从leader节点复制来的记录。无论是leader节点还是follower节点，对于刚刚接收到的Entry记录首先都会被存储在unstable中。然后按照Raft协议将unstable中缓存的这些Entry记录交给上层模块进行处理，上层模块会将这些Entry记录发送到集群其他节点或进行保存（写入storage类）。之后，上层模块会调用Advance（）方法通知底层的raft模块将unstable  中对应的Entry记录删除（因为己经保存到了Storage中）。正因为unstable中保存的Entry记录并未进行持久化，可能会因节点故障而意外丢失，所以被称为unstable。



在unstable 中提供了很多与Storage类似的方法，在raftLog中，很多方法都是先尝试调用unstable的相应方法，在其失败后（unstable的方法返回（0, false）即表示失败），再尝试调用Storage的对应方法。



unstable.maybeFirstlndex（）方法会尝试获取unstable 的第一条Entry 记录的索引值，unstable.maybeLastlndex（）方法会尝试获取unstable 的最后一条Entry记录的索引值，如果获取失败则返回（0, false），unstable.maybeTerm（）方法的主要功能是尝试获取指定Entry记录的Term值，根据条件查找指定的Entry记录的位置。

当unstable.entries 中的Entry记录己经被写入Storage之后，会调用unstable.stableTo（）方法清除entries 中对应的Entry记录，stableTo（）方法的具体实现如下：

entries：用于保存未写入Storage中的Entry记录

offset：entries中的第一条Entry记录的索引值

snapshot：快照数据，该快照数据也是未写入storage中的

```C++
//从unstable类中获取可能的第一个
void Unstable::maybe_first_index(uint64_t &index, bool &ok) {
    if (snapshot_) {
        ok = true;
        index = snapshot_->metadata.index + 1;
    } else {
        ok = false;
        index = 0;
    }
}
```

snapshot包含了已经持久化的日志状态到某个索引index的数据，在此快照之前的日志条目都可以丢弃。

如果entries为空，返回的仍然是`snapshot_->metadata.index + 1`。是不是如果entries数组发生变动，日志系统还是可以通过快照索引准确获取日志起始位置，从而避免依赖entries的状态
