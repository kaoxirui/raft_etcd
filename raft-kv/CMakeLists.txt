cmake_minimum_required(VERSION 3.16)

if(COMMAND cmake_policy)
cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

# 设置调试信息
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -g")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(SRC
common/log.h
common/slice.h
common/status.cpp
common/bytebuffer.cpp
common/random_device.cpp
raft/proto.cpp
raft/config.cpp
raft/raft.cpp
raft/storage.cpp
raft/node.cpp
raft/raft_log.cpp
raft/unstable.cpp
raft/progress.cpp
raft/readonly.cpp
raft/raft_status.cpp
raft/ready.cpp
raft/util.cpp
server/raft_node.cpp
server/redis_session.cpp
server/redis_store.cpp
snap/snapshotter.cpp
transport/proto.h
transport/transport.h
transport/transport.cpp
transport/peer.h
transport/raft_server.cpp
transport/peer.cpp
wal/wal.cpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB REQUIRED glib-2.0)
include_directories(${GLIB_INCLUDE_DIRS})

find_package(Threads REQUIRED)

find_package(Boost REQUIRED COMPONENTS filesystem)
include_directories(${Boost_INCLUDE_DIRS})

add_library(raft-kv++ ${SRC})
target_link_libraries(raft-kv++ ${LIBS} ${GLIB_LIBRARIES} Threads::Threads ${Boost_LIBRARIES})

add_executable(raft-kv raft-kv.cpp)
target_link_libraries(raft-kv ${LIBS} raft-kv++)