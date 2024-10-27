cmake_minimum_required(VERSION 3.16)

if(COMMAND cmake_policy)
cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(SRC
    common/status.cpp
    raft/config.h
    raft/proto.cpp
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(GLIB REQUIRED glib-2.0)
include_directories(${GLIB_INCLUDE_DIRS})

find_package(Threads REQUIRED)

find_package(Boost REQUIRED COMPONENTS filesystem)
include_directories(${Boost_INCLUDE_DIRS})

add_library(raft-kv++ ${SRC})
target_link_libraries(raft-kv++ ${LIBS} ${GLIB_LIBRARIES} Threads::Threads ${Boost_LIBRARIES})