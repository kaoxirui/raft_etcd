cmake_minimum_required(VERSION 3.16)

if(COMMAND cmake_policy)
cmake_policy(SET CMP0003 NEW)
endif(COMMAND cmake_policy)

set(SRC
    common/status.cpp
    raft/config.h
)
add_library(raft-kv++ ${SRC})
target_link_libraries(raft-kv++)