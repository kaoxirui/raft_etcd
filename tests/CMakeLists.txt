cmake_minimum_required(VERSION 3.16)
cmake_policy(SET CMP0054 NEW)

set(LIBS
    ${LIBS}
    raft-kv++
)

enable_testing()
find_package(GTest REQUIRED)

add_executable(test_proto test_proto.cpp)
target_link_libraries(test_proto ${LIBS})
gtest_add_tests(TARGET test_proto)

add_executable(test_storage test_storage.cpp)
target_link_libraries(test_storage ${LIBS})
gtest_add_tests(TARGET test_storage)