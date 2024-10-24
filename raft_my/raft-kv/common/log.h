#pragma once
#include <string.h>
#include <stdio.h>
#include <string>
#include <exception>
#include <stdexcept>

#ifdef DEBUG
#    define LOG_DEBUG(format, ...)                                                                 \
        {                                                                                          \
            fprintf(stderr, "DEBUG [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__,    \
                    ##__VA_ARGS__);                                                                \
        }
#else
#    define LOG_DEBUG(format, ...)
#endif

/*
__FILE__预定义的宏，当前源文件的完整路径
strrchr(__FILE__, '/') + 1：这是为了从文件路径中提取文件名。strrchr 函数在字符串中查找最后一个 / 字符，并返回其位置。通过加 1，我们得到了文件名部分。
__LINE__表示当前行号
__VA_ARGS__可变参数部分，如果有%s，%d类似的占位符，用__VA_ARGS__传入的值进行替换
使用 do { ... } while (0) 的结构包裹宏的主体。这种设计可以确保宏在使用时总是作为一个单独的语句运行，即使它被放置在条件语句等复杂结构中，也不会产生语法问题。
*/

#define LOG_INFO(format, ...)                                                                      \
    do {                                                                                           \
        fprintf(stderr, "INFO [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__,         \
                ##__VA_ARGS__);                                                                    \
    } while (0)
#define LOG_WARN(format, ...)                                                                      \
    do {                                                                                           \
        fprintf(stderr, "WARN [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__,         \
                ##__VA_ARGS__);                                                                    \
    } while (0)
#define LOG_ERROR(format, ...)                                                                     \
    do {                                                                                           \
        fprintf(stderr, "ERROR [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, __LINE__,        \
                ##__VA_ARGS__);                                                                    \
    } while (0)
#define LOG_FATAL(format, ...)                                                                     \
    do {                                                                                           \
        char buffer[1024];                                                                         \
        snprintf(buffer, sizeof(buffer), "FATAL [%s:%d] " format "\n", strrchr(__FILE__, '/') + 1, \
                 __LINE__, ##__VA_ARGS__);                                                         \
        throw std::runtime_error(buffer);                                                          \
    } while (0)