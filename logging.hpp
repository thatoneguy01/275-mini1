#pragma once

#include <stdio.h>

#ifdef ENABLE_LOGGING
#define LOG(fmt, ...) do { \
    fprintf(stderr, "[%s:%d] " fmt "\n", __FILE__, __LINE__, ##__VA_ARGS__); \
} while(0)
#else
#define LOG(fmt, ...) do {} while(0)
#endif