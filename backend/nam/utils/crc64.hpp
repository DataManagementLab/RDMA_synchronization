#ifndef CRC64_HPP
#define CRC64_HPP

#include <stdint.h>

void crc64_init(void);
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l);

#ifdef REDIS_TEST
int crc64Test(int argc, char *argv[], int flags);
#endif

#endif
