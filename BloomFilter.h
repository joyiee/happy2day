//
// Created by h1386 on 2023/5/24.
//

#ifndef LSMKV_BLOOMFILTER_H
#define LSMKV_BLOOMFILTER_H

#include <bitset>
using namespace std;

#define BF_SIZE 81920

class BloomFilter{
private:
    ::bitset<BF_SIZE> bitmap;
public:
    BloomFilter();
    BloomFilter(const char *buffer);
    void put(const ::uint64_t &);
    bool get(const ::uint64_t &);
    void WriteBuffer(char *buffer);
};
#endif //LSMKV_BLOOMFILTER_H
