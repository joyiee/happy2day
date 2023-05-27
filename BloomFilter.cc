//
// Created by h1386 on 2023/5/15.
//
#include "BloomFilter.h"
#include "MurmurHash3.h"
#include <string.h>
BloomFilter::BloomFilter() {}
BloomFilter::BloomFilter(const char *buffer) {
    memcpy((char*)&bitmap,buffer,BF_SIZE/8);
}

 void BloomFilter:: put(const ::uint64_t &key)
{

    ::uint32_t  position[4] = {0};
    MurmurHash3_x64_128(&key, sizeof (key), 1, position);
    for(int i=0;i<4;i++)
    {
        if(position[i]>BF_SIZE)
            position[i]%=BF_SIZE;
        bitmap.set(position[i]);
    }
}
bool BloomFilter::get(const ::uint64_t &key)
{
    ::uint32_t  position[4] = {0};
    MurmurHash3_x64_128(&key, sizeof (key), 1, position);
    for(int i=0;i<4;i++)
    {
        if(position[i]>BF_SIZE)
            position[i]%=BF_SIZE;
        if(!bitmap[position[i]]) return false;
    }
    return true;
}
void BloomFilter::WriteBuffer(char *buffer) {
    memcpy(buffer,(char*)&bitmap, BF_SIZE/8);
}



