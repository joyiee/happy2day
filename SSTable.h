//
// Created by h1386 on 2023/5/15.
//

#ifndef LSMKV_SSTABLE_H
#define LSMKV_SSTABLE_H

#include <iostream>
#include <string>
#include <cstdint>
#include <list>
#include "BloomFilter.h"
#include "SkipList.h"
using namespace std;

struct PairIndex
{
    uint64_t key;
    uint64_t offset;

    PairIndex(uint64_t k = 0, uint64_t off = 0) {
        key = k;
        offset = off;
    }
};

class SSTInfo{
public:
    uint64_t timestamp;
    uint64_t pair_num;
    uint64_t min_key;
    uint64_t max_key;
    BloomFilter *bloom_filter;
    vector<PairIndex> IndexList;
    string file_addr;

    SSTInfo();
    SSTInfo(string path, uint64_t &t_stamp, string dir);
    SSTInfo(::uint64_t &buffer_size,string &value_data, ::uint64_t t_stamp, int num, int min, int max, vector<pair<::uint64_t,string>> &pair_list, string dir);
    bool Get(const ::uint64_t &key, ::uint32_t &offset, ::uint32_t &length, bool &end);
    string Filename();
    string Fileaddr();
    ~SSTInfo();
};

class SSTable{
private:
    uint64_t timestamp;
    uint64_t size;
    uint32_t count;
    vector<pair<uint64_t,string>> pairdata;
    string file_addr;
public:
    /*basic*/
    SSTable();
    ~SSTable();

    /*cooperate with SSTInfo*/
    //using cachedata to initialize SST
    SSTable(SSTInfo *cache_data);
    //write data to cache
    SSTInfo WriteCache(SSTInfo *cachedata);

    /*cooperate with realFile*/
    //get value from file
    string GetVal(::uint32_t offset,::uint32_t length, string file, bool end);
    //set file address
    //save the sstable in file
    SSTInfo* WriteFile(string dir, ::uint32_t s, ::uint64_t t_stamp, vector<pair<::uint64_t,string>> &mem,::uint64_t min, ::uint64_t max);
    vector<SSTInfo*>  Merge(vector<SSTable*> &tablelist, string dir, bool last);
    SSTable* CombineTwo(SSTable* t1, SSTable *t2, bool last);
    vector<SSTInfo*> Divide(SSTable *table);



};
#endif //LSMKV_SSTABLE_H
