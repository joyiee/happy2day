//
// Created by h1386 on 2023/5/15.
//
#include "SSTable.h"
#include <iostream>
#include <fstream>
#include <limits.h>
#include <string.h>

using namespace std;

SSTInfo::SSTInfo() {}
//use .sst file to initialize SSTable Cache
SSTInfo::SSTInfo(string path, uint64_t &t_stamp, string dir){
    //???
    file_addr = dir;
    //read .sst file
    ifstream file(path, ios::binary);
    if(!file){
        cout<<"Error,failed to open file !";
        exit(-1);
    }
    //get head read timestamp
    file.seekg(0,ios::beg);
    file.read(reinterpret_cast<char *>(&timestamp), sizeof(uint64_t));
    t_stamp = timestamp;

    //get pair_num, min_key, max_key
    file.read(reinterpret_cast<char*>(&pair_num),sizeof (::uint64_t));
    file.read(reinterpret_cast<char*>(&min_key),sizeof (::uint64_t));
    file.read(reinterpret_cast<char*>(&max_key),sizeof (::uint64_t));

    //get bloomfilter
    char *buffer = (char*)malloc(10241);
    //make sure buffer can be read as a string
    buffer[10240] = '\0';
    file.read(buffer,10240);
    bloom_filter = new BloomFilter(buffer);
    //gey pairindex
    ::uint64_t key;
    ::uint32_t offset;
    for(::uint64_t i=0;i<pair_num;i++)
    {
        file.read(reinterpret_cast<char*>(&key),sizeof (::uint64_t));
        file.read(reinterpret_cast<char*>(&offset),sizeof (::uint32_t));
        IndexList.emplace_back(PairIndex(key,offset));
    }
    free(buffer);
}

//when memtable changes to sstable, use the data in memtable to initailize sstable cache
SSTInfo::SSTInfo(::uint64_t &buffer_size,string &value_data, ::uint64_t t_stamp, int num, int min, int max, vector<pair<::uint64_t,string>> &list, string dir) {
    //get data from memtable,and return data (the sst file size and the last value)
    file_addr = dir;
    timestamp = t_stamp;
    pair_num = num;
    min_key = min;
    max_key = max;
    bloom_filter = new BloomFilter();
    ::uint64_t off = 32+10240+12*num;
    for(int i=0;i<num;i++)
    {
        IndexList.emplace_back(PairIndex(list[i].first,off));
        off+=(list[i].second.length());
        value_data+=list[i].second;
        bloom_filter->put(list[i].first);
    }

    buffer_size = off;

}
bool SSTInfo::Get(const ::uint64_t &key, ::uint32_t &offset, ::uint32_t &length, bool &end)
{
    if(!bloom_filter->get(key)) return false;
    pair_num = IndexList.size();
    //binary search
    ::uint64_t left = 0,right = pair_num-1,mid;
    while(left<=right) {
        mid = left + (right - left) / 2;
        if (mid < 0 || mid > pair_num - 1) return false;
        if (IndexList[mid].key == key) {
            offset = IndexList[mid].offset;
            if (mid != pair_num - 1)
                length = IndexList[mid + 1].offset - offset;
            else {
                end = true;
                length = INT_MAX;
            }
            return true;
        } else if (IndexList[mid].key < key)
            left = mid + 1;
        else
            right = mid - 1;
    }
    return false;
}

string SSTInfo::Filename()
{
    return file_addr+ "/"+to_string(timestamp)+ "_"+ to_string(pair_num)+"_"+to_string(min_key)+".sst";
}
string SSTInfo::Fileaddr(){
    return file_addr;
}

SSTInfo::~SSTInfo()
{
    delete bloom_filter;
}



SSTable::SSTable() {
    size = 0;
    count = 0;
}

SSTable::SSTable(SSTInfo *cache) {
    file_addr = cache->Fileaddr();
    count = cache->pair_num;
    size = 10272+12*count;
    timestamp = cache->timestamp;
    string file_name = cache->Filename();
    ifstream file(file_name,ios::binary);
    if(!file){
        cout<<"Error! open file "<<file_name<<" falied"<<endl;
        ::exit(-1);
    }
    string tmpval;
    ::uint32_t l_val;
    //file.seekg(cache->IndexList[0].offset,ios::beg);
    for(::uint32_t i=0;i<cache->pair_num-1;i++){
        l_val = cache->IndexList[i+1].offset-cache->IndexList[i].offset;
        char *value = nullptr;
        file.seekg(cache->IndexList[i].offset,ios::beg);
        value = (char*) ::malloc(l_val+1);
        if(!value){
            cout<<"value create error!"<<endl;
        }
        value[l_val] = '\0';
        file.read(value,l_val);
        pairdata.emplace_back(pair<::uint64_t,string>(cache->IndexList[i].key,value));
        free(value);
    }
    string endval;
    file>>endval;
    pairdata.emplace_back(pair<::uint64_t,string>(cache->IndexList[count-1].key,endval));
}
SSTable:: ~SSTable(){
    std::vector<std::pair<uint64_t, string>>().swap(pairdata);
}
SSTInfo* SSTable::WriteFile(string dir, ::uint32_t n, ::uint64_t t_stamp, vector<pair<::uint64_t,string>> &mem,::uint64_t min, ::uint64_t max){
    ::uint64_t buffer_size = 0;
    string lastvalue;
    timestamp = t_stamp;
    file_addr = dir;
    count = n;
    string file_name = dir+"/"+to_string(timestamp)+ "_"+ to_string(count)+"_"+to_string(min)+".sst";
    //new a cache
    SSTInfo *cache = new SSTInfo(buffer_size,lastvalue,timestamp,n,min,max,mem,dir);
    ofstream file(file_name,ios::app|ios::binary);
    if(buffer_size > 2*1024*1024)
    {
        cout<<"Error! the file size is overflow!"<<endl;
        cout<<buffer_size<<endl;
    }
    //prepare the data to write into .sst file
    char *buffer = (char*)malloc(buffer_size);
    char *bitmap = (char*)malloc(10240);
    cache->bloom_filter->WriteBuffer(bitmap);
    *(::uint64_t*)buffer = timestamp;
    *(::uint64_t*)(buffer+8) = count;
    *(::uint64_t*)(buffer+16) = min;
    *(::uint64_t*)(buffer+24) = max;
    memcpy(buffer+32,bitmap,10240);
    char* off_index = buffer+10272;
    for(::uint32_t i=0;i<count;i++)
    {
        *(::uint64_t*)(off_index) = cache->IndexList[i].key;
        off_index+=8;
        *(::uint32_t*)(off_index) = cache->IndexList[i].offset;
        off_index+=4;
        memcpy(buffer+cache->IndexList[i].offset,mem[i].second.c_str(),mem[i].second.size());
    }
   file.write(buffer,buffer_size);
    free(buffer);
    free(bitmap);
    file.close();
    return cache;
}

string SSTable::GetVal(::uint32_t offset, uint32_t length, string file_name, bool end){
    ifstream file(file_name,ios::binary);
    if(!file){
        cout<<"open file error"<<endl;
    }
    char *result = nullptr;
    if(end) length = 2*1024*1024-offset;
    result = (char*)malloc(length+1);
    if(!result){
        cout<<"malloc error!"<<endl;
    }
    if(end)
    {
        file.seekg(offset,ios::beg);
        file.read(result,length);
        string res(result);
        ::free(result);
        file.close();
        res = res.substr(0,length);
        return res;
    }

    else{
        file.seekg(0,ios::end);
        int filesize  =file.tellg();
        length = filesize -offset;
        file.seekg(offset,ios::beg);
        string res;
        file>>res;
        return  res.substr(0,length);
    }

}



vector<SSTInfo*>  SSTable::Merge(vector<SSTable*> &tablelist, string dir , bool last){
    while (tablelist.size()>1)
    {
        SSTable *t1 = tablelist.back();
        tablelist.pop_back();
        SSTable *t2 = tablelist.back();
        tablelist.pop_back();
        SSTable *table_combined = CombineTwo(t1,t2,last);
        tablelist.emplace_back(table_combined);
    }

    tablelist[0]->file_addr = dir;
    vector<SSTInfo*> re = Divide(tablelist[0]);
    return re;
}
SSTable* SSTable::CombineTwo(SSTable* t1, SSTable *t2, bool last){

    SSTable *res = new SSTable();
    res->file_addr = max(t1->file_addr,t2->file_addr);
    res->timestamp = max(t1->timestamp,t2->timestamp);
    auto i = t1->pairdata.begin(),j = t2->pairdata.begin();
    if(last)
    {
        while(i != t1->pairdata.end() && j != t2->pairdata.end()){
            if((*i).first < (*j).first){
                res->pairdata.emplace_back(*i);
                ++i;
            }
            else if((*i).first > (*j).first){
                res->pairdata.emplace_back(*j);
                ++j;
            }
            else{
                if(t1->timestamp <=t2->timestamp)
                    res->pairdata.emplace_back(*j);
                else
                    res->pairdata.emplace_back(*i);
                ++i;
                ++j;
            }
        }
        while(i != t1->pairdata.end()){
            res->pairdata.emplace_back(*i);
            ++i;
        }
        while(j != t2->pairdata.end()){
            res->pairdata.emplace_back(*j);
            ++j;
        }
    }
    else
    {
        while(i != t1->pairdata.end() && j != t2->pairdata.end()){
            if((*i).first < (*j).first){
                if((*i).second!="~DELETED~")
                    res->pairdata.emplace_back(*i);
                ++i;
            }
            else if((*i).first > (*j).first){
                if((*j).second!="~DELETED~")
                    res->pairdata.emplace_back(*j);
                ++j;
            }
            else{
                if(t1->timestamp <=t2->timestamp)
                {
                    if((*j).second!="~DELETED~")
                        res->pairdata.emplace_back(*j);
                }
                else
                {
                    if((*i).second!="~DELETED~")
                        res->pairdata.emplace_back(*i);
                }
                ++i;
                ++j;
            }
        }
        while(i != t1->pairdata.end()){
            if((*i).second!="~DELETED~")
                res->pairdata.emplace_back(*i);
            ++i;
        }
        while(j != t2->pairdata.end()){
            if((*j).second!="~DELETED~")
                res->pairdata.emplace_back(*j);
            ++j;
        }
    }

    delete t1;
    delete t2;
    return res;
}
vector<SSTInfo*> SSTable::Divide(SSTable *table){
    vector<SSTInfo*>res;
    vector<pair<uint64_t,string>> data;
    string addr = table->file_addr;
    ::uint32_t space = 2*1024*1024-10272;
    auto tmp = table->pairdata.begin();
    while(tmp!=table->pairdata.end()){
        while(space&&tmp!=table->pairdata.end())
        {
            if(space<=12+(*tmp).second.size()) break;
            data.emplace_back(*tmp);
            space = space-12-(*tmp).second.size();
            tmp++;
        }
        space = 2*1024*1024-10272;
        ::uint64_t min = data[0].first;
        int num = data.size();
        ::uint64_t max = data[num-1].first;
        SSTInfo *new_cache = WriteFile(table->file_addr,data.size(),table->timestamp,data,min,max);
        res.emplace_back(new_cache);
        data.clear();
    }
    return res;
}