#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "SSTable.h"
using namespace std;
class level{
public:
    ::uint32_t index;
    ::uint32_t max_file_num;
    bool isTiering;
    level(){
        index = 0;
        max_file_num = 0;
        isTiering = true;
    }
    level(::uint32_t in, ::uint32_t max, bool tier)
    {
        index =in;
        max_file_num = max;
        isTiering = tier;
    }
};


class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
    SkipList *MemTable;
    string file_root;
    uint64_t timestamp;
    ::uint32_t n_level;
    vector<level*> LevelList;
    vector<vector<SSTInfo*>> CacheList;
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;

    pair<::uint64_t,::uint64_t> getInterval(vector<SSTInfo*> &up_files);

    void Compaction(::uint32_t whichlevel);
};
