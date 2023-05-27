#include "kvstore.h"
#include "utils.h"
#include "SkipList.h"
#include "SSTable.h"
#include <string>
#include <algorithm>
#include <vector>
#include <fstream>

using namespace std;

struct
{
    bool operator()(const SSTInfo* p1, const SSTInfo* p2){
        return p1->timestamp > p2->timestamp || (p1->timestamp == p2->timestamp && p1->min_key > p2->min_key);
    }
}filesort;

struct
{
    bool operator()(const SSTInfo *p1, const SSTInfo *p2){
        return p1->max_key <= p2->min_key;
    }
}keysort;

/**
 * logic:
 * if the dir is not exsited,make one
 * else read all the .sst files, store their head and bloomhead
 * also make a memtable
 */
KVStore::KVStore(const std::string &dir): KVStoreAPI(dir)
{

    timestamp = 1;
    file_root = dir;
    vector<std::string> files;
    if(!utils::dirExists(dir))
    {
        utils::_mkdir(dir.c_str());
        n_level = 0;
    }
    else
        n_level = utils::scanDir(dir,files);
    //read existed files
    if(n_level){
        for(::uint32_t i=0;i<n_level;i++)
        {
            string subdir = "level-"+ to_string(i);
            vector<SSTInfo*> level_cache;
            files.clear();
            string full_path = dir+"/"+subdir;
            //sort()
            sort(files.begin(),files.end());
            for(::uint32_t j=0;j<files.size();j++)
            {
                string file_path = full_path+"/"+files[j];
                ::uint64_t tmp_ts = 0;
                SSTInfo *cache = new SSTInfo(file_path,tmp_ts,full_path);
                timestamp = max(timestamp,tmp_ts);
                level_cache.emplace_back(cache);
            }
            CacheList.push_back(level_cache);
        }
    }

    //set level config
    string config = "./default.conf";
    cout<<"config "<<config<<endl;
    ifstream configfile(config);
    if(!configfile) cout<<"open config error"<<endl;
    if(configfile){
        string line;
        while(getline(configfile,line))
        {
            string w1,w2,w3;
            stringstream ss(line);
            ss>>w1>>w2>>w3;
            level *tmp = new level(atoi(w1.c_str()), ::atoi(w2.c_str()),(w3=="Tiering"));
            LevelList.push_back(tmp);
        }
    }
    cout<<"level list "<<LevelList.size()<<endl;
    MemTable = new SkipList();
}


/**
* logic:
* 1. write the memtable to file
* 2. Compctione(important!!!!)
* 3.relese cachedata
* 4. delete the  memtable
*/
KVStore::~KVStore()
{
    //save Memtable
    vector<pair<::uint64_t,string>> mem_table;
    ::uint64_t min_key=0, max_key=0;
    MemTable->pre_sstable(min_key,max_key,mem_table);
    int s = mem_table.size();
    if(!mem_table.empty())
    {
        string dir = file_root+"/level-0";
        if((!CacheList.size())&&n_level==0)
        {
            utils::_mkdir(dir.c_str());
            CacheList.emplace_back(vector<SSTInfo*>{});
            n_level++;
        }
        SSTable table;
        SSTInfo *cache = table.WriteFile(dir,s,timestamp,mem_table,min_key,max_key);
        CacheList[0].push_back(cache);
        Compaction(0);
        timestamp++;
    }
    //release cache data
    while(!CacheList.empty())
    {
        vector<SSTInfo*> level_cache  = CacheList.back();
        CacheList.pop_back();
        if(!level_cache.empty())
        {
            SSTInfo *tmp = level_cache.back();
            level_cache.pop_back();
            delete tmp;
        }
    }
    delete MemTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    bool res = MemTable->put(key,s);
    if(res) return;

    //Memtable overflow,write it into SStable(level 0)and Compaction
    vector<pair<::uint64_t,string>> mem_table;
    ::uint64_t min_key=0, max_key=0;
    ::uint32_t num=0;
    MemTable->pre_sstable(min_key,max_key,mem_table);
    num = mem_table.size();
    if(!mem_table.empty())
    {
        string dir = file_root+"/level-0";
        if((!CacheList.size())&&n_level==0)
        {
            utils::_mkdir(dir.c_str());
            CacheList.emplace_back(vector<SSTInfo*>{});
            n_level++;
        }
        SSTable table;
        SSTInfo *cache = table.WriteFile(dir,num,timestamp,mem_table,min_key,max_key);
        CacheList[0].push_back(cache);
        //compaction
        Compaction(0);
        timestamp++;
        MemTable = new SkipList();
        MemTable->put(key,s);
    }
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    string res;
    bool deleted;
    bool in_mem = MemTable->get(key,res,deleted);
    if(in_mem||deleted) return res;
    //tring to find sstable
    for(::uint32_t i=0;i<n_level;i++)
    {
        vector<SSTInfo*> level_cache = CacheList[i];
        int num = level_cache.size();
        //cout<<num<<endl;
        ::uint32_t offset = 0,length = 0;
        bool isend = false;
        for(int j=num-1;j>=0;j--)
        {
            if(level_cache[j]->Get(key,offset,length,isend)){
                SSTable target(level_cache[j]);
                string file_name = level_cache[j]->Filename();
                //if(key==2026)cout<<"find key "<<key<<" in level "<<i<<" file "<<file_name<<" offset "<<offset<<" length "<<length<<endl;
                res = target.GetVal(offset,length,file_name, isend);

                //cout<<"read from file"<<endl;
                if(res!="~DELETED~") return res;
                else return "";
            }
        }
    }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false if the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    string res = get(key);
    if(res=="") return false;
    put(key,"~DELETED~");
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    //cout<<file_root<<endl;
    //delete files
    for(::uint32_t i=0;i<n_level;i++)
    {
        string dir = file_root+"/"+"level-"+ to_string(i);
        vector<string> file_names;
        int n_file  = utils::scanDir(dir,file_names);
        for(int j = 0; j < n_file;j++){
            string file_path = dir+"/"+file_names[j];
            //cout<<file_path<<endl;
            utils::rmfile(file_path.c_str());
        }
        utils::rmdir(dir.c_str());
    }

    //delete cache
    while(!CacheList.empty())
    {
        vector<SSTInfo*> level_cache  = CacheList.back();
        CacheList.pop_back();
        if(!level_cache.empty())
        {
            SSTInfo *tmp = level_cache.back();
            level_cache.pop_back();
            delete tmp;
        }
    }
    //delete memtable
    delete MemTable;
    MemTable = new SkipList();
    timestamp = 1;
    n_level = 0;
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list)
{
}

/**
 * get the interval of key in all th files
 * */
pair<::uint64_t,::uint64_t> KVStore::getInterval(vector<SSTInfo*> &up_files)
{
    ::uint64_t mink = up_files[0]->min_key;
    ::uint64_t maxk = up_files[0]->max_key;
    int num = up_files.size();
    for(int i=1;i<num;i++)
    {
        mink = min(mink,up_files[i]->min_key);
        maxk = max(maxk,up_files[i]->max_key);
    }
    return pair<::uint64_t ,::uint64_t >(mink,maxk);
}

/**
 * logic:
 * 1. choose files
 * 2. merge files
 * 3. recursive
 * */
void KVStore::Compaction(::uint32_t l) {

    if(LevelList.size()<=l+1)
    {
        int n = LevelList.size();
        int new_num = LevelList.back()->max_file_num*2;
        level *new_level = new level(n,new_num, false);
        LevelList.push_back(new_level);
    }

    //judge if compaction is needed
    ::uint32_t cur_size = CacheList[l].size();
    if(cur_size<=LevelList[l]->max_file_num)
    {
        //cout<<"cur level "<<l<<" cur size "<<cur_size<<endl;
        return;
    }

    //cout<<"start to choose file"<<endl;
    //choose file
    int top_files_num = 0;
    vector<string> com_files_name;
    vector<SSTInfo*> com_files;
    vector<SSTable*> com_files_ss;
    vector<pair<size_t,::uint64_t>> filetime;

    //choose top files
    //topfiles
    if(LevelList[l]->isTiering)
    {
        com_files = CacheList[l];
        //top_files_num = CacheList[l].size();
        CacheList[l].clear();
    }
    else{
        top_files_num = cur_size-LevelList[l]->max_file_num;
        //sort by timestamp
        sort(CacheList[l].begin(),CacheList[l].end(),filesort);
        for(int i=0;i<top_files_num;i++)
            com_files.emplace_back(CacheList[l][cur_size-i-1]);
        //delete the cooperate cache data
        for(int i=0;i<top_files_num;i++)
            CacheList[l].pop_back();
        //restore the sort by key
        sort(CacheList[l].begin(),CacheList[l].end(),keysort);
    }

    //choose down files
    string target_dir = file_root+"/level-"+ to_string(l+1);
    if(l+1<n_level)
    {
        if(!LevelList[l+1]->isTiering)
        {
            //sort(CacheList[l+1].begin(), CacheList[l+1].end(), keysort);
            pair<::uint64_t, ::uint64_t> interval = getInterval(com_files);
            int num = CacheList[l+1].size();
            for(int i=num-1;i>=0;i--) {
                SSTInfo *tmp = CacheList[l + 1][i];
                if ((tmp->min_key <= interval.first && tmp->max_key >= interval.second) ||
                    (tmp->min_key >= interval.first && tmp->min_key <= interval.second)) {
                    //big--->small
                    com_files.emplace_back(tmp);
                    CacheList[l + 1].erase(CacheList[l + 1].begin() + i);
                }
            }
        }
    }

        //create a new level
    else{
        n_level++;
        utils::_mkdir(target_dir.c_str());
        CacheList.emplace_back(vector<SSTInfo*>{});
    }

    //get sstable
    while(!com_files.empty())
    {
        SSTInfo *tmp = com_files.back();
        com_files_name.push_back(tmp->Filename());
        SSTable *table  = new SSTable(tmp);
        com_files_ss.push_back(table);
        com_files.pop_back();
    }

    //delete the files to compaction
    while(!com_files_name.empty())
    {
        string tmp = com_files_name.back();
        utils::rmfile(tmp.c_str());
        com_files_name.pop_back();
    }

    //merge
    SSTable helper;
    bool islast = (n_level==l+2);
    vector<SSTInfo*> new_cachelist = helper.Merge(com_files_ss,target_dir,islast);
    CacheList[l+1].insert(CacheList[l+1].end(),new_cachelist.begin(),new_cachelist.end());
    if(LevelList[l+1]->isTiering) sort(CacheList[l+1].begin(),CacheList[l+1].end(),filesort);
    else sort(CacheList[l+1].begin(),CacheList[l+1].end(),keysort);

    //clear
    int num = com_files.size();
    for(int i=num-1;i>=0;i--)
    {
        SSTInfo *tmp = com_files.back();
        com_files.pop_back();
        delete tmp;
    }

    num = com_files_ss.size();
    for(int i=num-1;i>=0;i--)
    {
        SSTable *tmp = com_files_ss.back();
        com_files_ss.pop_back();
        delete tmp;
    }

    vector<SSTInfo*>().swap(com_files);
    std::vector<SSTable*>().swap(com_files_ss);
    std::vector<std::pair<size_t, uint64_t>>().swap(filetime);
    std::vector<std::string>().swap(com_files_name);


    //recursive
    Compaction(l+1);
    return;
}
