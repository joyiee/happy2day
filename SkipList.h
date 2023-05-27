//
// Created by h1386 on 2023/5/15.
//
#ifndef LSMKV_SKIPLIST_H
#define LSMKV_SKIPLIST_H
#include <cstdint>
#include <string>
#include <random>
#include <list>
using namespace std;
#define MAX_LEVEL 32
struct Node{
    Node *right,*down;
    ::uint64_t key;
    string val;
    Node(Node *right,Node *down, int key,string val): right(right), down(down), key(key),val(val){}
    Node(): right(nullptr), down(nullptr) {}
};
class SkipList{
private:
    Node *head;
    uint64_t n_nodes;
    uint64_t size;
    unsigned int n_level;
    unsigned int RandomLevel();
public:
    SkipList();
    ~SkipList();
    void clear();
    unsigned int getLevel();
    bool get(const uint64_t key, string &val, bool &deleted);
    bool put(const uint64_t key, const string &val);
    bool remove(const uint64_t key);
    Node* scan(uint64_t key1, uint64_t key2);
    void pre_sstable(uint64_t &minkey, uint64_t &maxkey, vector<pair<uint64_t,string>> &list);
};

#endif //LSMKV_SKIPLIST_H
