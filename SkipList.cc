//
// Created by h1386 on 2023/5/15.
//
#include "SkipList.h"
#include <random>
#include <chrono>
using namespace std;
#define MAX_SIZE 2086880
SkipList::SkipList(){
    head = new Node();
    n_nodes = 0;
    size = 0;
    n_level = 1;
}

SkipList::~SkipList() {
    Node *horizon= head;
    Node *vertical = head;
    while(horizon)
    {
        vertical = horizon->down;
        while(horizon){
            Node *tmp = horizon;
            horizon = horizon->right;
            delete tmp;
        }
        horizon = vertical;
    }
    head = nullptr;
    n_nodes = 0;
    size = 0;
}

void SkipList::clear(){
    Node *horizon= head;
    Node *vertical = head;
    while(horizon)
    {
        vertical = horizon->down;
        while(horizon){
            Node *tmp = horizon;
            horizon = horizon->right;
            delete tmp;
        }
        horizon = vertical;
    }
    head = new Node();
    n_nodes = 0;
    size = 0;
    n_level = 1;
}

unsigned int SkipList::RandomLevel()
{
    static std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
    static std::uniform_real_distribution<double> distribution(0.0, 1.0);
    unsigned int level = 1;
    while((distribution(generator))<=0.25&&level<MAX_LEVEL) {
        level++;
    }
    return level;
}

unsigned int SkipList::getLevel(){
    return n_level;
}
bool SkipList::get(const uint64_t key, string &value, bool &deleted){
    deleted = false;
    if(!n_nodes) return false;
    Node *p = head;
    while(p){
        while(p->right && p->right->key<=key){
            p = p->right;
            if(p->key == key) {
                if(p->val!= "~DELETED~")
                {
                    value = p->val;
                    return true;
                }
                else
                {
                    deleted = true;
                    return false;
                }
            }
        }
        p = p->down;
    }
    return false;
}
bool SkipList::put(const uint64_t key, const string &val)
{
    vector<Node*> path;
    Node *p = head;
    bool exited = false;
    while(p){
        while(p->right&&p->right->key<key){
            p = p->right;
        }
        //the key exists,put--->replace
        if(p->right&&p->right->key==key){
            p = p->right;
            exited = true;
            break;
        }
        path.push_back(p);
        p = p->down;
    }
    if(exited)
    {
        int tmp = size + val.size()-p->val.size();
        if(tmp>MAX_SIZE) return false;
        while(p)
        {
            p->val = val;
            p = p->down;
        }
        size = tmp;
        return true;
    }
    int tmp = size+val.size()+12;
    if(tmp>MAX_SIZE) return false;
    Node *downNode = nullptr;
    ::uint32_t level =1;
    ::uint32_t height = RandomLevel();
    while(level<=height && path.size()>0){
        Node *insert = path.back();
        path.pop_back();
        insert->right = new Node(insert->right, downNode, key, val);
        downNode = insert->right;
        level++;
    }
    if(height>n_level)
    {
        Node * oldHead = head;
        head = new Node();
        head->right = new Node(nullptr, downNode,key,val);
        head->down = oldHead;
        n_level++;
    }
    n_nodes++;
    size = tmp;
    return true;
}

Node *  SkipList::scan(uint64_t key1, uint64_t key2) {
    Node *res = nullptr;
    Node *p = head;
    while(p)
    {
        while(p->right && p->right->key < key1)
            p = p->right;
        if(p->right &&p->right->key >= key1 && p->key <= key2)
        {
            res = p->right;
            break;
        }
        p = p->down;
    }
    return res;
}

void SkipList::pre_sstable(uint64_t &minkey, uint64_t &maxkey, vector<pair<uint64_t, string>> &list) {
   Node *p = head;
   while(p->down) p= p->down;
   p = p->right;
   while(p)
   {
       if(size == 1) minkey = p->key;
       maxkey = p->key;
       list.push_back(pair<uint64_t,string>(p->key,p->val));
       p = p->right;
   }
   //cout<<"size: "<<size<<endl;
}
