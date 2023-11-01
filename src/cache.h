#include "csapp/csapp.h"

typedef struct CacheNode {
    char* key;
    char* value;
    int used;
} CacheNode;

typedef struct Cache {
    CacheNode* array;
    int cache_num;
    int time_index;
    int size;
} Cache;

void init_cache(Cache* cache, int cache_num);
char* lookup_cache(Cache* cache, char* key, sem_t* mutex, sem_t* w, int* readcnt);
void update_time_index(Cache* cache);
void write_cache(Cache* cache, char* key, char* value, sem_t* mutex, sem_t* w);