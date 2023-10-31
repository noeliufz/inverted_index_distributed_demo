#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cache.h"
#include "utils.h"

void init_cache(Cache* cache, int cache_num) {
    cache->cache_num = cache_num;
    cache->table = calloc(cache_num, sizeof(CacheNode));
    cache->time_index = 0;
    cache->size = 0;
}

int lookup_cache(Cache* cache, char* key, sem_t* mutex, sem_t* w, int* readcnt) {
    P(mutex);
    (*readcnt)++;
    if (*readcnt == 1) /* First in */
        P(w);
    V(mutex);

    int i;
    int index = -1;
    for(i = 0; i < cache->size; i++) {
        if (strcmp(cache->table[i].key, key) == 0) {
            index = i;
            break;
        }
    }

    P(mutex);
    (*readcnt)--;
    if (*readcnt == 0) /* Last out */
        V(w);
    V(mutex);

    return index;
}

char* get_from_cache(Cache* cache, int index, sem_t* mutex, sem_t* w) {
    P(w);

    CacheNode node = cache->table[index];
    char* result = strdup(node.value);
    node.used = 1;
    V(w);

    return result;
}


void update_time_index(Cache* cache) {
    int time_index = cache->time_index;
    if (time_index == cache->cache_num - 1)
        time_index = 0;
    else 
        time_index++;
}


void write_cache(Cache* cache, char* key, char* value, sem_t* mutex, sem_t* w) {
    P(w);

    int index;
    // if the hash table is full

    if (cache->size == cache->cache_num) {
        while (cache->table[cache->time_index].used != 0) {
            cache->table[cache->time_index].used = 0;
            update_time_index(cache);
        }
        cache->table[cache->time_index].used = 1;
        free(cache->table[cache->time_index].value);
        free(cache->table[cache->time_index].key);
        index = cache->time_index;
    } else {
        index = cache->size;
        cache->size++;
    }

    cache->table[index].used = 1;
    cache->table[index].key = strdup(key);
    cache->table[index].value = strdup(value);


    V(w);
}   