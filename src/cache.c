#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "cache.h"
#include "utils.h"

/**
 * Init the cache
*/
void init_cache(Cache* cache, int cache_num) {
    cache->cache_num = cache_num;
    cache->array = calloc(cache_num, sizeof(CacheNode));
    cache->time_index = 0;
    cache->size = 0;
}

/**
 * Reader. Look up the index of the key in cache.
 * @return The index of the data requested, or -1 if not found
*/
char* lookup_cache(Cache* cache, char* key, sem_t* mutex, sem_t* w, int* readcnt) {
    P(mutex);
    (*readcnt)++;
    if (*readcnt == 1) /* First in */
        P(w);
    V(mutex);

    int i;
    char* result = NULL;
    int index = -1;
    for(i = 0; i < cache->size; i++) {
        if (strcmp(cache->array[i].key, key) == 0) {
            index = i;
            break;
        }
    }

    if (index != -1) 
        result = strdup(cache->array[index].value);
    

    P(mutex);
    (*readcnt)--;
    if (index != -1)
        cache->array[index].used += 1;
    if (*readcnt == 0) /* Last out */
        V(w);
    V(mutex);

    return result;
}

/**
 * Update the time index
*/
void update_time_index(Cache* cache) {
    int time_index = cache->time_index;
    if (time_index == cache->cache_num - 1)
        time_index = 0;
    else 
        time_index++;
}

/**
 * Writer, write the key and value to cache. Remove one cache when it is full.
*/
void write_cache(Cache* cache, char* key, char* value, sem_t* mutex, sem_t* w) {
    P(w);

    int index;
    // if the array is full

    if (cache->size == cache->cache_num) {
        while (cache->array[cache->time_index].used != 0) {
            cache->array[cache->time_index].used = 0;
            update_time_index(cache);
        }
        cache->array[cache->time_index].used = 1;
        free(cache->array[cache->time_index].value);
        free(cache->array[cache->time_index].key);
        index = cache->time_index;
        update_time_index(cache);
    } else {
        index = cache->size;
        cache->size++;
    }

    cache->array[index].used = 1;
    cache->array[index].key = strdup(key);
    cache->array[index].value = strdup(value);


    V(w);
}   