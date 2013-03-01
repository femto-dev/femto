/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

    * This is a work of the United States Government and is not protected by
      copyright in the United States.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Lesser General Public License for more details.

  You should have received a copy of the GNU Lesser General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.

  femto/src/utils/cache.h
*/
// Implements an LRU cache. 
// Assumes that there's a fixed-size and small address space
// stores 4 bytes for every possible id with 0<=id<max_id

#ifndef _CACHE_H_
#define _CACHE_H_

#include "hashmap.h"
#include "error.h"

// Emtpy id is NULL.
// Numeric ids must start from 1, not zero.
typedef void* cache_id_t;

typedef struct {
  int prev;
  int next;
  cache_id_t id;
  //int data_number;
} cache_list_entry_t;

typedef error_t (*cache_fault_t)(cache_id_t id, int data_size, void* data, void* context);
typedef void (*cache_evict_t)(cache_id_t id, int data_size, void* data, void* context);
typedef cache_id_t (*cache_copy_id_t)(cache_id_t id);
typedef void (*cache_free_id_t)(cache_id_t id);

typedef struct {
  // function to load data with number ID into the data section
  cache_fault_t fault;
  // function to release anything occupied by data when evicting
  cache_evict_t evict;
  // function to copy an ID
  cache_copy_id_t copy_id;
  // function to free an ID
  cache_free_id_t free_id;

  void* context;
  int cache_size; // number of elements to cache.
  int data_size; // the size of each element in the cache.
  // a list storing
  // occupied cache slots 
  // unoccupied cache slots
  int head; // index of the first non-free item in the list
  int tail; // index of the last non-free item in the list
  cache_list_entry_t* list; // list always has cache_size elements
  void* data;
  hashmap_t mapping; // from id to list entry number
} cache_t;

// id will always be < max_id
error_t cache_create(cache_t* c, int cache_size, 
                     hashmap_hash_t hash, hashmap_cmp_t cmp,
                     int data_size, void* context,
                     cache_fault_t fault, cache_evict_t evict,
                     cache_copy_id_t copy_id, cache_free_id_t free_id
                     );
error_t numeric_cache_create(cache_t* c, int cache_size, 
                     int data_size, void* context,
                     cache_fault_t fault, cache_evict_t evict
                     );

error_t cache_get(cache_t* c, cache_id_t id, void** ret_data);

void cache_destroy(cache_t* c);

// Return 1 if the id is loaded (without changing whether it's 
// cached or not).
int is_cached(cache_t* c, cache_id_t id);

static inline
cache_id_t numeric_cache_id_for_num(long num)
{
  return (cache_id_t) (num + 1);
}
static inline
long numeric_cache_num_for_id(cache_id_t id)
{
  return ((long) id) - 1;
}
// macro to avoid stupid "dereferencing type-punned ptr" error in GCC
#define NUMERIC_CACHE_GET(error,cache,idnum,ptr) \
{ \
  void* cache_get_ptr; \
  error = cache_get(cache, numeric_cache_id_for_num(idnum), &cache_get_ptr); \
  ptr = cache_get_ptr; \
}

#define CACHE_GET(error,cache,idnum,ptr) \
{ \
  void* cache_get_ptr; \
  error = cache_get(cache, idnum, &cache_get_ptr); \
  ptr = cache_get_ptr; \
}

static inline 
int is_cached_numeric(cache_t* c, long id)
{
  return is_cached(c, numeric_cache_id_for_num(id));
}

#endif
