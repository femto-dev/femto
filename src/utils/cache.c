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

  femto/src/utils/cache.c
*/
// Implements an LRU cache.

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "cache.h"

error_t cache_create(cache_t* c, int cache_size, 
                     hashmap_hash_t hash, hashmap_cmp_t cmp,
                     int data_size, void* context,
                     cache_fault_t fault, cache_evict_t evict,
                     cache_copy_id_t copy_id, cache_free_id_t free_id
                     ) 
{
  error_t err;
  int i;
  memset(c, 0, sizeof(cache_t));

  // allocate the data section.
  c->data = malloc(cache_size * data_size);
  if( ! c->data ) {
    err = ERR_MEM;
    goto error;
  }
  // allocate the list section
  c->list = (cache_list_entry_t*) malloc(cache_size * sizeof(cache_list_entry_t));
  if( ! c->list ) {
    err = ERR_MEM;
    goto error;
  }
  // allocate the mapping.
  err = hashmap_create(&c->mapping, hashmap_size_for_elements(cache_size),
                       hash, cmp);
  if( err ) goto error;

  c->fault = fault;
  c->evict = evict;
  c->copy_id = copy_id;
  c->free_id = free_id;
  c->context = context;
  c->cache_size = cache_size;
  c->data_size = data_size;

  for( i = 0; i < c->cache_size; i++ ) {
    c->list[i].prev = i - 1;
    c->list[i].next = i + 1;
    c->list[i].id = NULL;
    //c->list[i].data_number = i;
  }
  c->list[0].prev = -1;
  c->list[c->cache_size-1].next = -1;
  c->head = 0;
  c->tail = c->cache_size-1;

  return ERR_NOERR;

error:
  free( c->data );
  free( c->list );
  return err;
}

hash_t numeric_hash(const void* a)
{
  long i = (long) a;
  assert( a > 0 );
  return i - 1;
}

int numeric_cmp(const void* a, const void* b)
{
  long ai = (long) a;
  long bi = (long) b;
  return ai - bi;
}

cache_id_t numeric_copy_id(cache_id_t a)
{
  return a;
}

void numeric_free_id(cache_id_t a)
{
}

// Numeric caches use number+1 as the id.
error_t numeric_cache_create(cache_t* c, int cache_size,
                     int data_size, void* context,
                     cache_fault_t fault, cache_evict_t evict
                     )
{
  return cache_create(c, cache_size, numeric_hash, numeric_cmp,
                      data_size, context, fault, evict, numeric_copy_id, numeric_free_id);
}

// moves entry li to the front and sets
// its id to id.
static inline void
move_to_front(cache_t* c, long li, cache_id_t id)
{
  cache_id_t to_free = NULL;
  cache_list_entry_t* le;
  cache_list_entry_t* prev = NULL;
  cache_list_entry_t* next = NULL;
  cache_list_entry_t* head = NULL;
  hm_entry_t entry;

  assert( li >= 0 );

  le = & c->list[li];

  if( li != c->head ) {

    if( le->prev != -1 ) prev = & c->list[le->prev];
    if( le->next != -1 ) next = & c->list[le->next];

    // remove the element from the list
    if( prev ) prev->next = le->next;
    else c->head = le->next;
    if( next ) next->prev = le->prev;
    else c->tail = le->prev;

    // now place it at the beginning of the list.
    head = &c->list[c->head];
    le->next = c->head;
    le->prev = -1;
    head->prev = li;
    c->head = li;
  }

  // update the IDs.
  if( le->id ) {
    // remove the old entry
    entry.key = le->id;
    entry.value = NULL;
    hashmap_delete(&c->mapping, &entry);
    to_free = le->id;
  }
  le->id = c->copy_id(id);
  if( to_free ) c->free_id(to_free);
  entry.key = le->id;
  entry.value = (void*) li;
  hashmap_insert(&c->mapping, &entry);
}

int is_cached(cache_t* c, cache_id_t id)
{
  hm_entry_t entry;
  long idx;

  entry.key = id;
  entry.value = NULL;

  if( hashmap_retrieve(&c->mapping, &entry)) {
    idx = (long) entry.value;
    if( 0 == c->mapping.cmp(id, c->list[idx].id) ) {
      return 1;
    } else {
      return 0;
    }
  } else {
    return 0;
  }
}

error_t cache_get(cache_t* c, cache_id_t id, void** ret_data)
{
  cache_list_entry_t* le;
  void* data;
  error_t err;
  hm_entry_t entry;
  long idx;
  int data_number;

  entry.key = id;
  entry.value = NULL;

  // first, check the mapping.
  if( hashmap_retrieve(&c->mapping, &entry) ) {
    idx = (long) entry.value;
    if( 0 == c->mapping.cmp(id, c->list[idx].id ) ) {
      // it's a valid mapping.
      // move it to the front of the list.
      //printf("Cache before mtf; id=%p\n", c->list[idx].id);
      move_to_front(c, idx, c->list[idx].id);
      data_number = idx;
      data = ((unsigned char*) c->data) + data_number * c->data_size;
      *ret_data = data;
      return ERR_NOERR;
    }
  }

  // if it's not an valid mapping, we'll evict the data
  // item pointed to by the last list element, then
  // load the new item, and finally move it to the front
  // of the list.

  // find a home for the new data item.
  // does the tail item in the list already have a data item?
  le = & c->list[c->tail];
  data_number = c->tail;
  data = ((unsigned char*) c->data) + data_number * c->data_size;
  if( le->id && c->evict ) {
    c->evict(le->id, c->data_size, data, c->context);
  }

  // load the new item.
  //printf("Cache before fault; id=%p\n", id);
  err = c->fault(id, c->data_size, data, c->context);
  if( err ) {
    if( le->id ) {
      // remove the old entry
      entry.key = le->id;
      entry.value = NULL;
      hashmap_delete(&c->mapping, &entry);
      c->free_id(le->id);
      le->id = NULL;
    }
    return err;
  }

  //printf("Cache after fault; id=%p\n", id);

  // move the tail element to the front of the list.
  move_to_front(c, c->tail, id);

  *ret_data = data;

  return ERR_NOERR;
}

void cache_destroy(cache_t* c)
{
  int i;
  cache_list_entry_t* le;
  unsigned char* data;
  int data_number;

  // OK to destroy a zero-initialized cache.
  if( c->data == NULL ) return;

  for( i = 0; i < c->cache_size; i++ ) {
    le = & c->list[i];
    data_number = i;
    data = ((unsigned char*) c->data) + data_number * c->data_size;
    if( le->id != NULL && c->evict ) c->evict(le->id, c->data_size, data, c->context);
    if( le->id ) c->free_id(le->id);
  }

  free(c->list);
  free(c->data);
  hashmap_destroy(&c->mapping);
  c->list = NULL;
  c->data = NULL;
}

