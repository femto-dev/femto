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

  femto/src/utils/hashmap.c
*/
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "hashmap.h"
#include "bit_funcs.h"

int table_inited;
hash_t random_table[256];

void init_table(void)
{
  int i;
  unsigned int j;
  hash_t value;

  for( i = 0; i < 256; i++ ) {
    value = 0;
    // super-random randomizer!
    // the nice property of this code is that it supports
    // 32-bit or 64-bit random numbers. Not particularily fast.
    for( j = 0; j < 2 * sizeof(hash_t); j++ ) {
      // ask for one byte at a time.
      value <<= 8; // shift over one byte.
      value ^= random(); // XOR in a random value.
    }
    random_table[i] = value;
  }

  table_inited = 1;
}

hash_t hash_data(const unsigned char* data, const int len)
{
  int i;
  hash_t hash;

  hash = 0;
  for( i = 0; i < len; i++ ) {
    hash = ROL_hash(hash,1);
    hash ^= random_table[data[i]];
  }
  return hash;

}
hash_t hash_string(const char* data)
{
  int i;
  hash_t hash;

  hash = 0;
  for( i = 0; data[i]; i++ ) {
    hash = ROL_hash(hash,1);
    hash ^= random_table[(unsigned char) data[i]];
  }
  return hash;
}

error_t hashmap_create(hashmap_t* h, hash_t map_size, hashmap_hash_t hash, hashmap_cmp_t cmp)
{
  memset(h, 0, sizeof(hashmap_t));
  h->size = map_size;
  h->num = 0;
  h->map = (hm_entry_t*) malloc(h->size * sizeof(hm_entry_t));
  if( ! h->map ) return ERR_MEM;
  memset(h->map, 0, h->size * sizeof(hm_entry_t));
  h->hash = hash;
  h->cmp = cmp;

  if( !table_inited ) init_table();
  return ERR_NOERR;
}

void hashmap_destroy(hashmap_t* h)
{
  free( h->map );
}

#define MAX_FULL_RATIO 0.90
#define HIGH_FULL_RATIO 0.75
#define LOW_FULL_RATIO 0.50

hash_t hashmap_size_for_elements(int num_elements)
{
  return (hash_t) (num_elements / LOW_FULL_RATIO);
}

static inline 
hash_t hashmap_increment_internal(hash_t hash, hash_t size)
{
  hash++;
  if( hash >= size ) hash -= size;
  return hash;
}

static inline
hash_t hashmap_increment(hashmap_t* h, hash_t hash)
{
  return hashmap_increment_internal(hash, h->size);
}

void hashmap_insert_basic(hash_t hash, hm_entry_t* entry, hm_entry_t* map, hash_t size)
{

  // next, figure out the nearest open slot.
  while( map[hash].key != NULL ) {
    // otherwise, we need to look in the next location.
    hash = hashmap_increment_internal(hash, size);
  }

  // place the value.
  map[hash].key = entry->key;
  map[hash].value = entry->value;

}

void hashmap_insert_internal(hashmap_t* h, hm_entry_t* entry)
{
  hash_t hash;

  // first, find the hash.
  hash = h->hash(entry->key);
  hash %= h->size;

  //printf("Inserting key %p\n", entry->key);

  hashmap_insert_basic(hash, entry, h->map, h->size);

  h->num++;
}

// inserts the entry
error_t hashmap_insert(hashmap_t* h, hm_entry_t* entry)
{
  if( (h->num+1) >= MAX_FULL_RATIO * h->size ) return ERR_FULL;

  hashmap_insert_internal(h, entry);

  return ERR_NOERR;
}

error_t hashmap_resize(hashmap_t* h)
{
  hm_entry_t* new_map;
  int new_size = 1 + (h->num / HIGH_FULL_RATIO);

  if( h->size < new_size ) { // we're past the high-water mark
    new_size = 1 + (h->num / LOW_FULL_RATIO); // make a new one at low-water mark.
    new_map = malloc(new_size * sizeof(hm_entry_t));
    if( ! new_map ) return ERR_MEM;
    memset(new_map, 0, new_size * sizeof(hm_entry_t));

    // rehash every element and store into new_map....
    // !!!
    for( hash_t i = 0; i < h->size; i++ ) {
      if( h->map[i].key != NULL ) {
        hash_t hash;

        // first, find the hash.
        hash = h->hash(h->map[i].key);
        hash %= new_size;

        hashmap_insert_basic( hash, & h->map[i], new_map, new_size);
      }
    }

    free( h->map );
    h->map = new_map;
    h->size = new_size;
  }
  
  return ERR_NOERR;
}


hm_entry_t* hashmap_retrieve_internal(hashmap_t* h, void* key)
{
  hash_t hash;

  hash = h->hash(key);
  hash %= h->size;

  // go through the run looking for it.
  while( 1 ) {
    if( h->map[hash].key == NULL ) {
      return NULL; 
    } else if( 0 == h->cmp(key, h->map[hash].key ) ) {
      return &h->map[hash];
    }
    // otherwise, move on to the next.
    hash = hashmap_increment(h, hash);
  }
}

// retrieves the entry
// If the entry is not found, entry->key == NULL on output.
// resets both key and value
int hashmap_retrieve_ptr(hashmap_t* h, void* key, void*** valuePtr)
{
  hm_entry_t* found = hashmap_retrieve_internal(h, key);
  if( found ) {
    *valuePtr = &found->value;
    return 1;
  } else {
    *valuePtr = NULL;
    return 0;
  }
}

int hashmap_retrieve(hashmap_t* h, hm_entry_t* entry)
{
  hm_entry_t* found = hashmap_retrieve_internal(h, entry->key);

  if( found ) {
    entry->key = found->key;
    entry->value = found->value;
    return 1;
  } else {
    entry->key = NULL;
    entry->value = NULL;
    return 0;
  }
}

// deletes the entry
// returns 1 if it was deleted, 0 if it wasn't found.
int hashmap_delete(hashmap_t* h, hm_entry_t* entry)
{
  hash_t hash;
  hm_entry_t temp;

  hash = h->hash(entry->key);
  hash %= h->size;

  // go through the run looking for it.
  while( 1 ) {
    if( h->map[hash].key == NULL ) {
      entry->key = NULL;
      entry->value = NULL;
      return 0;
    } else if( 0 == h->cmp(entry->key, h->map[hash].key ) ) {
      // delete that entry
      break;
    }
    // otherwise, move on to the next
    hash = hashmap_increment(h, hash);
  }

  // at the end of the loop, the entry to delete is 
  // the one at hash.
  // delete that entry.
  entry->key = h->map[hash].key;
  entry->value = h->map[hash].value;
  h->map[hash].key = NULL;
  h->map[hash].value = NULL;
  h->num--;
  hash = hashmap_increment(h, hash);
  // take out & replace entries until we're at the end of the run.
  while( h->map[hash].key != NULL ) {
    memcpy(&temp, &h->map[hash], sizeof(hm_entry_t));
    h->map[hash].key = NULL;
    h->map[hash].value = NULL;
    h->num--;
    if( 0 != h->cmp(entry->key, temp.key ) ) {
      // if it's not the same key we're deleting
      // insert it in again.
      hashmap_insert_internal(h, &temp);
    }
    hash = hashmap_increment(h, hash);
  }
  
  return 1;
}


void hm_print_strings (hm_entry_t* entry)
{
  printf("\n"
	 "The key for this is: %s\n"
	 "The value for it is: %s\n\n",
	 (char*)entry->key, (char*)entry->value);
}

hash_t hash_string_fn(const void* data)
{
  return hash_string((const char*) data);
}

int hash_string_cmp(const void* a_in, const void* b_in)
{
  const char* a = (const char*) a_in;
  const char* b = (const char*) b_in;
  return strcmp(a,b);
}

