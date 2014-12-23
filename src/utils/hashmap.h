/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/hashmap.h
*/
#ifndef _HASHMAP_H_
#define _HASHMAP_H_

#include "error.h"

typedef unsigned int hash_t;
#define ROL_hash rol32

typedef struct {
  void* key;
  void* value;
} hm_entry_t;

/** hash_t is a function type that returns a hash code from a void pointer.
  */
typedef hash_t (*hashmap_hash_t)(const void*);
/** Returns 0 if they are equal, -1 if a < b, 1 otherwise
  */
typedef int (*hashmap_cmp_t)(const void*, const void*);

typedef struct {
  hash_t size; // size of the hashmap
  hash_t num; // number of entries
  hm_entry_t* map;
  hashmap_hash_t hash;
  hashmap_cmp_t cmp;
} hashmap_t;

/**
  create a hashmap. map_size is the size of the hashmap
  (until hashmap_resize is called), hash is the hash function,
  cmp is the comparison function.
  */
error_t hashmap_create(hashmap_t* h, hash_t map_size, hashmap_hash_t hash, hashmap_cmp_t cmp);
/**
  free the memory allocated by hashmap_create.
  */
void hashmap_destroy(hashmap_t* h);

/**
  returns the suggested of the hashmap given the target number of elements.
  */
hash_t hashmap_size_for_elements(int num_elements);

/**
  resizes the hashmap, so that it has room for new entries, if it
  is necessary. There will always be room for one more element.
  */
error_t hashmap_resize(hashmap_t* h);

/**
  inserts the entry passed by entry->key and entry->value.
  Assumes that there is no entry in the hashmap already with that key.
*/
error_t hashmap_insert(hashmap_t* h, hm_entry_t* entry);
/**
  retrieves the entry; returns 1 if it was found, 0 otherwise
  entry->key and entry->value will be set to the values from the hashmap.
  */
int hashmap_retrieve(hashmap_t* h, hm_entry_t* entry);
/**
  retrieves the entry; returns 1 if it was found, 0 otherwise
  *valuePtr will be set to, on output, &table_entry.value.
  This pointer is valid until the next hashtable operation.
  */
int hashmap_retrieve_ptr(hashmap_t* h, void* key, void*** valuePtr);

/**
  deletes an entry with key entry->key from the hashmap.
  Sets entry->key and entry->value to the key and value for
  the deleted hashmap.
  */
int hashmap_delete(hashmap_t* h, hm_entry_t* entry);


/**
  Compute the hash of a string or any other generic data
  using a table.
  */
hash_t hash_data(const unsigned char* data, const int len);
/**
  Compute the hash of a NULL-terminated C string using a table.
  */
hash_t hash_string(const char* data);

void hm_print_strings (hm_entry_t* entry);

// These can be used directly... they assume the key is a char*
hash_t hash_string_fn(const void* data);
int hash_string_cmp(const void* a, const void* b);

#endif
