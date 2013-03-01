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

  femto/src/utils/queue_map.h
*/
////////////////////////////////////
// PLEASE NOTE: THIS USER-DEFINED //
// DATA STRUCTURE IS NOT ACTUALLY //
//  A QUEUE; IT IS A STACK (THAT  //
//  IS: FIRST IN, LAST OUT) AND   //
//  SHOULD BE THOUGHT OF AS SUCH  //
////////////////////////////////////

#ifndef _QUEUE_MAP_H_
#define _QUEUE_MAP_H_

#include <stdlib.h>
#include "hashmap.h"

struct queue_map_value {
  struct queue_map_value* next; // linked list pointer
  void* key; // the key, duplicated here.
  void* value; // the value for the queue_map.
};

typedef struct queue_map_value queue_map_value_t;

typedef struct {
  hashmap_t hashmap;
  // in the hashmap, the key is an entry->key
  // value is a queue_map_value_t which stores a pointer and entry->value.
  queue_map_value_t* first;
} queue_map_t;

// create a queue map. Assumes that q is an allocated region for the
// queue map; this function initializes it.
error_t queue_map_create(queue_map_t* q, hashmap_hash_t hash, hashmap_cmp_t cmp);

// call queue_map_pop until the queue is empty before calling this function
// destroy a queue map; frees memory allocated in queue_map_create.
void queue_map_destroy(queue_map_t* q);

// returns the retrieved value by setting entry->key and entry->value
// return 1 if found, 0 if not
int queue_map_retrieve(queue_map_t* q, hm_entry_t* entry);

// inserts an entry, assuming it's not already present.
error_t queue_map_push(queue_map_t* q, hm_entry_t* entry);

// returns the element at the top of the list in entry->key and 
// entry->value. This element is removed from the queue_map.
// Returns 0 if no element could be returned (empty queue_map)
// or 1 if an element was returned
// 
int queue_map_pop(queue_map_t* q, hm_entry_t* entry);

#endif
