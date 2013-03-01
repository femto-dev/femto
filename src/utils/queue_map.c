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

  femto/src/utils/queue_map.c
*/
////////////////////////////////////
// PLEASE NOTE: THIS USER-DEFINED //
// DATA STRUCTURE IS NOT ACTUALLY //
//  A QUEUE; IT IS A STACK (THAT  //
//  IS: FIRST IN, LAST OUT) AND   //
//  SHOULD BE THOUGHT OF AS SUCH  //
////////////////////////////////////
#include "queue_map.h"

// create a queue map. Assumes that q is an allocated region for the
// queue map; this function initializes it.
error_t queue_map_create(queue_map_t* q, hashmap_hash_t hash, hashmap_cmp_t cmp)
{
  q->first = NULL;
  return (hashmap_create( &q->hashmap, 16, hash, cmp ));
}

// destroy a queue map; frees memory allocated in queue_map_create.
void queue_map_destroy(queue_map_t* q)
{
  hashmap_destroy ( &q->hashmap );  
}

// returns the retrieved value by setting entry->key and entry->value
// return 1 if found, 0 if not
int queue_map_retrieve(queue_map_t* q, hm_entry_t* entry)
{  
  // if the key is found in the hashmap
  if (hashmap_retrieve( &q->hashmap, entry )) {
    
    // set a queue_map_value equal to entry's value
    queue_map_value_t *value = entry-> value;
    //queue_map_value_t *value;
    

    // retrieve the value stored and make
    // entry->value equal to the value stored
    entry-> value = value-> value;
    
    return 1;
  }

  // was not found
  return 0;
}


// inserts an entry, assuming it's not already present.
error_t queue_map_push(queue_map_t* q, hm_entry_t* entry)
{
  error_t error;
  
  // make a copy of 'entry'
  hm_entry_t entry2 = *entry;
  
  // if it's not already present [ERROR CHECKING]
  if (! (hashmap_retrieve( &q->hashmap, &entry2 ))) {
    // allocate memory for it
    queue_map_value_t *value = malloc( sizeof(queue_map_value_t) );

    // malloc failed
    if (!value) { return ERR_MEM; }
    
    // resize the hashmap if it's not big enough
    error = hashmap_resize (&q->hashmap);
    
    if (error != ERR_NOERR ) {
      return error;
    }
    
    // assign value's variables
    // so it's equal to entry
    value-> key   = entry->key;
    value-> value = entry->value;
    
    // have it point to the next entry,
    // previously pointed to by first
    value->next = q->first;

    // reassign first to point to the inserted entry    
    q->first = value;

    // so the SUN won't complain
    {
      // create a new variable
      hm_entry_t temp;

      // assign it the proper values
      temp.key   = entry-> key;
      temp.value = value;
      
      // insert 'value' into the hash table
      error = hashmap_insert (&q->hashmap, &temp);

      // check for errors and return
      if (error) {   return error; }
    }
      
    // no error
    return ERR_NOERR;
  }
  
  return ERR_PARAM;
}

// returns the element at the top of the list in entry->key and 
// entry->value. This element is removed from the queue_map.
// Returns 0 if no element could be returned (empty queue_map)
// or 1 if an element was returned
// 
int queue_map_pop(queue_map_t* q, hm_entry_t* entry)
{
  // switching variable
  queue_map_value_t *temp;
  
  //check for an empty queue_map
  if (q->first == NULL) {
    // queue_map empty
    return 0;
  }

  // temp = entry to be popped
  temp = q->first;

  // assign first to point to next entry
  q->first = temp->next;

  // copy 'key' value to 'entry'
  entry->key = temp->key;
  // make value NULL so the delete works
  entry->value = NULL;
  
  // remove it from the hashmap
  if (hashmap_delete(&q->hashmap, entry) == 0) {
    return 0;
  }

  // copy 'value' value to 'entry'
  entry->value = temp->value;
  
  // deallocate the memory
  free(temp);
  
  // queue_map not empty
  return 1;
}
