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

  femto/src/utils/cache_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "cache.h"

int verbose = 0;
int faults;
int evicts; 

error_t fault(cache_id_t cid, int data_size, void* data, void* context)
{
  int neg = -1;
  int id = numeric_cache_num_for_id(cid);
  if( verbose ) printf("Fault: %i %p\n", id, data);
  assert(0 == memcmp(data, &neg, sizeof(int)));
  memcpy(data, &id, sizeof(int));
  assert(context == (void*) 0x1);
  faults++;
  return ERR_NOERR;
}

void evict(cache_id_t cid, int data_size, void* data, void* context)
{
  int neg = -1;
  int id = numeric_cache_num_for_id(cid);
  if( verbose ) printf("-Evict: %i %p\n", id, data);
  assert(context == (void*) 0x1);
  assert(0 == memcmp(data, &id, sizeof(int)));
  memcpy(data, &neg, sizeof(int));
  evicts++;
}

void cache_check(cache_t* c)
{
  cache_list_entry_t* le;
  int li;
  int z;
  // test the cache data structures.
  // check that the list still is the entire length.
  z = 0;
  li = c->head;
  while( li != -1 ) {
    le = &c->list[li];
    z++;
    li = le->next;
  }
  assert(z == c->cache_size);


  z = 0;
  li = c->tail;
  while( li != -1 ) {
    le = &c->list[li];
    z++;
    li = le->prev;
  }
  assert(z == c->cache_size);
}

void test_get(cache_t* c, int id)
{
  error_t err;
  void* data;

  cache_check(c);
  data = NULL;
  NUMERIC_CACHE_GET(err, c, id, data);
  //err = cache_get(c, id, &data);
  assert(!err);
  assert(0==memcmp(data, &id, sizeof(int)));
  if( verbose ) printf("GOT: %i %p\n", id, data);
  cache_check(c);
}


int main(int argc, char** argv)
{
  error_t err;
  cache_t c;
  // test cache.
  err = numeric_cache_create(&c, 4, 16, (void*)1, fault, evict);
  assert(!err);
  faults = evicts = 0;

  memset(c.data, 0xff, 16*4); // we check that the data was evicted
                              // so we have to set it first.

  test_get(&c, 0);
  test_get(&c, 1);
  test_get(&c, 2);
  test_get(&c, 3);
  test_get(&c, 4);
  test_get(&c, 5);
  test_get(&c, 6);
  test_get(&c, 7);
  test_get(&c, 8);
  test_get(&c, 9);
  test_get(&c, 2);
  test_get(&c, 2);
  test_get(&c, 4);
  test_get(&c, 3);
  test_get(&c, 9);
  test_get(&c, 1);
  test_get(&c, 2);
  test_get(&c, 7);
  test_get(&c, 3);
  test_get(&c, 5);
  test_get(&c, 6);
  test_get(&c, 8);
  test_get(&c, 0);
  test_get(&c, 2);
  test_get(&c, 1);
  test_get(&c, 3);
  test_get(&c, 2);
  test_get(&c, 1);
  test_get(&c, 2);
  test_get(&c, 0);
  test_get(&c, 2);
  test_get(&c, 3);
  test_get(&c, 2);

  cache_destroy(&c);

  err = numeric_cache_create(&c, 2, 16, (void*)1, fault, evict);
  assert(!err);
  faults = evicts = 0;
  memset(c.data, 0xff, 16*2); // we check that the data was evicted
                              // so we have to set it first.
  test_get(&c, 2);
  test_get(&c, 3);
  test_get(&c, 2);
  test_get(&c, 2);
  test_get(&c, 3);

  assert(faults == 2);
  cache_destroy(&c);


  err = numeric_cache_create(&c, 1, 16, (void*)1, fault, evict);
  assert(!err);
  faults = evicts = 0;
  memset(c.data, 0xff, 16*1); // we check that the data was evicted
                              // so we have to set it first.
  test_get(&c, 2);
  test_get(&c, 2);
  test_get(&c, 2);
  assert(faults == 1);
  cache_destroy(&c);

  printf("Cache test PASSED\n");
  return 0;
}
