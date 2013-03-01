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

  femto/src/utils/hashmap_test.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "hashmap.h"


hash_t hash(const void* a)
{
  return ((long) a) & 0xff;
}

int cmp(const void* a, const void* b)
{
  return (long)a - (long)b;
}

void test_hashmap(void)
{
  hashmap_t hm;
  hm_entry_t ent;
  error_t err;

  err = hashmap_create(&hm, 8, hash, cmp);
  die_if_err(err);

  // put in some stuff.
  ent.key = ent.value = (void*) 0x1006;
  err = hashmap_insert(&hm, &ent);
  die_if_err(err);

  ent.key = ent.value = (void*) 0x2007;
  err = hashmap_insert(&hm, &ent);
  die_if_err(err);

  ent.key = ent.value = (void*) 0x3006;
  err = hashmap_insert(&hm, &ent);
  die_if_err(err);

  ent.key = ent.value = (void*) 0x4007;
  err = hashmap_insert(&hm, &ent);
  die_if_err(err);

  ent.key = ent.value = (void*) 0x5001;
  err = hashmap_insert(&hm, &ent);
  die_if_err(err);

  assert(hm.num == 5);

  // check the hashmap!
  assert(hm.map[0x06].key == (void*) 0x1006);
  assert(hm.map[0x07].key == (void*) 0x2007);
  assert(hm.map[0x00].key == (void*) 0x3006);
  assert(hm.map[0x01].key == (void*) 0x4007);
  assert(hm.map[0x02].key == (void*) 0x5001);

  // try retrieving elements.
  ent.key = (void*) 0x1006;
  ent.value = NULL;
  assert( 1 == hashmap_retrieve(&hm, &ent) );
  assert( ent.key == ent.value );

  ent.key = (void*) 0x2007;
  ent.value = NULL;
  assert( 1 == hashmap_retrieve(&hm, &ent) );
  assert( ent.key == ent.value );

  ent.key = (void*) 0x3006;
  ent.value = NULL;
  assert( 1 == hashmap_retrieve(&hm, &ent) );
  assert( ent.key == ent.value );

  ent.key = (void*) 0x4007;
  ent.value = NULL;
  assert( 1 == hashmap_retrieve(&hm, &ent) );
  assert( ent.key == ent.value );

  ent.key = (void*) 0x5001;
  ent.value = NULL;
  assert( 1 == hashmap_retrieve(&hm, &ent) );
  assert( ent.key == ent.value );

  ent.key = (void*) 0x5002;
  ent.value = NULL;
  assert( 0 == hashmap_retrieve(&hm, &ent) );

  assert(hm.num == 5);

  // delete an element
  ent.key = (void*) 0x1006;
  ent.value = NULL;
  assert( 1 == hashmap_delete(&hm, &ent) );

  assert(hm.num == 4);

  assert(hm.map[0x07].key == (void*) 0x2007);
  assert(hm.map[0x06].key == (void*) 0x3006);
  assert(hm.map[0x00].key == (void*) 0x4007);
  assert(hm.map[0x01].key == (void*) 0x5001);

  hashmap_destroy(&hm);
}

int main(int argc, char** argv)
{
  test_hashmap();
  printf("All hashmap tests PASSED\n");
}

