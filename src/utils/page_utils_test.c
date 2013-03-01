/*
  (*) 2007-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/page_utils_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "page_utils.h"

int main(int argc, char** argv)
{
  int meg = 1024*1024;
  size_t page_size = get_page_size();
  size_t size = 100*meg;
  size_t i;
  error_t err;

  printf("Page size is %li\n", (long int) page_size);

  {
    struct allocated_pages a[20];
    size_t num = 20;
    size_t sz = 16*page_size;

    for( i = 0; i < num; i++ ) {
      a[i] = allocate_pages(sz);
    }

    printf("sz is %x\n", (int) sz);

    for( i = 0; i < num; i++ ) {
      printf("a[%02i] %p\n", (int) i, a[i].data);
    }

    for( i = 0; i < num; i++ ) {
      free_pages(a[i]);
    }
  }


  struct allocated_pages mem;
  mem = allocate_pages_mmap(size);

  assert(mem.data && mem.data != (char*) -1);
  memset(mem.data, 1, size);

  // Go through the malloced region dropping the pages.
  for( i = 0; i < size; i+= 2*page_size) {
    err = drop_pages(mem.data+i, 2*page_size);
    die_if_err(err);
  }

  free_pages_mmap(mem);

  printf("All tests PASSED\n");
  return 0;
}
