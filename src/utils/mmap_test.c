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

  femto/src/utils/mmap_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <assert.h>

int main(int argc, char** argv)
{
  FILE* f = tmpfile();
  unsigned char* data;

  assert( f );
  printf("Saving some chars with fputc.\n");

  for( int i = 0; i < 10; i++ ) {
    fputc(i, f);
  }
  fflush(f);

  printf("MMapping the tmp file\n");
  // now mmap the file.
  data = mmap(NULL, 10, PROT_READ|PROT_WRITE, MAP_SHARED, fileno(f), 0);
  assert(data && data != MAP_FAILED);

  printf("Updating mmaped region\n");
  data[0] = 100;
  data[5] = 105;

  printf("Saving some chars with fputc.\n");
  for( int i = 10; i < 100; i++ ) {
    fputc(i, f);
  }

  printf("Updating mmaped region\n");
  data[7] = 107;

  printf("MUnmapping the file\n");
  munmap(data, 10);

  printf("Checking the file\n");
  // check the file.
  rewind(f);

  for( int i = 0; i < 100; i++ ) {
    int ch = fgetc(f);
    if( i == 0 ) assert( ch == 100 );
    else if ( i == 5 ) assert( ch == 105 );
    else if ( i == 7 ) assert( ch == 107 );
    else assert( ch == i );
  }

  return 0;
}
