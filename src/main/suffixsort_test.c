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

  femto/src/main/suffixsort_test.c
*/
#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include "timing.h"
#include "suffixsort.h"

int main(int argc, char** argv)
{
  double one_gb = 1024*1024*1024;
  double num_gb;
  int size;
  int* T;
  int* SA;
  FILE* f;
  int c, j;
  error_t err;

  if( argc == 1 ) {
    printf("Usage: %s <num_gb> [input_file] [input_file] ...\n", argv[0]);
    return -1;
  }
  sscanf(argv[1], "%lf", &num_gb);
  printf("Will run test with %lf GB\n", num_gb);

  size = num_gb * one_gb;

  printf("Allocating memory (size=%i)\n", size);
  T = malloc((size+4)*sizeof(int));
  assert(T);
  SA = malloc(size*sizeof(int));
  assert(SA);

  // read in some input files.
  j = 0;
  for( int i = 2; i < argc && j < size; i++ ) {
    printf("Reading %s\n", argv[i]);
    f = fopen(argv[i], "r");
    assert(f);
    while( EOF != (c = fgetc(f)) &&
           j < size ) {
      T[j++] = c;
    }
    fclose(f);
  }
  if( j < size ) {
    // make some random data
    printf("Making random data\n");
    for( int i = j; i < size; i++ ) {
      T[i] = 1 + (rand() & 0xff);
    }
  }

  printf("Suffix sorting\n");
  // sort it.
  start_clock();
  err = suffixArray(T, SA, size, 256);
  die_if_err(err);
  stop_clock();
  print_timings("Suffix Sort Bytes", size);

  return 0;
}

