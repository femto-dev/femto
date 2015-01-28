/*
  (*) 2014-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/winnow/dump_hashes.c
*/



#define _FILE_OFFSET_BITS 64
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <inttypes.h>

#include "hash.h"


int main(int argc, char** argv)
{
  char* fname_in = NULL;
  struct stat stats;
  int rc;
  int fd_in;
  struct hash* data_in;
  uint64_t i;
  uint64_t n_rows;
  uint64_t got_rows;
  unsigned char zero = 0;
  ssize_t got;
  off_t off;

  if( argc != 2 ) {
    printf("Usage: %s <input file>\n", argv[0]);
    return 1;
  }

  fname_in = argv[1];

  rc = stat(fname_in, &stats);
  if( rc ) {
    fprintf(stderr, "Could not stat %s\n", fname_in);
    perror("Could not stat");
    return 2;
  }

  fd_in = open(fname_in, O_RDONLY);
  if( fd_in < 0 ) {
    fprintf(stderr, "Could not open %s\n", fname_in);
    perror("Could not open");
    return 2;
  }

  n_rows = stats.st_size / sizeof(struct hash);
  if( n_rows * sizeof(struct hash) != stats.st_size ) {
    fprintf(stderr, "Uneven input length - wrong format?\n");
    return 2;
  }
  if( n_rows == 0 ) {
    fprintf(stderr, "No input\n");
    return 2;
  }

  data_in = (struct hash*) mmap(NULL, n_rows * sizeof(struct hash), PROT_READ, MAP_SHARED, fd_in, 0);
  if( data_in==NULL || data_in==MAP_FAILED ) {
    fprintf(stderr, "Could not mmap %s\n", fname_in);
    perror("Could not mmap input");
    return 2;
  }
 

  for( i = 0; i < n_rows; i++ ) {
    printf("%016" PRIX64 " pos %" PRIi64 " len %" PRIi32 "\n",
           data_in[i].hash, data_in[i].position, data_in[i].length);
  }

  rc = munmap(data_in, n_rows * sizeof(struct hash));
  if( rc ) { perror("Could not munmap input"); return 2; }
  rc = close(fd_in);
  if( rc ) { perror("Could not close input"); return 2; }

  return 0;
}


