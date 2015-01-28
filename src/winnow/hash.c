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

  femto/src/winnow/hash.c
*/



#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <inttypes.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <assert.h>

#include "hash.h"

// the minimum k-gram-size or noise threshold
int K = 64;
// sequences of at least T bytes will produce a hash collision if
// they are the same
// T == W + K - 1
// or 
// W == T - K + 1
int W = 1024-64+1;
//int W = 1024;

int show_hashes = 1;
int show_offsets = 1;
int hex_offsets = 1;
int show_bytes = 1;
int output_binary = 0;

#define EXTRA_CHECKS 0

// Rolling hash information
int table_inited;
hash_t random_table[256];

void init_table(void)
{
  int i;
  unsigned int j;
  hash_t value;

  if( table_inited ) return;

  for( i = 0; i < 256; i++ ) {
    value = HASH_ZERO;
    // super-random randomizer!
    // the nice property of this code is that it supports
    // 32-bit or 64-bit random numbers. Not particularily fast.
    for( j = 0; j < 2 * sizeof(hash_t); j++ ) {
      // ask for one byte at a time.
      value = ROL_hash(value, 8); // shift over one byte
      value = XOR_hash_uint64(value, random()); // XOR in a random value.
    }
    random_table[i] = value;
  }

  table_inited = 1;
}


void print_hash(FILE* f, hash_t x)
{
  fprintf(f, "%016" PRIx64 "%016" PRIx64, x.a, x.b);
}

// bytes must be of size W+K-1
// hashes must be of size W
void print_window(long start, unsigned char *bytes, hash_t *hashes, hash_t min, long minpos)
{
  long i,j;
  unsigned char byte;

  printf("printing window starting at %li\n", start);
  for( i = 0; i < W; i++ ) {
    printf("% 3li ", start+i);
    print_hash(stdout, hashes[(start+i)%W]);
    printf(" <- ");
    for( j = 0; j < K; j++ ) {
      byte = bytes[(start + i + j)%(W+K-1)];
      if( isprint(byte) ) printf("%c", byte);
      else printf("\\x%02x", byte);
    }
    printf("\n");
  }
  printf("min hash is ");
  print_hash(stdout, min);
  printf(" at position %li\n", minpos);
}

// bytes must be of size W+K-1
void output_hash(long min_position, hash_t min_hash, unsigned char *bytes)
{
  int j;
  long max_position;
  struct hash r;
  memset(&r, 0, sizeof(struct hash));
  r.position = min_position;
  r.hash = min_hash;
  r.length = K;
  max_position = min_position + K;
  // Binary output:
  if( output_binary ) {
    fwrite(&r, sizeof(struct hash), 1, stdout);
    return;
  }
  // Text output:
  //if( EQUALS_hash(min_hash, HASH_MAX) ) return;
  //print_hash(stdout, min_hash);
  //print_hash(stdout, min_hash);
  if( show_hashes ) {
    const uint64_t mask = 0x0000ffffffffffffull;
    printf("%012" PRIx64 "%016" PRIx64, (min_hash.a & mask), min_hash.b);
  }
  if( show_offsets ) {
    if( hex_offsets ) {
      printf(" at 0x%lx <= o < 0x%lx", min_position, max_position);
    } else {
      printf(" at %li <= o < %li", min_position, max_position);
    }
  }
  if( show_bytes ) {
    printf(" ");
    for( j = 0; j < K; j++ ) {
      unsigned char byte = bytes[(min_position + j)%(W+K-1)];
      printf("%02x", byte);
    }
  }
  printf("\n");
  /*long j;
  unsigned char byte;
  //printf("%016" PRIx64 " %i\n", min_hash, min_position);
  printf("%016" PRIx64, min_hash);
  printf(" ");
  for( j = 0; j < K; j++ ) {
    byte = bytes[(min_position + j)%(W+K-1)];
    if( isprint(byte) ) printf("%c", byte);
    else printf("\\x%02x", byte);
  }
  //printf(" %li", min_position);
  printf("\n");

  if( EXTRA_CHECKS ) {
    hash_t expected = 0;
    for( j = 0; j < K; j++ ) {
      //printf("%c", data[i+j]);
      expected = hash_push(expected, bytes[(min_position + j)%(W+K-1)]);
    }
    //printf(" expected hash is %016" PRIx64 "\n", expected);
    assert(expected == min_hash);
  }*/
}

void winnow(unsigned char* data, long nbytes)
{
  long read_byte_i, pop_byte_i, next_byte_i;
  hash_t hash;
  hash_t hashes[W];
  unsigned char bytes[W+K-1];
  unsigned char byte, pop_byte;
  long i, j;
  int hashed_any;
  hash_t min_hash;
  long min_position;
  long modular_distance;

  if( ! table_inited) init_table();

  memset(bytes, 0, sizeof(bytes));

  min_hash = HASH_MAX;

  // First, copy in to bytes.

  // Do a run-up. Compute the hashes for the first W k-grams,
  // and find the minimum hash and position.
  for( i = 0; i < W; i++ ) {
    hash = HASH_ZERO;
    hashed_any = 0;
    for( j = 0; j < K && i + j < nbytes; j++ ) {
      // add bytes[i+j] to the hash.
      byte = data[i+j];
      bytes[i+j] = byte;
      hash = hash_push(hash, byte);
      hashed_any = 1;
    }
    if( !hashed_any ) hash = HASH_MAX;
    if( LESS_EQUALS_hash(hash, min_hash) ) {
      min_hash = hash;
      min_position = i;
    }
    hashes[i] = hash;
  }

  //printf("Done Wind-Up\n");
  //print_window(0, bytes, hashes, min_hash, min_position);

  output_hash(min_position, min_hash, bytes);

  for( i = W; i + K-1 < nbytes; i++ ) {
    // Considering window from j = i ; j < i+W
    // which means bytes from k = i ; k < i+W+K
    // compute the hashes...

    //printf("Before\n");
    //print_window(i-W, bytes, hashes, min_hash, min_position);

    // Update the hashes for the new byte.
    read_byte_i = i+K-1;
    pop_byte_i = i-1;
    byte = data[read_byte_i];
    pop_byte = bytes[pop_byte_i % (W+K-1)];
    next_byte_i = (read_byte_i) % (W+K-1);
    //printf("Reading byte at %i: %c; popping byte at %i: %c\n", read_byte_i, byte, pop_byte_i, pop_byte);
    hash = hashes[(i+W-1)%W];
    hash = hash_push(hash, byte);
    hash = hash_pop(hash, pop_byte, K);
    //printf("new hash is %016" PRIx64 "\n", hash);
    hashes[i%W] = hash;
    bytes[next_byte_i] = byte;
    if( EXTRA_CHECKS ) {
      hash_t expected = HASH_ZERO;
      for( j = 0; j < K; j++ ) {
        //printf("%c", data[i+j]);
        expected = hash_push(expected, data[i+j]);
      }
      //printf(" expected hash is %016" PRIx64 "\n", expected);
      assert(EQUALS_hash(expected, hash));
    }

    // For 'Robust Winnowing' - select minimum hash
    // value, but break ties by selecting the same hash as the
    // window one position to the left; if not possible, select
    // the right-most minimal hash.
    
    if( LESS_hash(hash, min_hash) ) {
      // Is the new hash less than the old minimum?
      // ... then we change the minimum.
      min_hash = hash;
      min_position = i;
      output_hash(min_position, min_hash, bytes);
    } else if( min_position < i+1-W ) {
      // Is the old minimum not in this window?
      // ... then we must recompute the minimum.
      min_hash = HASH_MAX;
      min_position = 0;
      for( j = 0; j < W; j++ ) {
        if( LESS_EQUALS_hash(hashes[j], min_hash) ) {
          min_hash = hashes[j];
          min_position = j;
        }
      }
      // Now fix min_position to be no longer modular and at or before i.
      modular_distance = (i%W) - min_position;
      modular_distance %= W;
      if( modular_distance < 0 ) modular_distance += W;
      min_position = i - modular_distance;
      output_hash(min_position, min_hash, bytes);
    }
    //printf("After\n");
    //print_window(i+1-W, bytes, hashes, min_hash, min_position);
  }
}

void test(void)
{
  hash_t a, got;
  init_table();

  a.a = 0x0001020304050607ull;
  a.b = 0x08090a0b0c0d0e0full;

  got = ROL_hash(a, 4);
  assert(got.a == 0x0010203040506070ull);
  assert(got.b == 0x8090a0b0c0d0e0f0ull);

  got = ROL_hash(a, 8);
  assert(got.a == 0x0102030405060708ull);
  assert(got.b == 0x090a0b0c0d0e0f00ull);

  got = ROL_hash(a, 12);
  assert(got.a == 0x1020304050607080ull);
  assert(got.b == 0x90a0b0c0d0e0f000ull);

  got = ROL_hash(a, 12);
  assert(got.a == 0x1020304050607080ull);
  assert(got.b == 0x90a0b0c0d0e0f000ull);

  got = ROL_hash(a, 32);
  assert(got.a == 0x0405060708090a0bull);
  assert(got.b == 0x0c0d0e0f00010203ull);

  got = ROL_hash(a, 36);
  assert(got.a == 0x405060708090a0b0ull);
  assert(got.b == 0xc0d0e0f000102030ull);

  got = ROL_hash(a, 40);
  assert(got.a == 0x05060708090a0b0cull);
  assert(got.b == 0x0d0e0f0001020304ull);

  got = ROL_hash(a, 64);
  assert(got.a == 0x08090a0b0c0d0e0full);
  assert(got.b == 0x0001020304050607ull);

  got = ROL_hash(a, 68);
  assert(got.a == 0x8090a0b0c0d0e0f0ull);
  assert(got.b == 0x0010203040506070ull);

  got = ROL_hash(a, 72);
  assert(got.a == 0x090a0b0c0d0e0f00ull);
  assert(got.b == 0x0102030405060708ull);


  //got = hash_push(a, 0x55);
  //assert(EQUALS_hash(hash_pop(got, 0x55, 0), a));
}

void usage(char* argv0) {
  printf("Usage: %s [options] file\n", argv0);
  printf(" options can include:\n"
         " --min-match-bytes <num> (also known as K)\n"
         "      the minimum number of bytes that must match\n"
         " --guarantee-match-bytes <num> (also known as T)\n"
         "      the number of bytes for which we must find a hash collision\n"
         " --show-hashes or --no-show-hashes\n"
         "      output (or don't output) the hash value (default is to output hashes)\n"
         " --show-offsets or --no-show-offsets\n"
         "      output (or don't output) the offsets value (default is to output offsets)\n"
         " --hex-offsets or --decimal-offsets\n"
         "      when outputting offsets, output them in hex or decimal\n"
         "      (implies --output-offsets)\n"
         " --show-bytes or --no-show-bytes\n"
         "      output the hashed bytes\n"
         " --output-binary or --no-output-binary\n"
         "      output binary hash records (struct hash)\n"
        );
  exit(1);
}
 
int main(int argc, char** argv)
{
  char* fname = NULL;
  struct stat stats;
  int rc;
  int fd;
  unsigned char* data;
  uint64_t data_len;
  int i;
  int T = W + K - 1;

  srandom(1);
  test();


  for( i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "--min-match-bytes") ) {
      i++;
      K = atoi(argv[i]);
    } else if( 0 == strcmp(argv[i], "--gaurantee-match-bytes") ) {
      i++;
      T = atoi(argv[i]);
    } else if( 0 == strcmp(argv[i], "--show-hashes") ) {
      show_hashes = 1;
    } else if( 0 == strcmp(argv[i], "--no-show-hashes") ) {
      show_hashes = 0;
    } else if( 0 == strcmp(argv[i], "--show-offsets") ) {
      show_offsets = 1;
    } else if( 0 == strcmp(argv[i], "--no-show-offsets") ) {
      show_offsets = 0;
    } else if( 0 == strcmp(argv[i], "--hex-offsets") ) {
      show_offsets = 1;
      hex_offsets = 1;
    } else if( 0 == strcmp(argv[i], "--decimal-offsets") ) {
      show_offsets = 1;
      hex_offsets = 0;
    } else if( 0 == strcmp(argv[i], "--show-bytes") ) {
      show_bytes = 1;
    } else if( 0 == strcmp(argv[i], "--no-show-bytes") ) {
      show_bytes = 0;
    } else if( 0 == strcmp(argv[i], "--output-binary") ) {
      output_binary = 1;
    } else if( 0 == strcmp(argv[i], "--no-output-binary") ) {
      output_binary = 0;
    } else if( 0 == strcmp(argv[i], "-h") ||
               0 == strcmp(argv[i], "--help") ) {
      usage(argv[0]);
    } else {
      // file name.
      if( fname ) usage(argv[0]);
      fname = argv[i];
    }
  }

  // Recompute window sized based on the supplied arguments.
  W = T - K + 1;

  if( K <= 0 ) {
    printf("Invalid minimum match size %i\n", W);
    exit(-1);
  }


  if( W <= 0 ) {
    printf("Invalid window size %i\n", W);
    exit(-1);
  }

  if( !fname ) usage(argv[0]);

  rc = stat(fname, &stats);
  if( rc ) {
    fprintf(stderr, "Could not stat %s\n", fname);
    return 2;
  }

  fd = open(fname, O_RDONLY);
  if( fd < 0 ) {
    fprintf(stderr, "Could not open %s\n", fname);
    return 2;
  }

  data_len = stats.st_size;
  data = mmap(NULL, data_len, PROT_READ, MAP_SHARED, fd, 0);
  if( data==NULL || data==MAP_FAILED ) {
    fprintf(stderr, "Could not mmap %s\n", fname);
    return 2;
  }
  
  winnow(data, data_len);

  return 0;
}

