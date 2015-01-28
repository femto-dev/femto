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

  femto/src/winnow/cluster_hashes.c
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
#include "bit_funcs.h"

const int MINIMUM_IN_GROUP_TO_COMBINE = 4;
const int MINIMUM_GROUP_SIZE = 8;
const int MAX_PASSES = 20;
const int MAX_SKIPS = 1;
const int MAX_LENGTH = 1024*1024*1024;

struct candidate {
  int64_t position;
  uint64_t hash;
  uint64_t next_hash;
  uint32_t old_length;
  uint32_t new_length;
  uint8_t old_nerrors;
  uint8_t new_nerrors;
  uint8_t old_nhashes;
  uint8_t new_nhashes;
  uint8_t isgap;
};

// 1. Create hashes based on winnowing algo, into an array (sorted initially by position).
// 2. Create array of candidates (creating up to 2 per (if no errors already recorded in the hash); 1 with skip, 1 without; abc produces ab and ac with 1 skip/error)
// 3. Sort candidates by hash, next_hash
// 4. Create new hashes by merging candidates with appropriate counts, taking into account the error information.
//    Still has 2 entries per position, but each entry has a score. We'll sort that out in step 6.
// 5. Sort new hashes by position.
// 6. Remove hashes that are totally subsumed or duplicates both with error and without.
//    If we have 2 hashes at the same position, keep the one with a better score (breaking ties with the no-error version)
// 7. Go to step 2.
static
int cmp_candidate_by_hashes(const void* av, const void* bv)
{
  const struct candidate* a = (const struct candidate*) av;
  const struct candidate* b = (const struct candidate*) bv;
  if( a->hash < b->hash ) return -1;
  if( a->hash > b->hash ) return 1;
  if( a->next_hash < b->next_hash ) return -1;
  if( a->next_hash > b->next_hash ) return 1;
  if( a->position < b->position ) return -1;
  if( a->position > b->position ) return 1;
  return 0;
}

static
int cmp_hash_by_position(const void* av, const void* bv)
{
  const struct hash* a = (const struct hash*) av;
  const struct hash* b = (const struct hash*) bv;
  if( a->position < b->position ) return -1;
  if( a->position > b->position ) return 1;
  return 0;
}

static
uint16_t should_combine(uint64_t n_with_this_hash, uint64_t n_with_this_next_hash)
{
  uint64_t others = (n_with_this_hash - n_with_this_next_hash)/2;
  if( n_with_this_next_hash < MINIMUM_IN_GROUP_TO_COMBINE ) return 0;
  if( n_with_this_next_hash > others ) {
    if( n_with_this_next_hash - others >= 0xffff ) return 0xffff;
    else return n_with_this_next_hash - others;
  }
  return 0;
}

uint64_t cluster(uint64_t nrows, struct hash* initial_in, struct candidate* tmp, struct hash* out, uint64_t data_len, unsigned char* data)
{
  int pass = 0;
  uint64_t i,j,k,n, start_hash, end_hash, start_next_hash, end_next_hash;
  uint64_t use_i, next_i;
  uint64_t hash, next_hash;
  uint16_t score;
  struct hash* in = initial_in;
  int combined_any;

  n = nrows;
  for( pass = 0; pass < MAX_PASSES; pass++ ) {
    printf("cluster pass  %i %" PRIi64 "/%" PRIi64 " rows\n", pass, n, nrows );
    assert( n <= nrows );
    // Create array of candidates.
    j = 0;
    for( i = 0; i < n; i++ ) {
      int hasnext = (i+1 < n);
      int hasnextnext = (i+2 < n);
      int nhashes, next_nhashes;

      //printf("in %016" PRIX64 " pos %" PRIi64 " len %i score %i errors %i hashes %i\n", in[i].hash, in[i].position, (int) in[i].length, (int) in[i].score, (int) in[i].nerrors, (int) in[i].nhashes);

      // first, output this and next
      tmp[j].position = in[i].position;
      tmp[j].hash = in[i].hash;
      tmp[j].next_hash = hasnext ? in[i+1].hash : 0;
      tmp[j].old_length = in[i].length;
      if( hasnext ) {
        tmp[j].new_length = in[i+1].length + in[i+1].position - in[i].position;
      } else {
        tmp[j].new_length = in[i].length;
      }
      tmp[j].old_nerrors = in[i].nerrors;
      tmp[j].new_nerrors = in[i].nerrors + (hasnext ? in[i+1].nerrors : 0);
      nhashes = in[i].nhashes;
      next_nhashes = (hasnext ? in[i+1].nhashes : 0);
      if( nhashes == 0 ) nhashes = 1;
      if( next_nhashes == 0 ) next_nhashes = 1;
      tmp[j].old_nhashes = nhashes;
      tmp[j].new_nhashes = nhashes + next_nhashes;
      tmp[j].isgap = 0;

      //printf("c %016" PRIX64 " %016" PRIX64 " pos %" PRIi64 " oldlen %i newlen %i oldn %i newn %i\n", tmp[j].hash, tmp[j].next_hash, tmp[j].position, (int) tmp[j].old_length, (int) tmp[j].new_length, (int) tmp[j].old_nhashes, (int) tmp[j].new_nhashes);
      j++;
      // then, output this and next next (skipping one)
      tmp[j].position = in[i].position;
      tmp[j].hash = in[i].hash;
      tmp[j].next_hash = hasnextnext ? in[i+2].hash : 0;
      tmp[j].old_length = in[i].length;
      if( hasnextnext ) {
        tmp[j].new_length = in[i+2].length + in[i+2].position - in[i].position;
      } else {
        tmp[j].new_length = in[i].length;
      }
      tmp[j].old_nerrors = in[i].nerrors;
      tmp[j].new_nerrors = 1 + in[i].nerrors + (hasnextnext ? in[i+2].nerrors : 0);
      nhashes = in[i].nhashes;
      next_nhashes = (hasnextnext ? in[i+2].nhashes : 0);
      if( nhashes == 0 ) nhashes = 1;
      if( next_nhashes == 0 ) next_nhashes = 1;
      tmp[j].old_nhashes = nhashes;
      tmp[j].new_nhashes = nhashes + next_nhashes;
      tmp[j].isgap = 1;

      //printf("c %016" PRIX64 " %016" PRIX64 " pos %" PRIi64 " oldlen %i newlen %i oldn %i newn %i\n", tmp[j].hash, tmp[j].next_hash, tmp[j].position, (int) tmp[j].old_length, (int) tmp[j].new_length, (int) tmp[j].old_nhashes, (int) tmp[j].new_nhashes);
      j++;
    }
    n = j;
    assert( n <= 2*nrows );
    // Sort candidates by hash, next hash.
    qsort(tmp, n, sizeof(struct candidate), cmp_candidate_by_hashes);
    // Create new hashes by merging candidates with appropriate counts,
    // taking into account error information.

    /*
    for( j = 0; j < n; j++ ) {
      printf("% 4li sc %016" PRIX64 " %016" PRIX64 " pos %" PRIi64 " oldlen %i newlen %i oldn %i newn %i\n", (long) j, tmp[j].hash, tmp[j].next_hash, tmp[j].position, (int) tmp[j].old_length, (int) tmp[j].new_length, (int) tmp[j].old_nhashes, (int) tmp[j].new_nhashes);
    }*/

    combined_any = 0;
    k = 0;
    start_hash = end_hash = 0;
    while( start_hash < n ) {
      start_hash = end_hash;
      hash = tmp[start_hash].hash;
      // find the first entry with a different hash
      while( end_hash < n && tmp[end_hash].hash == hash ) end_hash++;
      // OK, now we are working with a group of hashes with the same
      // initial hash, and a variety of next hashes, from [start_hash,end_hash)

      //printf(" found hash group from %li to %li with hash  %016" PRIX64 "\n", (long) start_hash, (long) end_hash, hash);
      // for each group of next hashes...
      start_next_hash = end_next_hash = start_hash;
      while ( start_next_hash < end_hash ) {
        start_next_hash = end_next_hash;
        next_hash = tmp[start_next_hash].next_hash;
        // find the first entry with a different next hash.
        while( end_next_hash < end_hash &&
               tmp[end_next_hash].next_hash == next_hash ) end_next_hash++;
        // OK, now we have a group of next hashes from [start_next_hash,end_next_hash).

        //printf(" found hash sub group from %li to %li with next_hash  %016" PRIX64 "\n", (long) start_next_hash, (long) end_next_hash, next_hash);
        // Is it worth combining?
        score = should_combine( end_hash - start_hash, end_next_hash - start_next_hash );

        for( j = start_next_hash; j < end_next_hash; j++ ) {
          assert(k < 2*nrows);

          //printf("% 4li cc %016" PRIX64 " %016" PRIX64 " pos %" PRIi64 " oldlen %i newlen %i oldn %i newn %i olderr %i newerr %i\n", (long) j, tmp[j].hash, tmp[j].next_hash, tmp[j].position, (int) tmp[j].old_length, (int) tmp[j].new_length, (int) tmp[j].old_nhashes, (int) tmp[j].new_nhashes, (int) tmp[j].old_nerrors, (int) tmp[j].new_nerrors);

          if( score > 0 && tmp[j].new_nerrors <= MAX_SKIPS && tmp[j].new_length <= MAX_LENGTH ) {
            // Output the merged rows.
            combined_any = 1;
            out[k].position = tmp[j].position;
            out[k].hash = ROL_hash(tmp[j].hash, tmp[j].new_nhashes - tmp[j].old_nhashes) ^ tmp[j].next_hash;
            out[k].nerrors = tmp[j].new_nerrors; 
            out[k].nhashes = tmp[j].new_nhashes;
            out[k].score = (score << 1) | 1;
            out[k].length = tmp[j].new_length;
            //printf("comb out %016" PRIX64 " pos %" PRIi64 " len %i score %i errors %i\n", out[k].hash, out[k].position, (int) out[k].length, (int) out[k].score, (int) out[k].nerrors);
            k++;
          } else if( ! tmp[j].isgap ) {
            // Output the non-merged original row only.
            out[k].position = tmp[j].position;
            out[k].hash = tmp[j].hash;
            out[k].nerrors = tmp[j].old_nerrors;
            out[k].nhashes = tmp[j].old_nhashes;
            if( end_hash - start_hash >= MINIMUM_GROUP_SIZE ) 
              out[k].score = (log2lli( end_hash - start_hash ) << 1) | 0;
            else
              out[k].score = 0;
            out[k].length = tmp[j].old_length;
            //printf("out %016" PRIX64 " pos %" PRIi64 " len %i score %i errors %i\n", out[k].hash, out[k].position, (int) out[k].length, (int) out[k].score, (int) out[k].nerrors);
            k++;
          }
        }
      }
    }

    //printf(" DONE MERGE\n");
    n = k;
    // Sort the new hashes by position.
    qsort(out, n, sizeof(struct hash), cmp_hash_by_position);

    /*
    for( i = 0; i < n; i++ ) {
      printf("s out %016" PRIX64 " pos %" PRIi64 " len %i score %i errors %i hashes %i\n", out[i].hash, out[i].position, (int) out[i].length, (int) out[i].score, (int) out[i].nerrors, (int) out[i].nhashes);
    }*/

 
    // Remove hashes that are totally subsumed by others
    // if we have 2 hashes at the same position, keep the one with a better score
    // or fewer errors.
    k = 0;
    for( i = 0; i < n; i = next_i) {
      use_i = i;
      next_i = i + 1;
      // handle choosing the best of two entries at the same position.
      if( i + 1 < n && out[i].position == out[i+1].position ) {
        int i_score, i_1_score;
        i_score = out[i].score;
        i_score -= out[i].nerrors;
        i_1_score = out[i+1].score;
        i_1_score -= out[i+1].nerrors;
        // handle them both now.
        if( ((i_1_score & 1) && !(i_score & 1) ) ||
            i_1_score > i_score ) use_i = i+1;
        next_i = i + 2;
      }
      // Now, look at the last entry we output.
      // Is this entry totally subsumed by it?
      // If so, we must remove it.
      if( k > 0 &&
          out[k-1].position + out[k-1].length >=
          out[use_i].position + out[use_i].length ) {
        // We are totally inside the previously output
        // element, so not output here.
      } else {
        //printf("o out %016" PRIX64 " pos %" PRIi64 " len %i score %i errors %i hashes %i\n", out[use_i].hash, out[use_i].position, (int) out[use_i].length, (int) out[use_i].score, (int) out[use_i].nerrors, (int) out[use_i].nhashes);
        if( k != use_i ) {
          out[k] = out[use_i];
        }
        k++;
      }
    }

    n = k;

    assert( n <= nrows );

    // For the next loop, use the output we just made.
    in = out;

    // Stop looping if we didn't find any to combine.
    if( combined_any == 0 ) break;
  }

  printf("Completed clustering after %i passes and returning  %" PRIi64 "/%" PRIi64 " rows\n",
         pass, n, nrows );

  in = initial_in;
  // Explain the cluster patterns in terms of hashes and in terms of data offsets.
  i = 0;
  j = 0;
  while( i < nrows && j < n ) {
    int p = 0;

    // advance past any input elements less than out[j].position.
    while( in[i].position < out[j].position ) i++;

    if( out[j].score > 0 ) p = 1;

    if( p ) printf("score %u at %" PRIi64 " %" PRIi32 " bytes { ", (unsigned int) out[j].score, out[j].position, out[j].length);

    // Scroll through any hashes in the input.
    for( ; i < nrows; i++ ) {
      if( in[i].position + in[i].length > out[j].position + out[j].length ) break;
      if( p ) printf("%016" PRIX64 " ", in[i].hash);
    }
    if( p ) printf("}");
    if( data && p ) {
      printf(" : ");
      for( k = 0; k < out[j].length; k++ ) {
        uint64_t off = out[j].position + k;
        unsigned char byte = data[off];
        if( byte == '\n' ) printf("\\n");
        else if( byte == '\\' ) printf("\\\\");
        else if( isprint(byte) ) printf("%c", byte);
        else printf("\\x%02x", (int) byte);
      }
    }
    if( p ) printf("\n");
    j++;
  }

  return n;
}

int main(int argc, char** argv)
{
  char* fname_in = NULL;
  char* fname_tmp = NULL;
  char* fname_out = NULL;
  char* fname_data = NULL;
  struct stat stats;
  int rc;
  int fd_in, fd_tmp, fd_out, fd_data;
  struct hash* data_in;
  struct candidate* data_tmp;
  struct hash* data_out;
  unsigned char* data = NULL;
  uint64_t data_len = 0;
  uint64_t n_rows;
  uint64_t got_rows;
  unsigned char zero = 0;
  ssize_t got;
  off_t off;

  if( argc < 4 ) {
    printf("Usage: %s <hashes input file> <temporary file> <output file> <data input file>\n", argv[0]);
    return 1;
  }

  fname_in = argv[1];
  fname_tmp = argv[2];
  fname_out = argv[3];
  if( argc > 4 ) fname_data = argv[4];

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
 

  // Create tmp and output files.
  fd_tmp = open(fname_tmp, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  if( fd_tmp == -1 ) { perror("Could not open tmp"); return 2; }
  rc = ftruncate(fd_tmp, 0);
  if( rc ) { perror("Could not truncate tmp"); return 2; }
  off = lseek(fd_tmp, 2 * n_rows * sizeof(struct candidate) - 1, SEEK_SET);
  if( off == -1 ) {  perror("Could not seek tmp"); return 2; }
  got = write(fd_tmp, &zero, 1);
  if( got != 1 ) { perror("Could not write to tmp"); return 2; }
  data_tmp = (struct candidate*) mmap(NULL, 2 * n_rows * sizeof(struct candidate), PROT_READ|PROT_WRITE, MAP_SHARED, fd_tmp, 0);
  if( data_tmp==NULL || data_tmp==MAP_FAILED ) {
    fprintf(stderr, "Could not mmap %s\n", fname_tmp);
    perror("Could not mmap tmp");
    return 2;
  }
 
  fd_out = open(fname_out, O_RDWR|O_CREAT, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP);
  if( fd_out == -1 ) { perror("Could not open out"); return 2; }
  rc = ftruncate(fd_out, 0);
  if( rc ) { perror("Could not truncate out"); return 2; }
  off = lseek(fd_out, 2 * n_rows * sizeof(struct hash) - 1, SEEK_SET);
  if( off == -1 ) {  perror("Could not seek out"); return 2; }
  got = write(fd_out, &zero, 1);
  if( got != 1 ) { perror("Could not write to out"); return 2; }
  data_out = (struct hash*) mmap(NULL, 2 * n_rows * sizeof(struct hash), PROT_READ|PROT_WRITE, MAP_SHARED, fd_out, 0);
  if( data_out==NULL || data_out==MAP_FAILED ) {
    fprintf(stderr, "Could not mmap %s\n", fname_out);
    perror("Could not mmap out");
    return 2;
  }
 
  if( fname_data ) {
    rc = stat(fname_data, &stats);
    if( rc ) {
      fprintf(stderr, "Could not stat %s\n", fname_data);
      perror("Could not stat");
      return 2;
    }
    data_len = stats.st_size;

    fd_data = open(fname_data, O_RDONLY);
    if( fd_data == -1 ) { perror("Could not open data input"); return 2; }
    data = (unsigned char*) mmap(NULL, 2 * data_len, PROT_READ, MAP_SHARED, fd_data, 0);
    if( data==NULL || data==MAP_FAILED ) {
      fprintf(stderr, "Could not mmap %s\n", fname_data);
      perror("Could not mmap out");
      return 2;
    }
  }
 
  got_rows = cluster(n_rows, data_in, data_tmp, data_out, data_len, data);

  rc = munmap(data_in, n_rows * sizeof(struct hash));
  if( rc ) { perror("Could not munmap input"); return 2; }
  munmap(data_tmp, 2 * n_rows * sizeof(struct candidate));
  if( rc ) { perror("Could not munmap tmp"); return 2; }
  munmap(data_out, 2 * n_rows * sizeof(struct hash));
  if( rc ) { perror("Could not munmap output"); return 2; }

  rc = ftruncate(fd_out, got_rows * sizeof(struct hash));
  if( rc ) { perror("Could not truncate output"); return 2; }

  rc = close(fd_in);
  if( rc ) { perror("Could not close input"); return 2; }
  rc = close(fd_tmp);
  if( rc ) { perror("Could not close tmp"); return 2; }
  rc = close(fd_out);
  if( rc ) { perror("Could not close output"); return 2; }

  return 0;
}


