/*
  (*) 2009-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/random_read_test.c
*/
/* Compile with
gcc -o random_access random_access.c  -Wall -O3 -laio
*/

/*
 * Some tests with FusionIO
ext2 mmap reading a file; 1 Mi random reads
$ time ./random_access

real    0m54.441s
user    0m0.576s
sys     0m26.863s


mmap reading directly from the block device
# time ./random_access

real    0m44.962s
user    0m0.513s
sys     0m20.670s

reading directly from the block device with Linux Kernel AIO:
# time ./random_access 

real    0m15.445s
user    0m0.044s
sys     0m5.703s

# time ./random_access 
Read 1048576 bytes; sum is 128270972 and bitmask is 255

real    0m37.726s
user    0m0.054s
sys     0m12.729s

# time ./random_access 
Read 1048576 bytes; sum is 128270972 and bitmask is 255

real    0m25.543s
user    0m0.034s
sys     0m8.894s

# time ./random_access 
Read 1048576 bytes; sum is 128270972 and bitmask is 255

real    0m20.675s
user    0m0.059s
sys     0m7.465s





reading from a file with kernel aio:
# time ./random_access 
Read 1048576 bytes; sum is 133746096 and bitmask is 255

real    0m40.374s
user    0m0.048s
sys     0m14.211s
# time ./random_access 
Read 1048576 bytes; sum is 133746096 and bitmask is 255

real    0m41.218s
user    0m0.051s
sys     0m14.270s

# time ./random_access 
Read 1048576 bytes; sum is 133746096 and bitmask is 255

real    0m18.723s
user    0m0.049s
sys     0m6.823s

# time ./random_access 
Read 1048576 bytes; sum is 133746096 and bitmask is 255

real    0m2.601s
user    0m0.041s
sys     0m1.710s


O_DIRECT didn't work because I'm only reading 1 byte.

I think the kernel is caching all of the relevant pages,
  so I up number of requests to 32 Mi.

ext2 kernel aio test:
-- enabled O_DIRECT and read 512b (used only 1b)
# time ./random_access 
Read 33554432 bytes; sum is 4278698453 and bitmask is 255

real    11m25.489s
user    0m1.618s
sys     4m6.412s
-- this is 20 us per read

block device kernel aio test
-- no O_DIRECT, read 512b
# time ./random_access 
Read 33554432 bytes; sum is 4103791035 and bitmask is 255

real    21m41.075s
user    0m1.592s
sys     7m30.255s

block device kernel ai test, with O_DIRECT
 -- is this an anomaly? Not sure we're reading everything
    that we're supposed to.
# time ./random_access 
Read 33554432 bytes; sum is 27568863 and bitmask is 255

real    0m24.328s
user    0m1.168s
sys     0m22.143s

*/


// allow O_DIRECT
#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>

#ifdef HAS_LINUX_LIBAIO
#include <libaio.h>
#endif

#include <unistd.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "timing.h"

#define WRITE_BUF_SIZE (100*1024*1024)

unsigned char* expect_buf;
unsigned char* got_buf;

long next_record_idx(long records_total)
{
  unsigned long ret;
  ret = rand();
  ret <<= 24;
  ret ^= rand();
  ret %= records_total;
  return ret;
}

void record_at_idx(long record_idx, long bytes_per_record, unsigned char* out) 
{
  int k=0;
  for( long i = 0; i < bytes_per_record; i++ ) {
    out[i] = record_idx >> (7*k);
    k++;
    if( k >= 8*sizeof(long) ) k %= 8*sizeof(long);
  }
}

// dies if it doesn't check.
void check_num(unsigned char* buf, long record_idx, long bytes_per_record)
{
  int got;
  long i;

  // uses global variable expect_buf
  
  // get the record we expect into expect_buf 
  record_at_idx(record_idx, bytes_per_record, expect_buf);

  got = memcmp(buf, expect_buf, bytes_per_record);
  if( got != 0 ) {
    printf("check_num idx=%#lx failing;\nexpect \t\tgot\n", record_idx);
    for( i = 0; i < bytes_per_record; i++ ) {
      printf("%x \t\t%x\n", expect_buf[i], buf[i]);
    }
    assert(got == 0);
  }
}

void produce_file(char* to, long bytes_per_record, long nbytes_total)
{
  FILE* f;
  long records_total = nbytes_total / bytes_per_record;
  long i,j;
  //long extra_space=4*1024;
  long buf_sz = WRITE_BUF_SIZE;
  long buf_sz_records = buf_sz / bytes_per_record;
  static unsigned char buf[WRITE_BUF_SIZE];
  long num;
  size_t wrote;

  f = fopen(to, "w");
  assert(f);

  assert(bytes_per_record < WRITE_BUF_SIZE);

  printf("Producing %li-byte file with %li-byte records\n", nbytes_total, bytes_per_record);
  start_clock();

  for( i = 0; i < records_total;) {
    num = buf_sz_records;
    if( i + num > records_total ) num = records_total - i;

    for( j = 0; j < num; j++ ) {
      record_at_idx(i, bytes_per_record, buf+j*bytes_per_record);
      i++;
    }

    wrote = fwrite(buf, bytes_per_record, num, f);
    assert(wrote==num);
  }

  stop_clock();
  print_timings("fwrite record writes", records_total);

  // Add some extra space to the end of the file.
  /*for( i = 0; i < extra_space; i++ ) {
    fputc(0, f);
  }*/

  fclose(f);
}

void fread_random_read_test(char* path,
                            long bytes_per_record,
                            long nbytes_total,
                            long num_rand_reads)
{
  long i;
  FILE* f;
  long record_idx;
  long records_total = nbytes_total / bytes_per_record;
  int rc;

  printf("Doing %li fread random reads with %li-byte records\n",
         num_rand_reads, bytes_per_record);

  f = fopen(path, "r");
  assert(f);

  start_clock();

  for(i = 0; i < num_rand_reads; i++ ) {
    record_idx = next_record_idx(records_total);
    rc = fseek(f, bytes_per_record*record_idx, SEEK_SET);
    assert(rc==0);
    rc = fread(got_buf, bytes_per_record, 1, f);
    assert(rc==1);
    check_num( got_buf, record_idx, bytes_per_record );
  }

  stop_clock();
  print_timings("fread random reads", num_rand_reads);

  rc = fclose(f);
  assert(rc == 0);
}


void mmap_random_read_test(char* path,
                           long bytes_per_record,
                           long nbytes_total,
                           long num_rand_reads)
{
  long i;
  int fd;
  unsigned char* big;
  long record_idx;
  long records_total = nbytes_total / bytes_per_record;
  int rc;

  printf("Doing %li mmap random reads if %li-byte records\n",
         num_rand_reads, bytes_per_record);

  fd = open(path, O_RDONLY);
  assert(fd>=0);
  big = mmap(NULL, nbytes_total, PROT_READ, MAP_SHARED, fd, 0);
  assert(big && big != MAP_FAILED);
  assert(fd>=0);
  
  madvise(0, nbytes_total, MADV_RANDOM);

  start_clock();

  for(i = 0; i < num_rand_reads; i++ ) {
    record_idx = next_record_idx(records_total);
    check_num( &big[bytes_per_record*record_idx], record_idx, bytes_per_record );
  }

  stop_clock();
  print_timings("mmap random reads", num_rand_reads);

  rc = munmap(big, nbytes_total);
  assert(rc == 0);

  rc = close(fd);
  assert(rc == 0);
}

#ifdef HAS_LINUX_LIBAIO
void linux_aio_random_read_test(char* path,
                                long bytes_per_record,
                                long nbytes_total,
                                long num_rand_reads,
                                long read_size,
                                long n_events,
                                int direct)
{
  long i;
  long j;
  long records_total = nbytes_total / bytes_per_record;
  int fd;
  struct iocb* cb[n_events];
  struct io_event evt[n_events];
  unsigned char* bufs[n_events];
  long record_idxs[n_events];
  long block_idx;
  long offset_within_block[n_events];
  long block_size = 512;
  long offset;
  io_context_t ctx;
  int err;
  int flags;
  long n_clumps = num_rand_reads / n_events;

  printf("Doing %li linux_aio random reads if %li-byte records\n"
         "read_size=%li n_events=%li direct=%i\n",
         num_rand_reads, bytes_per_record,
         read_size, n_events, direct);

  flags = O_RDONLY;
  if( direct ) {
    flags |= O_DIRECT;
    if( read_size < block_size ) {
      printf("Rounding up read size to a %li-byte block for O_DIRECT\n", block_size);
      read_size = block_size;
    }
    assert(0 == (read_size % block_size));
    assert( bytes_per_record < block_size );
  }

  fd = open(path, flags);
  assert(fd>=0);

  memset(&ctx, 0, sizeof(ctx));
  err = io_setup(n_events, &ctx);
  assert(!err);

  for( j = 0; j < n_events; j++ ) {
    cb[j] = malloc(sizeof(struct iocb));
    bufs[j] = valloc(read_size);
  }

  start_clock();

  for( i = 0; i < n_clumps; i++ ) {

    for( j = 0 ; j < n_events; j++ ) {
      record_idxs[j] = next_record_idx(records_total);
      offset = bytes_per_record*record_idxs[j];
      if( direct ) {
        block_idx = offset/block_size;
        offset_within_block[j] = offset - block_size*block_idx;
        io_prep_pread(cb[j], fd, bufs[j], read_size,
                      block_size * block_idx);
      } else {
        io_prep_pread(cb[j], fd, bufs[j], read_size,
                      bytes_per_record*record_idxs[j]);
      }
    }

    err = io_submit(ctx, n_events, cb);
    assert(err == n_events);

    err = io_getevents(ctx, n_events, n_events, evt, NULL);
    assert(err == n_events);

    for( j = 0; j < n_events; j++ ) {
      assert( evt[j].res2 == 0 );
      if( direct ) {
        offset = offset_within_block[j];
        check_num(&bufs[j][offset], record_idxs[j], bytes_per_record);
      } else {
        check_num(bufs[j], record_idxs[j], bytes_per_record);
      }
    }
  }
  stop_clock();
  print_timings("linux aio random reads", num_rand_reads);


  err = io_destroy(ctx);
  assert(!err);
}
#endif


int main(int argc, char** argv)
{
  long gibi = 1024*1024*1024;
  long num_rand_reads = 32*1024*1024;
  long bytes_per_record;
  long nbytes_total;
  char* path;
  int write_file = 1;
  int i = 1;
  int seed = time(NULL);

  if( argc < 5 ) {
    printf("Usage: %s [--reuse] path file-size-gibibytes num-rand-reads bytes-per-record\n", argv[0]);
    printf("   e.g %s file 16 32 100000\n", argv[0]);
    return -1;
  }

  if( 0 == strcmp(argv[i], "--reuse") ) {
    i++;
    write_file = 0;
  }

  path = argv[i++];
  nbytes_total = atof(argv[i++])*gibi;
  num_rand_reads = atof(argv[i++]);
  bytes_per_record = atol(argv[i++]);

  assert(path);
  assert(bytes_per_record > 0);

  expect_buf = malloc(bytes_per_record);
  got_buf = malloc(bytes_per_record);

  // First, create the file.
  if( write_file ) {
    produce_file(path, bytes_per_record, nbytes_total);
  }

  printf("Seeding RNG with %i\n", seed);

  // Now test fread random reads.
  fread_random_read_test(path, bytes_per_record, nbytes_total, num_rand_reads);

  // Now test mmap random reads.
  mmap_random_read_test(path, bytes_per_record, nbytes_total, num_rand_reads);

#ifdef HAS_LINUX_LIBAIO
  {
    int n_events = 128;
    int read_size = bytes_per_record;
    // Now test linux aio random reads without o_direct.
    linux_aio_random_read_test(path, bytes_per_record, nbytes_total,
                               num_rand_reads, read_size, n_events, 0);

    // Now test linux aio random reads with o_direct.
    linux_aio_random_read_test(path, bytes_per_record, nbytes_total,
                               num_rand_reads, read_size, n_events, 1);
  }
#endif

  return 0;
}


