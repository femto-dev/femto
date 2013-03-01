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

  femto/src/utils/buffer_test.c
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


#include "buffer_funcs.h"

#define TG_DATA_SIZE 4096
unsigned char tg_data[TG_DATA_SIZE];

void test_numeric(void)
{
  // test cieldiv
  assert(2 == CEILDIV(3,2));
  assert(1 == CEILDIV(2,2));
  assert(0 == CEILDIV(0,2));

  // test num_bits
  assert(leadz32(0) == 32);
  assert(num_bits32(0) == 1);
  assert(num_bits32(1) == 1);
  assert(num_bits32(2) == 2);
  assert(num_bits32(3) == 2);
  assert(num_bits32(4) == 3);
  assert(num_bits32(5) == 3);
  assert(num_bits32(6) == 3);
  assert(num_bits32(7) == 3);
  assert(num_bits32(0x46) == 7);
  assert(num_bits32(255) == 8);
  assert(num_bits32(256) == 9);

  assert(leadz64(0) == 64);
  assert(num_bits64(0) == 1);
  assert(num_bits64(1) == 1);
  assert(num_bits64(2) == 2);
  assert(num_bits64(3) == 2);
  assert(num_bits64(4) == 3);
  assert(num_bits64(5) == 3);
  assert(num_bits64(6) == 3);
  assert(num_bits64(7) == 3);
  assert(num_bits64(0x46) == 7);
  assert(num_bits64(255) == 8);
  assert(num_bits64(256) == 9);


}

void test_bitbuffer(void)
{
  // test bitbuffer output
  buffer_t b = build_buffer(TG_DATA_SIZE, tg_data);
  int i;

  bsInitWrite( & b );
  bsW( &b, 1, 1 );
  bsW( &b, 1, 0 );
  bsW( &b, 1, 0 );
  bsW( &b, 1, 1 );
  bsW( &b, 1, 1 );
  bsW( &b, 1, 1 );
  bsW( &b, 1, 0 );
  bsFinishWrite( & b );
  assert(b.len == 1 );
  assert(b.data[0] == 0x9c );

  bsInitWrite( & b );
  bsW ( &b, 7,  0x12 );
  bsW ( &b, 11, 0x07e );
  bsW ( &b, 1,  0x1 );
  bsW ( &b, 16, 0x1234 );
  bsW ( &b, 16, 0x5678 );

  bsFinishWrite( & b );

  assert(b.data[0] == 0x9c );
  assert(b.data[1] == 0x24 );
  assert(b.data[2] == 0x1f );
  assert(b.data[3] == 0xa2 );
  assert(b.data[4] == 0x46 );
  assert(b.data[5] == 0x8a );
  assert(b.data[6] == 0xcf );
  assert(b.data[7] == 0x00 );

  bsInitWrite( & b );
  bsPutUInt32 ( &b, 0xdeadbeef );
  bsPutUInt64 ( &b, 0xffff7eeff00fd00d );
  bsFinishWrite( & b );

  assert(b.data[8]  == 0xde );
  assert(b.data[9]  == 0xad );
  assert(b.data[10] == 0xbe );
  assert(b.data[11] == 0xef );

  assert(b.data[12] == 0xff );
  assert(b.data[13] == 0xff );
  assert(b.data[14] == 0x7e );
  assert(b.data[15] == 0xef );
  assert(b.data[16] == 0xf0 );
  assert(b.data[17] == 0x0f );
  assert(b.data[18] == 0xd0 );
  assert(b.data[19] == 0x0d );

  bsInitRead( & b );
  // now try reading it all!
  assert( 1 == bsR( &b, 1 ) );
  assert( 0 == bsR( &b, 1 ) );
  assert( 0 == bsR( &b, 1 ) );
  assert( 1 == bsR( &b, 1 ) );
  assert( 1 == bsR( &b, 1 ) );
  assert( 1 == bsR( &b, 1 ) );
  assert( 0 == bsR( &b, 1 ) );
  bsFinishRead( & b );

  bsInitRead( & b );
  assert( 0x12 == bsR( &b, 7 ) );
  assert( 0x07e == bsR( &b, 11 ) );
  assert( 0x1 == bsR( &b, 1 ) );
  assert( 0x1234 == bsR( &b, 16 ) );
  assert( 0x5678 == bsR( &b, 16 ) );
  bsFinishRead( & b );

  // test bsPick
  b.pos = 0;
  bsInitReadAt( &b, 2 );
  i = bsR( &b, 7 );
  assert( 0x38 == i );

  // test bsReadUInt32
  b.pos = 0;
  bsInitRead( & b );
  assert( 0x9c241fa2 == bsReadUInt32( &b ) );
  assert( 0x468acf00 == bsReadUInt32( &b ) );
  assert( 0xdeadbeef == bsReadUInt32( &b ) );
  assert( 0xffff7eef == bsReadUInt32( &b ) );
  bsFinishRead( & b );
  b.pos = 0;
  bsInitRead( & b );
  assert( 1 == bsR( &b, 1 ) );
  assert( 0x38483f44 == bsReadUInt32( &b ) );
  bsFinishRead( & b );

  // test bsReadUInt64
  b.pos = 0;
  bsInitRead( & b );
  assert( 0x9c241fa2468acf00LL == bsReadUInt64( &b ) );
  assert( 0xdeadbeefffff7eefLL == bsReadUInt64( &b ) );
  bsFinishRead( & b );

  // Try some more different writes.
  b.pos = 0;
  b.len = 0;
  bsInitWrite( &b );
  bsW( &b, 24, 0x0 );
  bsW( &b, 24, 0x2 );
  bsFinishWrite( &b );
  bsInitRead( &b );
  assert( 0 == bsR( &b, 24 ));
  assert( 2 == bsR( &b, 24 ));
  bsFinishRead( &b );


  // try Assign and Deref
  b.len = 0;
  bsAssignUInt32( &b, 0x12345678 );
  bsAssignUInt8( &b, 0x11 );
  bsAssignUInt8( &b, 0x22 );
  bsAssignUInt8( &b, 0x33 );
  bsAssignUInt8( &b, 0x44 );
  bsAssignUInt64( &b, 0xfdecba9876543210LL );
  assert( b.len == 16 );
  b.pos = 0;
  assert( 0x12345678 == bsDerefUInt32( &b ) );
  assert( 0x11 == bsDerefUInt8( &b ) );
  assert( 0x22 == bsDerefUInt8( &b ) );
  assert( 0x33 == bsDerefUInt8( &b ) );
  assert( 0x44 == bsDerefUInt8( &b ) );
  assert( 0xfdecba9876543210LL == bsDerefUInt64( &b ) );
  assert( b.pos == 16 );

  // test writing lots of data
  {
    int sizes[] = {10, 900, 1024, 6000, 1024*1024, 2*1000*1000, 32*1024*1024};
    int nsizes = 7;
    int nbits;

    for( int test = 0; test < nsizes; test++ ) {
      printf("testing big buffer test %i\n", sizes[test]);
      b = build_buffer(0, NULL);

      nbits = num_bits32(sizes[test]);
      b.len = 0;
      bsInitWrite(&b);
      for( int i = 0; i < sizes[test]; i++ ) {
        buffer_t b2;
        // write sizes[i] entries to the file.
        b2 = b;
        buffer_extend(&b, 8);
        assert( b2.bsLive == b.bsLive );
        assert( b2.bsBuff == b.bsBuff );
        assert( b2.pos == b.pos );
        assert( b2.len == b.len );
        bsW(&b, nbits, sizes[test]);
        bsW(&b, nbits, i);
      }
      bsFinishWrite(&b);

      // check the data.
      b.pos = 0;
      bsInitRead(&b);
      for( int i = 0; i < sizes[test]; i++ ) {
        assert( sizes[test] == bsR(&b, nbits));
        assert( i == bsR(&b, nbits));
      }
      bsFinishRead(&b);

      free(b.data);
    }
  }
}




void test_mmap(void)
{
  // tests extending an mmapped buffer.
  error_t err;
  buffer_t b;
  FILE* f = tmpfile();
  unsigned int max = 100000;
  int got, expect;

  assert(f);
  err = mmap_buffer(&b, f, 1, 1);
  assert( ! err );

  b.len = 0;
  // try writing to it 
  for( unsigned int i = 0; i < max; i++ ) {
    buffer_extend_mmap(&b, f, 2);
    bsInitWrite(&b);
    bsPutUInt8(&b, (i>>8)&0xff);
    bsPutUInt8(&b, i&0xff);
    bsFinishWrite(&b);
  }
  err = munmap_buffer(&b);
  assert( ! err );

  // try reading what we wrote.
  err = mmap_buffer(&b, f, 0, 0);
  assert( ! err );
  for( unsigned int i = 0; i < max; i++ ) {
    bsInitRead(&b);
    got = bsDerefUInt8(&b);
    expect = (i>>8)&0xff;
    assert( got == expect );
    got = bsDerefUInt8(&b);
    expect = i&0xff;
    assert( got == expect );
  }
  err = munmap_buffer(&b);
  assert( ! err );

  fclose(f);

  f = tmpfile();
  assert(f);
  err = mmap_buffer(&b, f, 1, max);
  assert( ! err );
  assert( b.max >= max );

  err = munmap_buffer(&b);
  assert( ! err );

  fclose(f);

  // try writing lots of data
  {
    int sizes[] = {10, 900, 1024, 6000, 1024*1024, 2*1000*1000, 32*1024*1024};
    int nsizes = 7;
    int nbits;

    for( int test = 0; test < nsizes; test++ ) {
      printf("testing big mmap buffer test %i\n", sizes[test]);
      f = tmpfile();

      err = mmap_buffer(&b, f, 1, 1);
      assert( !err );

      nbits = num_bits32(sizes[test]);
      b.len = 0;
      bsInitWrite(&b);
      for( int i = 0; i < sizes[test]; i++ ) {
        buffer_t b2;
        // write sizes[i] entries to the file.
        b2 = b;
        buffer_extend_mmap(&b, f, 8);
        assert( b2.bsLive == b.bsLive );
        assert( b2.bsBuff == b.bsBuff );
        assert( b2.pos == b.pos );
        assert( b2.len == b.len );
        bsW(&b, nbits, sizes[test]);
        bsW(&b, nbits, i);
      }
      bsFinishWrite(&b);

      // check the data.
      b.pos = 0;
      bsInitRead(&b);
      for( int i = 0; i < sizes[test]; i++ ) {
        assert( sizes[test] == bsR(&b, nbits));
        assert( i == bsR(&b, nbits));
      }
      bsFinishRead(&b);

      // unmap the file, map it again, then check the data.
      err = munmap_buffer(&b);
      assert( ! err );

      err = mmap_buffer(&b, f, 1, 1);
      assert( !err );

      // check the data again
      b.pos = 0;
      bsInitRead(&b);
      for( int i = 0; i < sizes[test]; i++ ) {
        assert( sizes[test] == bsR(&b, nbits));
        assert( i == bsR(&b, nbits));
      }
      bsFinishRead(&b);

      err = munmap_buffer(&b);
      assert( ! err );

      fclose(f);
    }
  }
}

void test_64(void)
{
  int num = 12;
  uint64_t values[] = {1, 3, 36, 72000, 0x100000000e9ce84d, 0x1fe4fed1ae9ce84d, 0xffffffffffffffff, 1, 2, 99, 129515, 0xffffffff};
  uint64_t temp;
  int n = 64;

  buffer_t buf = build_buffer(10000, malloc(10000));
  assert(buf.data);

  for( int i = 0; i < num; i++ ) {
    buf.len = buf.pos = 0;
    bsInitWrite(&buf);
    bsW64(&buf, n, values[i]);
    bsFinishWrite(&buf);
    bsInitRead(&buf);
    temp = bsR64(&buf, n);
    assert( temp == values[i] );
    bsFinishRead(&buf);
  }

  free(buf.data);
}


void test_gamma(void)
{
  int num = 12;
  uint64_t values[] = {1, 3, 36, 72000, 0x100000000e9ce84d, 0x1fe4fed1ae9ce84d, 0xffffffffffffffff, 1, 2, 99, 129515, 0xffffffff};
  uint64_t temp;

  buffer_t buf = build_buffer(10000, malloc(10000));
  assert(buf.data);

  // try some gamma encoding and decoding.
  for( int i = 0; i < num; i++ ) {
    buf.len = buf.pos = 0;
    bsInitWrite(&buf);
    buffer_encode_gamma(&buf, values[i]);
    bsFinishWrite(&buf);
    bsInitRead(&buf);
    temp = buffer_decode_gamma(&buf);
    assert( temp == values[i] );
    bsFinishRead(&buf);
  }

  buf.len = buf.pos = 0;
  bsInitWrite(&buf);
  for( int i = 0; i < num; i++ ) {
    buffer_encode_gamma(&buf, values[i]);
  }
  bsFinishWrite(&buf);
  bsInitRead(&buf);
  for( int i = 0; i < num; i++ ) {
    temp = buffer_decode_gamma(&buf);
    assert( temp == values[i] );
  }
  bsFinishRead(&buf);

  free(buf.data);
}

int main(int argc, char** argv)
{
  test_numeric();
  test_bitbuffer();
  test_mmap();
  test_64();
  test_gamma();
  printf("All bitbuffer tests PASSED\n");
  return 0;
}
