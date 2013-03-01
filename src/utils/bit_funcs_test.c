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

  femto/src/utils/bit_funcs_test.c
*/
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "bswap.h"
#include "bit_funcs.h"
#include "timing.h"


void test_mul(uint64_t x, uint64_t y, uint64_t hi, uint64_t lo)
{
#ifdef mul64_128
  uint64_t got_hi = 0;
  uint64_t got_lo = 0;
  uint64_t got_x = x;
  uint64_t got_y = y;

  mul64_128(got_x, got_y, got_hi, got_lo);

  assert( x == got_x );
  assert( y == got_y );
  assert( hi == got_hi );
  assert( lo == got_lo );
#endif
}

void test_div(uint64_t hi, uint64_t lo, uint64_t d, uint64_t q, uint64_t r)
{
#ifdef div128_64
  uint64_t got_hi = hi;
  uint64_t got_lo = lo;
  uint64_t got_d = d;
  uint64_t got_q = 0;
  uint64_t got_r = 0;

  div128_64(got_hi, got_lo, got_d, got_q, got_r);

  assert(got_hi == hi);
  assert(got_lo == lo);
  assert(got_d == d);
  assert(got_q == q);
  assert(got_r == r);
#endif
}

void test_ops(void)
{
    int64_t mask;

    assert( 64 == leadz64(0) );
    assert( 63 == leadz64(1) );
    assert( 55 == leadz64(0x120) );
    assert( 1 == leadz64(0x7fffffff00000000LL) );
    assert( 0 == leadz64(0xffffffff00000000LL) );
    assert( 0 == leadz64(0xffffffffffffffffLL) );
    assert( 32 == leadz32(0) );
    assert( 31 == leadz32(1) );
    assert( 23 == leadz32(0x120) );
    assert( 1 == leadz32(0x7fffffff) );
    assert( 0 == leadz32(0xffffffff) );

    for( int i = 0; i < 63; i++ ) {
      assert( 63 - i == leadz64(1LL << i) );
    }

    assert( 0 == log2i(1) );
    assert( 1 == log2i(2) );
    assert( 1 == log2i(3) );
    assert( 2 == log2i(4) );
    assert( 2 == log2i(5) );
    assert( 2 == log2i(6) );
    assert( 2 == log2i(7) );
    assert( 3 == log2i(8) );

    mask = 8 - 1;
    assert(0 == round_down_to_mask( 6, mask) );
    assert(8 == round_to_mask( 6, mask) );
    assert(0 == round_down_to_mask( 0, mask) );
    assert(0 == round_to_mask( 0, mask) );
    assert(8 == round_down_to_mask( 8, mask) );
    assert(8 == round_to_mask( 8, mask) );
    assert(8 == round_down_to_mask( 9, mask) );
    assert(16 == round_to_mask( 9, mask) );
    assert(8 == round_down_to_mask( 15, mask) );
    assert(16 == round_to_mask( 15, mask) );
    assert(16 == round_down_to_mask( 16, mask) );
    assert(16 == round_to_mask( 16, mask) );

    assert(0 == round_down_to_multiple( 0, 7 ) );
    assert(0 == round_down_to_multiple( 6, 7 ) );
    assert(7 == round_down_to_multiple( 7, 7 ) );
    assert(7 == round_down_to_multiple( 8, 7 ) );
    assert(0 == round_up_to_multiple( 0, 7 ) );
    assert(7 == round_up_to_multiple( 6, 7 ) );
    assert(7 == round_up_to_multiple( 7, 7 ) );
    assert(14 == round_up_to_multiple( 8, 7 ) );

    test_mul(0xAAAAAAAAAAAAAAAAL,
             0xCCCCCCCCCCCCCCCCL,
             0x8888888888888887L, 
             0x7777777777777778L);
    test_mul(0xFFFFFFFFFFFFFFFFL,
             0xFFFFFFFFFFFFFFFFL,
             0xFFFFFFFFFFFFFFFEL,
             0x0000000000000001L);
    test_mul(0xFFFFFFFFFFFFFFFFL,
             0x0000000000000001L,
             0x0000000000000000L,
             0xFFFFFFFFFFFFFFFFL);
    test_mul(0xFFFFFFFFFFFFFFFFL,
             0x0000000000000000L,
             0x0000000000000000L,
             0x0000000000000000L);
    test_mul(0x1234567812345678L,
             0x1234567812345678L,
             0x14B66DC208BA5F8L,
             0x3D35175C1DF4D840L);
    test_mul(0x91A912349928133BL,
             0x91A912349928133BL,
             0x52E0F648A03D0D0AL,
             0x7083093A89E1CF99L);

    test_div(0x0L,
             0x0L,
             0xABCL,
             0x0L,
             0x0L);

    test_div(0x0000000000000000L,
             0xF234567812345678L,
             0x1L,
             0xF234567812345678L,
             0x0L);

 
    test_div(0x0000000000000001L,
             0xFFFFFFFFFFFFFFFFL,
             0xABCL,
             0x2FB27DF354968BL,
             0x3EB);
}

int main(int argc, char** argv)
{
  int64_t i;
  int64_t num = 1000000000;
  int64_t r;


  test_ops();

  printf("all bit_funcs tests PASSED\n");

  if( argc == 1 ) return 0;

  start_clock();
  for( i = 0; i < num; i++ ) {
    r = ntoh_64(i);
    r = ntoh_64(i|r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
    r = ntoh_64(i^r);
  }
  stop_clock();
  print_timings("ntoh_64", 10*num);
  start_clock();


  for( i = 0; i < num; i++ ) {
    r = leadz64(0x00020502043812LL);
    r = leadz64(1 + r);
    r = leadz64(0x0100000000000000LL ^ r);
    r = leadz64(0x0010000000000100LL ^ r);
    r = leadz64(0x0000100000000100LL ^ r);
    r = leadz64(0x0000000100000100LL ^ r);
    r = leadz64(0x0000000001000100LL ^ r);
    r = leadz64(0x0000000000001100LL ^ r);
    r = leadz64(0x0000000000000100LL ^ r);
    r = leadz64(0x0008000000000100LL ^ r);
  }
  stop_clock();

  print_timings("leadz64", 10*num);

  start_clock();

  for( i = 0; i < num; i++ ) {
    r = 0x1030020502043812LL ^ num;
    r <<= 4;
    r |= num;
    r <<= 4;
    r ^= num;
    r >>= 8;
    r ^= num;
    r <<= 1;
    r &= num;
    r >>= 4;
  }

  stop_clock();
  print_timings("bit ops", 10*num);

  return 0;
}

