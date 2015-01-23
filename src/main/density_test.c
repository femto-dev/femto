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

  femto/src/main/density_test.c
*/

#include <stdio.h> 
#include <stdlib.h> 
#include <inttypes.h> 
#include <assert.h> 

#include "bit_funcs.h"
#include "density.h"

void print_bits(int n, uint64_t x)
{
  int i;
  for (i = n-1; i >= 0; i--) {
    printf("%i", (int) ((x >> i) & 1));
  } 
}


int main(int argc,char** argv)
{
  // precompute (n choose d) (n-1 choose d-1) ... for
  // n up to our limit.
  uint64_t n = 64;
  uint64_t i, d, x;
  uint64_t use_n, use_k;
  uint64_t sum = 0;
  struct density_offset d_o;
  int test_start_n = 0;
  int test_n = 20;
  int verbose = 0;

  assert(27540584512ull == (unsigned long long) density_choose(64, 55));
  assert(1777090076065542336ull == (unsigned long long) density_choose(64, 31));
  assert(1832624140942590534ull == (unsigned long long) density_choose(64, 32));
  assert(1777090076065542336ull == (unsigned long long) density_choose(64, 33));
  assert(1620288010530347424ull == (unsigned long long) density_choose(64, 34));

  for( n = test_start_n; n <= test_n; n++ ) {
    printf("testing n=%i\n", n);
    for( d = 0; d <= n; d++ ) {
      for( i = 0; i < density_choose(n, d); i++ ) {
        x = density_offset_to_number( n, d, i );
        // But only take the bottom n bits.
        x <<= (64 - n);
        x >>= (64 - n);
        if( verbose ) {
          printf("f(n=%02i,d=%02i,i=%02llu) = ", n, d, (long long unsigned int) i);
          print_bits(n, x);
          printf(" = %llu\n", (long long unsigned int) x);
        }

        d_o = density_number_to_density_offset(n, x);
        if( verbose ) {
          printf("g(n=%02i,x=%02llu) = (%02llu,%02llu)\n", n, (long long unsigned int) x, (long long unsigned int) d_o.density, (long long unsigned int) d_o.offset);
        }

        assert( d_o.density == d && d_o.offset == i );
      }
    }
    if( verbose ) printf("\n");
  }
  return 0;
}

