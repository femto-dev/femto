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

  femto/src/main/density.c
*/

#include <stdio.h> 
#include <stdlib.h> 
#include <inttypes.h> 
#include <assert.h> 

#include "bit_funcs.h"
#include "density.h"

/* This file is an investigation of the bit sequence compression
   technique described as RRR (Raman, Raman, and Rao from SODA 2002)
   in the paper "Practical Rank/Select Queries over Arbitrary Sequences"
   by Francisco Claude and Gonzalo Navarro (SPIRE 08). It looks like
   RRR - even with 64-bit chunks - does not significantly improve
   upon FEMTO's current technique. In experiments with English text,
   a PDF document, and random data, I observed only up to a 5% reduction
   in compressed size. That reduction would be attainable only
   if the RRR technique also includes a run-length encoding scheme
   so that a run of density 0 or density 1 words are run-length encoded.
   Without the run-length encoding, the RRR approach seems to be
   actually worse in compression than FEMTO's current technique.

   In any case, I did develop functions to translate between
    (up to) 64-bit number and (density,index)
    and (density,index) back to a number.
   These functions could be used by an RRR-like scheme if it were
   ever deemed worth implementing.
 */
static
uint64_t memoize[65][65];

static
void update_choose65(int n, int k);

static inline
uint64_t choose65(int n, int k)
{
  // this has overflow problems and is slower for computing our table..
  //if( k == 0 ) return 1;
  //else return (n * choose(n-1, k-1))/k;
  assert(n < 128);
  if( k <= 0 ) return 1;
  if( k >= n ) return 1;
  if( memoize[n][k] ) return memoize[n][k];
  update_choose65(n,k);
  return memoize[n][k];
}

static
void update_choose65(int n, int k)
{
  memoize[n][k] = choose65(n-1, k-1) + choose65(n-1,k);
}


uint64_t density_choose(int n, int k)
{
  assert(n < 65);
  return choose65(n, k);
}


uint64_t density_offset_to_number(int n, int d /*density*/, uint64_t index)
{
  uint64_t ret = 0;
  uint64_t part;
  uint64_t zero = 0;
  uint64_t one = 1;
  int bit;
  uint64_t tmp = 0;

  /*
  // This is the recursive way that is backwards.
  if( d == 0 ) ret = 0;
  else if( d == 1 ) ret = one << (n - 1 - index);
  else if( d == n ) ret = ~ zero;
  else if( d == n-1 ) ret = ~ (one << index);
  else {
    // More base cases might be interesting...
    part = choose65(n-1,d);
    //printf("part = choose65(%i,%i) = %llu\n", n-1, d, (long long unsigned int) part);
    bit = 0;
    if( index >= part ) { index -= part; bit = 1; }
    ret = (offset_to_number(n-1, d-bit, index) << 1) | bit;
  }*/

  /*
  // This is the recursive way.
  if( d == 0 ) ret = 0;
  else if( d == n ) ret = (~ zero) >> (64 - n);
  else {
    // More base cases might be interesting...
    part = choose65(n-1,d);
    //printf("part = choose65(%i,%i) = %llu\n", n-1, d, (long long unsigned int) part);
    bit = 0;
    if( index >= part ) { index -= part; bit = 1; }
    use_bit = bit;
    ret |= (use_bit << (n-1));
    ret |= offset_to_number(n-1, d-bit, index);
  }
  */

  // This is the iterative way.
  while( d > 1 && d < n-1 ) {
    part = choose65(n-1,d);
    bit = 0;
    if( index >= part ) { index -= part; bit = 1; d--; }
    n--;
    ret <<= 1;
    ret |= bit;
  }
  if( d == 0 ) {
    ret <<= n;
  } else if( d == 1 ) {
    ret <<= n;
    ret |= one << index;
  } else if( d == n-1 ) {
    ret <<= n;
    tmp = ~ (one << (n - 1 - index));
    // Clear all but the bottom n bits of tmp.
    tmp <<= (64 - n);
    tmp >>= (64 - n);
    ret |= tmp;
  } else if( d == n ) {
    ret <<= n;
    ret |= (~zero) >> (64 - n);
  }

  //printf("f(n=%i,d=%i,i=%llu) = ", n, d, (long long unsigned int) index_in);
  //print_bits(n, ret);
  //printf("\n");
  return ret;
}

uint64_t density_number_to_offset(int n, int d, uint64_t num)
{
  uint64_t bit;
  uint64_t tmp;
  uint64_t ret;

  /*
  // This is the recursive way that is backwards.
  if( d == 0 ) return 0;
  if( d == 1 ) return leadz64(num) - (64 - n);
  if( d == n ) return 0;
  if( d == n-1 ) return n - leadz64((~num) << (64 - n)) - 1;

  // extract the bottom  bit.
  bit = num & 1;
  rest = num >> 1;

  // return value.
  ret = 0;
  if( bit ) ret += choose65(n-1, d);
  ret += number_to_offset(n-1, d-bit, rest);
  */


  /*
  // This is the recursive way
  if( d == 0 ) return 0;
  if( d == n ) return 0;

  // extract the top bit.
  bit = (num >> (n-1)) & 1;

  // return value.
  ret = 0;
  if( bit ) ret += choose65(n-1, d);
  ret += number_to_offset(n-1, d-bit, num);
  */

  // This is the iterative way.
  ret = 0;
  while( d > 1 && d < n-1 ) {
    // extract the top bit.
    bit = (num >> (n-1)) & 1;
    if( bit ) { ret += choose65(n-1, d); d--; }
    n--;
  }
  if( d == 0 ) {
    // add nothing to ret.
  } else if( d == 1 ) {
    tmp = num;
    // Clear the top bits in tmp.
    tmp <<= (64 - n);
    // Count the number of trailing zeros in the n-bit 1-density number.
    ret += n - leadz64(tmp) - 1;
  } else if( d == n-1 ) {
    tmp = num;
    // Clear the top bits in tmp.
    tmp <<= (64 - n);
    tmp = ~tmp;
    // Count the number of leading 1s in the number.
    ret += leadz64(tmp);
  } else if( d == n ) {
    // add nothing to ret.
  }

  return ret;
}

struct density_offset density_number_to_density_offset(int n, uint64_t num)
{
  struct density_offset ret;
  uint64_t density;
  uint64_t offset;

  density = popcnt64(num);

  offset = density_number_to_offset(n, density, num);

  ret.density = density;
  ret.offset = offset;

  return ret;
}

