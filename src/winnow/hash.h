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

  femto/src/winnow/hash.h
*/

#ifndef _HASH_H_
#define _HASH_H_ 1

#include <stdint.h>

typedef struct hash_s {
  uint64_t a;
  uint64_t b;
} hash_t;

struct hash {
  int64_t position;
  hash_t hash;
  uint8_t nerrors;
  uint8_t nhashes;
  uint16_t score;
  uint32_t length;
};

static inline
uint32_t rotl32 ( uint32_t x, int8_t r )
{
  r = r % 32;
  if( r < 0 ) r += 32;
  return (x << r) | (x >> (32 - r));
}

static inline
uint64_t rotl64 ( uint64_t x, int8_t r )
{
  r = r % 64;
  if( r < 0 ) r += 64;
  return (x << r) | (x >> (64 - r));
}

static inline
hash_t shl_hash( hash_t x, int8_t r )
{
  x.a <<= r;
  x.a |= (x.b >> (64 - r));
  x.b <<= r;
  return x;
}

static inline
hash_t shr_hash( hash_t x, int8_t r )
{
  x.b >>= r;
  x.b |= (x.a << (64 - r));
  x.a >>= r;
  return x;
}

static inline
hash_t ROL_hash( hash_t x, int8_t r )
{
  uint64_t tmp, lost_a, lost_b;
  r = r % 128;
  if( r < 0 ) r += 128;
  if( r >= 64 ) {
    // Swap if we're rotating more than 64b
    tmp = x.a;
    x.a = x.b;
    x.b = tmp;
    r -= 64;
  }
  if( r == 0 ) return x;
  lost_a = (x.a >> (64 - r));
  lost_b = (x.b >> (64 - r));
  x.a = (x.a << r) | lost_b;
  x.b = (x.b << r) | lost_a;
  return x;
}

static inline
hash_t XOR_hash( hash_t x, hash_t y )
{
  x.a ^= y.a;
  x.b ^= y.b;
  return x;
}

static inline
hash_t XOR_hash_uint64( hash_t x, uint64_t y )
{
  x.b ^= y;
  return x;
}

static inline
int EQUALS_hash( hash_t x, hash_t y )
{
  return (x.a == y.a && x.b == y.b);
}

static inline
int LESS_hash( hash_t x, hash_t y )
{
  return (x.a < y.a || (x.a == y.a && x.b < y.b));
}

static inline
int LESS_EQUALS_hash( hash_t x, hash_t y )
{
  return (x.a <= y.a || (x.a == y.a && x.b <= y.b));
}



static hash_t HASH_MAX = {-1,-1};
static hash_t HASH_ZERO = {0,0};

extern hash_t random_table[256];

void init_table(void);

static inline
hash_t hash_push(hash_t hash, unsigned char byte)
{
  hash = ROL_hash(hash,1);
  hash = XOR_hash(hash, random_table[byte]);
  return hash;
}
static inline
hash_t hash_pop(hash_t hash, unsigned char byte, int nback)
{
  hash_t part = random_table[byte];
  /*for( i = 0; i < 64; i++ ) {
    printf("r% 2i part = %016" PRIX64 "\n", i, ROL_hash(part, i));
    printf("r% 2i xored = %016" PRIX64 "\n", i, hash ^ ROL_hash(part, i));
  }*/
  part = ROL_hash(part, nback);
  hash = XOR_hash(hash, part);
  return hash;
}

#endif
