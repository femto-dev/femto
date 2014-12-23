/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/bit_funcs.h
*/
#ifndef _BIT_FUNCS_H_
#define _BIT_FUNCS_H_

#include "config.h"
#include <inttypes.h>
#include <stdlib.h>

#ifndef _64_BIT
#ifdef __POINTER_WIDTH__
#if __POINTER_WIDTH__ == 64
#define _64_BIT
#endif
#endif
#ifdef __LP64__
#define _64_BIT
#endif
#endif

#define TOP_BIT_64 0x8000000000000000LL
#define TOP_BIT_32 0x80000000

#define leadz32(xxx) (__builtin_clz(xxx))
#define leadz64(xxx) (__builtin_clzll(xxx))
#define popcnt32(xxx) (__builtin_popcount(xxx))
#define popcnt64(xxx) (__builtin_popcountll(xxx))

#if defined(__GNUC__) && (defined(__x86_64) || defined(__x86_64__))

/*
Just use the builtin...
#define leadz32(xxx)  ({int32_t _x = xxx; int32_t _ans; const int32_t _tmp=-1;\
 asm ("bsr %1,%0 ; cmovz %2,%0" : "=r" (_ans) : "r" (_x), "r" (_tmp));\
 31 - _ans;\
})
#define leadz64(xxx)  ({int64_t _x = xxx; int64_t _ans; const int64_t _tmp=-1;\
 asm ("bsr %1,%0 ; cmovz %2,%0" : "=r" (_ans) : "r" (_x), "r" (_tmp));\
 63 - _ans;\
})
*/

#define rol32(x,by) ({ uint32_t __x = (x); char __by = (by); \
                       asm("rol %%cl,%0": "=r" (__x) : "c" (__by), "0" (__x) ); \
                       __x; })

// compute x*y in hi,lo
// asm ("assembly" : output operands : input operands : list of clobbered regs)
#define mul64_128(x,y,hi,lo) ({ uint64_t __x = (x); \
                                uint64_t __y = (y); \
                                uint64_t __hi; \
                                uint64_t __lo; \
                                asm("mul %3" \
                                    : "=d" (__hi), "=a" (__lo) \
                                    : "a" (__x),  "r" (__y) \
                                   ); \
                                hi = __hi; \
                                lo = __lo; \
                               })
// compute q = hi,lo / d; r is the remainder.
// asm ("assembly" : output operands : input operands : list of clobbered regs)
#define div128_64(hi,lo,d,q,r) ({ uint64_t __hi = (hi); \
                                  uint64_t __lo = (lo); \
                                  uint64_t __d = (d); \
                                  uint64_t __q; \
                                  uint64_t __r; \
                                  asm("div %4" \
                                      : "=a" (__q), "=d" (__r) \
                                      : "d" (__hi), "a" (__lo), "r" (__d) \
                                     ); \
                                  q = __q; \
                                  r = __r; \
                                 })
#endif

#ifndef leadz32
static inline int slow_leadz32(uint32_t v)
{
  int i;
  for(i = 0; i < 32; i++ ) {
    if( v & TOP_BIT_32 ) break;
    v <<= 1;
  }
  return i;
}
#define leadz32(xxx) slow_leadz32(xxx)
#endif

#ifndef leadz64
static inline int slow_leadz64(uint64_t v)
{
  /* Fancy impl. seems slower on sparc.
  v |= v >> 1;
  v |= v >> 2;
  v |= v >> 4;
  v |= v >> 8;
  v |= v >> 16;
  v |= v >> 32;

  v = ~v - ((~v >> 1) & 0x5555555555555555LL);
  v = ((v >> 2 ) & 0x3333333333333333LL) + (v & 0x3333333333333333LL);
  v = ((v >> 4 ) + v) & 0x0f0f0f0f0f0f0f0fLL;
  v = (v >> 8) + v;
  v = (v >> 16) + v;
  v = (v >> 32) + v;
  return v & 0xff;
  */
  int i;
  for(i = 0; i < 64; i++ ) {
    if( v & TOP_BIT_64 ) break;
    v <<= 1;
  }
  return i;
}
#define leadz64(xxx) slow_leadz64(xxx)
#endif

#ifndef rol32
static inline int slow_rol32(uint32_t x, char by)
{
  unsigned char bit;
  for(;by>0;by--) {
    bit = ((x & TOP_BIT_32) != 0);
    x <<= 1;
    x |= bit;
  }
  return x;
}
#define rol32(xxx,by) slow_rol32(xxx,by)
#endif

// returns the same as floor(log2(y))
static inline
int log2i( unsigned int y )
{
  return 31 - leadz32(y);
}

static inline
int log2lli( uint64_t y )
{
  return 63 - leadz64(y);
}

// returns 2^y
static inline
uint64_t pow2i( unsigned int y )
{
  uint64_t ret = 1;
  return ret << y;
}

// returns top/bottom rounded up.
#define ceildiv(top,bottom) ((top + bottom - 1) / bottom)

#define ceildiv8(top) (ceildiv(top,8))

// number of bits needed to encode 0..maxEnc
static inline
int num_bits32(int maxEnc)
{
  // must be able to encode 1 in 1 bit;
  // 8 in 4 bits, etc.
  if( maxEnc > 0 ) return log2i(maxEnc)+1;
  else return 1;
  //int i;
  //for( i = 0; ((1 << i)-1) < maxEnc; i++ ) { }
  //return i;
}

static inline
int num_bits64(int64_t maxEnc)
{
  if( maxEnc > 0 ) return log2lli(maxEnc)+1;
  else return 1;
}

// padding aligns so the bits matching this mask are 0.
#define ALIGN_MASK 7

// round_to_mask rounds down to a mask; mask is 00s followed by 1s
static inline 
int64_t round_down_to_mask( int64_t v, int64_t mask)
{
  return v & (~mask);
}

// round_to_mask rounds up to a mask; mask is 00s followed by 1s
static inline 
int64_t round_to_mask( int64_t v, int64_t mask)
{
  int64_t tmp = round_down_to_mask(v,mask);
  if( tmp != v ) tmp += mask + 1;
  return tmp;
}

static inline
int64_t round_up_to_multiple( int64_t num, int64_t mod )
{
  return mod * ceildiv(num,mod);
}

static inline
int64_t round_down_to_multiple( int64_t num, int64_t mod )
{
  return mod * (num / mod);
}

static inline size_t try_sync_add_size_t(size_t* m, size_t i)
{
#if HAVE_DECL___SYNC_FETCH_AND_ADD
  return __sync_fetch_and_add(m, i);
#else
#warning no __sync_fetch_and_add
  /*size_t tmp = *m;
  *m += i;
  return tmp;*/
  return *m;
#endif
}

#endif
