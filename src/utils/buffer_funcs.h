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

  femto/src/utils/buffer_funcs.h
*/
#ifndef _BUFFER_FUNCS_H_
#define _BUFFER_FUNCS_H_

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "error.h"
#include "bswap.h"
#include "bit_funcs.h"
#include "buffer.h"
#include "util.h"

static inline buffer_t build_buffer(int64_t max, unsigned char* data)
{
  buffer_t b = {max, 0, data, 0, 0, 0};
  return b;
}


/*---------------------------------------------------*/
/*--- Bit stream I/O  (adapted from BZIP2)        ---*/
/*---------------------------------------------------*/
                                                                               
/*---------------------------------------------------*/
static inline
void bsInitWrite ( buffer_t* s )
{
   s->bsLive = 0;
   s->bsBuff = 0;
}

static inline
void bsInitRead ( buffer_t* s ) 
{
   s->bsLive = 0;
   s->bsBuff = 0;
}
                                                                               
#define bsbuff_t_bits (sizeof(bsbuff_t)*8)
#define bsbuff_t_bits_less_8 (bsbuff_t_bits - 8)
/*---------------------------------------------------*/
static inline
void bsFinishWrite ( buffer_t* s )
{
   while (s->bsLive > 0) {
      s->data[s->len] = (unsigned char)(s->bsBuff >> bsbuff_t_bits_less_8);
      s->len++;
      s->bsBuff <<= 8;
      s->bsLive -= 8;
   }
}

static inline
void bsFinishRead ( buffer_t* s ) 
{
}
  
static inline
int  bsNumBits ( buffer_t* s ) 
{
  return 8 * s->len + s->bsLive;
}

/*---------------------------------------------------*/
#define bsNEEDW(nz)                           \
{                                             \
   while (s->bsLive >= 8) {                   \
      s->data[s->len]                         \
         = (unsigned char)(s->bsBuff >> bsbuff_t_bits_less_8);  \
      s->len++;                               \
      s->bsBuff <<= 8;                        \
      s->bsLive -= 8;                         \
   }                                          \
}
                                                                               
                                                                               
/*---------------------------------------------------*/
// Assumes that n is <= 24 bits.
static inline
void bsW24 ( buffer_t* s, int32_t n, bsbuff_t v )
{
   bsNEEDW ( n );
   s->bsBuff |= (v << (bsbuff_t_bits - s->bsLive - n));
   s->bsLive += n;
}

static inline
void bsW( buffer_t* s, uint32_t n, bsbuff_t v )
{
  // write a general number in multiple chunks.
  while ( n >= bsbuff_t_bits_less_8 ) {
    n -= bsbuff_t_bits_less_8;
    bsW24( s, bsbuff_t_bits_less_8, v >> n);
  }
  if( n > 0 ) bsW24( s, n, v );
}

static inline
void bsW64( buffer_t* s, uint32_t n, uint64_t v )
{
  // write a general number in multiple chunks.
  while ( n >= bsbuff_t_bits_less_8 ) {
    n -= bsbuff_t_bits_less_8;
    bsW24( s, bsbuff_t_bits_less_8, v >> n);
  }
  if( n > 0 ) bsW24( s, n, v );
}

// Assumes n <= bsbuff_t_bits_less_8 bits.
// Read from a binary stream buffer.
static inline
bsbuff_t bsR24 ( buffer_t* s, uint32_t n )
{
  bsbuff_t v;
  const bsbuff_t one = 1;
  while( 1 ) {
    if( s->bsLive >= n ) {
      v = (s->bsBuff >>  (s->bsLive-n)) & ((one << n)-one);
      // only keep the bottom n bits.
      s->bsLive -= n;
      return v;
    }
    s->bsBuff = (s->bsBuff << 8) | ((bsbuff_t) (s->data[s->pos])); 
    s->bsLive += 8;
    s->pos++;
  }

}
static inline
bsbuff_t bsR( buffer_t* s, uint32_t n ) 
{
  bsbuff_t v;
  v = 0;
  while( n >= bsbuff_t_bits_less_8 ) {
    v <<= bsbuff_t_bits_less_8;
    v |= bsR24(s, bsbuff_t_bits_less_8);
    n -= bsbuff_t_bits_less_8;
  }
  v <<= n;
  if( n > 0 ) v |= bsR24(s, n);
  return v;
}
static inline
uint64_t bsR64( buffer_t* s, uint32_t n ) 
{
  uint64_t v;
  v = 0;
  while( n >= bsbuff_t_bits_less_8 ) {
    v <<= bsbuff_t_bits_less_8;
    v |= (uint64_t) bsR24(s, bsbuff_t_bits_less_8);
    n -= bsbuff_t_bits_less_8;
  }
  v <<= n;
  if( n > 0 ) v |= (uint64_t) bsR24(s, n);
  return v;
}

static inline
void bsInitReadAt( buffer_t* s, intptr_t bitoffset )
{
  // scroll the buffer forward to the bit position.
  int byte;
  int prebits;

  byte = bitoffset / 8;
  prebits = bitoffset % 8;

  s->pos += byte;
  bsInitRead( s );
  bsR24( s, prebits );
}

/*---------------------------------------------------*/
static inline
void bsPutUInt32 ( buffer_t* s, int32_t u )
{
   bsW24 ( s, 8, (u >> 24) & 0xffL );
   bsW24 ( s, 8, (u >> 16) & 0xffL );
   bsW24 ( s, 8, (u >>  8) & 0xffL );
   bsW24 ( s, 8,  u        & 0xffL );
}
static inline
uint32_t bsReadUInt32 ( buffer_t* s ) 
{
  uint32_t ret;
  ret = bsR24 ( s, 8 );
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 );

  return ret; 
}

static inline
void bsPutUInt64 ( buffer_t* s, uint64_t u )
{
   bsW24 ( s, 8, (u >> 56) & 0xffLL );
   bsW24 ( s, 8, (u >> 48) & 0xffLL );
   bsW24 ( s, 8, (u >> 40) & 0xffLL );
   bsW24 ( s, 8, (u >> 32) & 0xffLL );
   bsW24 ( s, 8, (u >> 24) & 0xffLL );
   bsW24 ( s, 8, (u >> 16) & 0xffLL );
   bsW24 ( s, 8, (u >>  8) & 0xffLL );
   bsW24 ( s, 8,  u        & 0xffLL );
}
                                                                               
                                                                               
static inline
uint64_t bsReadUInt64 ( buffer_t* s ) 
{
  uint64_t ret;
  ret = bsR24 ( s, 8 );
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 
  ret <<= 8;
  ret |= bsR24 ( s, 8 ); 

  return ret;
}
                                                                               
/*---------------------------------------------------*/
static inline
void bsPutUChar ( buffer_t* s, unsigned char c )
{
   bsW24( s, 8, (uint32_t)c );
}
static inline
void bsPutUInt8 ( buffer_t* s, unsigned char c )
{
  bsPutUChar(s,c);
}


static inline
unsigned char bsDerefUInt8( buffer_t* s)
{
  unsigned char ret;
  ret = s->data[s->pos];
  s->pos += 1;
  return ret;
}

static inline
void bsAssignUInt8( buffer_t* s, unsigned char x ) 
{
  s->data[s->len] = x;
  s->len++;
}

static inline
uint32_t bsDerefUInt32( buffer_t* s)
{
  uint32_t ret;
  ret = ntoh_32(* (uint32_t*) &s->data[s->pos]);
  s->pos += sizeof(uint32_t);
  return ret;
}

static inline
void bsAssignUInt32( buffer_t* s, uint32_t x ) 
{
  * (uint32_t*) &s->data[s->len] = hton_32(x);
  s->len += sizeof(uint32_t);
}

static inline
uint64_t bsDerefUInt64( buffer_t* s)
{
  uint64_t ret;
  ret = ntoh_64(* (uint64_t*) &s->data[s->pos]);
  s->pos += sizeof(uint64_t);
  return ret;
}

static inline
void bsAssignUInt64( buffer_t* s, uint64_t x ) 
{
  * (uint64_t*) &s->data[s->len] = hton_64(x);
  s->len += sizeof(uint64_t);
}

// fails if the stuff won't fit into the buffer.
static inline
void* buffer_calloc(buffer_t* buf, intptr_t size )
{
  void* ret;

  if( buf->len + size < buf->max ) {
    ret = & buf->data[buf->len];
    buf->len += size;
    memset(ret, 0, size);
    return ret;
  } else {
    return NULL;
  }
}

static inline
error_t buffer_extend(buffer_t* buf, intptr_t size )
{
  uintptr_t newmax;
  unsigned char* newdata;

  if( buf->len + size < buf->max ) {
    // it's ok! it'll fit.
  } else {
    // extend the buffer.
    if( buf->max < BUFFER_CHUNK ) {
      newmax = 2 * buf->max + size;
    } else {
      newmax = buf->max + BUFFER_CHUNK * CEILDIV(size, BUFFER_CHUNK);
    }
    assert( buf->len + size <= newmax );
    newdata = (unsigned char*) realloc(buf->data, newmax);
    if( ! newdata ) return ERR_MEM;
    buf->max = newmax;
    buf->data = newdata;
  }

  return ERR_NOERR;
}

static inline
error_t buffer_extend_W24(buffer_t* buf, int32_t n, bsbuff_t v)
{
  error_t err;
  // make sure there's room in the buffer.
  err = buffer_extend(buf, sizeof(uint64_t));
  if( err ) return err;

  bsW24(buf, n, v);

  return ERR_NOERR;
}

static inline
void buffer_align( buffer_t* buf, intptr_t mask )
{
  while( buf->len & mask ) {
    buf->data[buf->len++] = 0;
  }
}

static inline
void buffer_encode_gamma( buffer_t* buf, uint64_t v)
{
  unsigned char log;
  uint64_t seg;

  log = log2lli(v);
  // encode log zeros. Could be as many 64.
  seg = 0;
  bsW64( buf, log, seg );

  // encode the number, including the leading one.
  bsW64( buf, log+1, v );

}

static inline
uint64_t buffer_decode_gamma( buffer_t* buf )
{
  unsigned char bit;
  unsigned char log;
  uint64_t value;
  // could be updated to look at bytes directly...
  // first, decode 1 bit to pull out log.
  log = 0; 
  bit = 0;
  value = 1;
  while( 1 ) {
    bit = bsR24( buf, 1 );
    if( bit ) break;
    log++;
    value <<= 1;
  }
  // log is the number of zeros we read.
  if( log != 0 ) {
    value |= bsR64( buf, log );
  }

  return value;
}

#endif
