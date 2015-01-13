/*
  (*) 2007-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/suffix_sort.h
*/

#ifndef _SUFFIX_SORT_H_
#define _SUFFIX_SORT_H_

#include <inttypes.h>
#include "error.h"
#include "bit_funcs.h"
#include "bswap.h"
#include "string_sort.h"

// suffix pointer type
typedef long sptr_t;
typedef unsigned long usptr_t;

typedef struct suffix_sorting_problem {
  sptr_t n; // the number of characters in T and suffixes in S
  sptr_t t_size; // the number of bytes in T
  sptr_t s_size; // the number of bytes in S
  sptr_t max_char; // the maximum attainable character (set to 1<<8*bytes_per_character - 1 if 0)
  sptr_t t_padding_size; // the number of bytes of 0 padding after T
  int bytes_per_character; // # of bytes per character in T
  int bytes_per_pointer; // # of bytes per pointer in S
  unsigned char* T; // the text in question.
                    // bytes_per_character*n bytes long
  unsigned char* S; // the suffix array (output)
                    // bytes_per_pointer*n bytes long
                    // each element of S is a *byte* offset
                    // into the T array.
} suffix_sorting_problem_t;

// Store a number in a big-endian variable-byte way.
// Stores a *signed* number.
static inline
void set_be(unsigned char* buf, const size_t bytes_per, const sptr_t x)
{
  switch( bytes_per ) {
    case 1:
      *buf = x;
      return;
    case 2:
      *(uint16_t*)buf = hton_16(x);
      return;
    case 3:
      *(uint16_t*)buf = hton_16(x>>8);
      buf[2] = x & 0xff;
      return;
    case 4:
      *(uint32_t*)buf = hton_32(x);
      return;
    case 5:
      *(uint32_t*)buf = hton_32(x>>8);
      buf[4] = x & 0xff;
      return;
    case 6:
      *(uint32_t*)buf = hton_32(x>>16);
      buf[4] = (x>>8) & 0xff;
      buf[5] = x & 0xff;
      return;
    case 7:
      *(uint32_t*)buf = hton_32(x>>24);
      buf[4] = (x>>16) & 0xff;
      buf[5] = (x>>8) & 0xff;
      buf[6] = x & 0xff;
      return;
    case 8:
      *(uint64_t*)buf = hton_64(x);
      return;
  }
}

// Get, from a "big-endian" stored multi-byte number,
// a value. Returns a *signed* number.
static inline
sptr_t get_be(const unsigned char* buf, const size_t bytes_per)
{
  switch( bytes_per ) {
    case 1:
      return (char) *buf;
    case 2:
      return (int16_t) ntoh_16(*(const int16_t*)buf);
    case 3:
      {
        int16_t num2 = *(const uint16_t*)buf;
        unsigned char num3 = buf[2];
        sptr_t ret;
        ret = (int16_t) ntoh_16(num2);
        ret <<= 8;
        ret |= num3;
        return ret;
      }
    case 4:
      return (int32_t) ntoh_32(*(const int32_t*) buf);
    case 5:
      {
        int32_t num4 = *(const int32_t*)buf;
        unsigned char num5 = buf[4];
        sptr_t ret;
        ret = (int32_t) ntoh_32(num4);
        ret <<= 8;
        ret |= num5;
        return ret;
      }
    case 6:
      {
        int32_t num4 = *(const int32_t*)buf;
        unsigned char num5 = buf[4];
        unsigned char num6 = buf[5];
        sptr_t ret;
        ret = (int32_t) ntoh_32(num4);
        ret <<= 8;
        ret |= num5;
        ret <<= 8;
        ret |= num6;
        return ret;
      }
    case 7:
      {
        int32_t num4 = *(const int32_t*)buf;
        unsigned char num5 = buf[4];
        unsigned char num6 = buf[5];
        unsigned char num7 = buf[6];
        sptr_t ret;
        ret = (int32_t) ntoh_32(num4);
        ret <<= 8;
        ret |= num5;
        ret <<= 8;
        ret |= num6;
        ret <<= 8;
        ret |= num7;
        return ret;
      }
    case 8:
      return (int64_t) ntoh_64(*(const int64_t*) buf);
  }
  return 0;
}

// Set an entry into S given a pointer into S.
// value should be a byte offset from T.
static inline
void set_S_with_ptr(const size_t bytes_per_pointer,
                    unsigned char* ptr,
                    const sptr_t byte_offset_value)
{
  set_be(ptr, bytes_per_pointer, byte_offset_value);
}

// sets S[i] = x
static inline
void set_S(const size_t bytes_per_pointer,
           unsigned char* S,
           const sptr_t byte_offset,
           const sptr_t byte_offset_value)
{
  set_be(S+byte_offset, bytes_per_pointer, byte_offset_value);
}


// returns S[i], which is a byte offset into T
static inline
sptr_t get_S(const size_t bytes_per_pointer,
             const unsigned char* S,
             const sptr_t byte_offset)
{
  return get_be(S+byte_offset, bytes_per_pointer);
}

// Given a pointer into S, returns a byte offset into T
static inline
sptr_t get_S_with_ptr(const size_t bytes_per_pointer,
                      const unsigned char* ptr)
{
  return get_be(ptr, bytes_per_pointer);
}

// Takes in a byte offset into T.
// sets T[i] = x
static inline
void set_T(unsigned char* T,
           const size_t bytes_per_character,
           const sptr_t byte_offset,
           const usptr_t x)
{
  set_be(T+byte_offset, bytes_per_character, x);
}

// returns T[i]. Note that this takes in a byte offset into T.
// Always returns a positive number.
static inline
usptr_t get_T(const unsigned char* T,
             const size_t bytes_per_character,
             const sptr_t byte_offset)
{
  usptr_t ret = get_be(T+byte_offset, bytes_per_character);
  usptr_t ones;
  ones = 0;
  ones = ~ ones; // now all ones.
  ret &= ones >> (8*(sizeof(sptr_t)-bytes_per_character));
  return ret;
}

// returns the number of bytes needed to 
// encode 0 to n-1
// as well as an extra sign bit so that we can
// store negative numbers in that range.
// The sign bit can be used for bit-stealing flags.
static inline
sptr_t pointer_bytes_needed_for(sptr_t n)
{
  int l = num_bits64(n-1);
  l++; // need room for a sign bit.
  return ceildiv(l, 8);
}

// set derived values in a suffix sorting problem.
error_t prepare_problem(suffix_sorting_problem_t* p);

typedef struct suffix_context_s {
  unsigned char* T; // used by get_string_getter.
  int bytes_per_pointer; // used by get_string_getter.
} suffix_context_t;

get_string_fun_t get_string_getter(suffix_context_t* p);
// get a string getter that will complement negative S values
// before looking up the text.
get_string_fun_t get_string_getter_comp(suffix_context_t* p);

void print_suffixes(suffix_sorting_problem_t* p, int len);

// Actual suffix sorting implementations
//error_t dcx_ssort(suffix_sorting_problem_t* p, int period);
//error_t two_stage_ssort(suffix_sorting_problem_t* p, suffix_context_t* context, size_t str_len, compare_fun_t compare, sptr_t max_char);
#endif
