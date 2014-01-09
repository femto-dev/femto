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

  femto/src/utils/util.h
*/
#ifndef _UTIL_H_
#define _UTIL_H_

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include "error.h"
#include "bit_funcs.h"

error_t append_array_internal(int* count, void** array, size_t member_size, void* newValue);

/* This macro is to avoid gcc's "dereferencing type-punned pointer" error.
   The intermediate cast to (void*) makes it feel better.. */
#define append_array(count, array, member_size, new_value) \
  append_array_internal(count, (void**) (void*) array, member_size, new_value)

int compare_int64(const void* aP, const void* bP);
int compare_uint64(const void* aP, const void* bP);

error_t write_int32(FILE* f, uint32_t x);
error_t write_int64(FILE* f, uint64_t x);
error_t write_int8(FILE* f, unsigned char x);
error_t write_string(FILE* f, char* s);
error_t pad_to_align(FILE* f);

// returns the new length of the array.
size_t sort_dedup(void* baseIn, size_t nmemb, size_t size,
                  int(*compar)(const void*, const void*));
size_t sort_dedup_free(void* baseIn, size_t nmemb, size_t size,
                       int(*compar)(const void*, const void*),
                       void(*freef)(void*));


void reverse_array(void* array, size_t nmemb, size_t size);

static inline
void reverse_character_string(char* string)
{
  reverse_array(string, strlen(string), sizeof(char));
}

error_t mkdir_if_needed(const char* path);

struct val_unit {
  double value;
  int unit;
};

struct val_unit get_unit(double value);
char* get_unit_prefix(struct val_unit x);

error_t resize_file(int fd, off_t length);

/* Finds and returns the integer index i
   such that 
   ntoh_32(arr[i]) <= target < ntoh_32(arr[i+1]).
   May return -1 for i; in that case, target < ntoh(arr[0]).
   May return n-1 for i; in that case, ntoh_32(arr[n-1]) <= target.
   Assumes that arr is a sorted list of numbers in network byte order.
*/
long bsearch_uint32_ntoh_arr(long n, uint32_t* arr, uint32_t target);

/* Finds and returns the integer index i
   such that 
   ntoh_64(arr[i]) <= target < ntoh_64(arr[i+1]).
   May return -1 for i; in that case, target < ntoh(arr[0]).
   May return n-1 for i; in that case, ntoh_64(arr[n-1]) <= target.
   Assumes that arr is a sorted list of numbers in network byte order.
*/
long bsearch_int64_ntoh_arr(long n, int64_t* arr, int64_t target);

uint64_t gcd64(uint64_t a, uint64_t b);
uint64_t lcm64(uint64_t a, uint64_t b);

typedef struct uint64_recip {
  int sh1;
  int sh2;
  uint64_t mprime;
} uint64_recip_t;


#ifdef _64_BIT

#define HAS_RECIPROCAL_DIVIDE 1

uint64_recip_t compute_reciprocal(uint64_t d);

static inline
uint64_t reciprocal_divide(uint64_t n, uint64_recip_t inv)
{
  // See Division by Invariant Integers using Multiplication
  // by Granlund and Montgomery
  uint64_t hi, lo;
  uint64_t ret;

  mul64_128(n, inv.mprime, hi, lo);

  ret = (hi + ((n - hi) >> inv.sh1)) >> inv.sh2;

  return ret;
}
#else

#warning reciprocal_divide not defined, not 64-bit

static inline
uint64_recip_t compute_reciprocal(uint64_t d)
{
  uint64_recip_t ret;
  ret.mprime = d;
}

static inline
uint64_t reciprocal_divide(uint64_t n, uint64_recip_t inv)
{
  return n / inv.mprime;
}

static inline
uint64_recip_t compute_reciprocal(uint64_t d)
{
  uint64_recip_t ret;
  ret.mprime = d;
}

static inline
uint64_t reciprocal_divide(uint64_t n, uint64_recip_t inv)
{
  return n / inv.mprime;
}

#endif

#endif

