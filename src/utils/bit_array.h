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

  femto/src/utils/bit_array.h
*/
#ifndef _BIT_ARRAY_H_
#define _BIT_ARRAY_H_

#include <stdlib.h>
#include <inttypes.h>
#include <stdio.h>

// bit array functions.


// allocates a zero-d out bit array 
// with len 64-bit chunks.
static inline
uint64_t* allocate_bit_array(int len)
{
  uint64_t* ret;
  ret = (uint64_t*) malloc(len*sizeof(uint64_t));
  for( int i = 0; i < len; i++ ) ret[i] = 0;
  return ret;
}

static inline
void free_bit_array(uint64_t* bits)
{
  free(bits);
}

// in the bit_array functions, the bits
// are stored most-significant-bit first (in slot 0)
// len is the number of 64-bit words.
// puts a 1 in the top slot.
static inline
void bit_array_right_shift_one(int len, uint64_t* bits)
{
  unsigned char bit = 1;
  uint64_t lastbit = 1;

  for( int i = 0; i < len; i++ ) {
    bit = bits[i] & 1; // bit that will fall off.
    bits[i] >>= 1;
    // put the bit that fell off in the top.
    bits[i] |= lastbit << 63;
    lastbit = bit;
  }
}

static inline
void bit_array_and(int len, uint64_t* dst, uint64_t* a, uint64_t* b )
{
  for( int i = 0; i < len; i++ ) {
    dst[i] = a[i] & b[i];
  }
}

static inline
void bit_array_or(int len, uint64_t* dst, uint64_t* a, uint64_t* b )
{
  for( int i = 0; i < len; i++ ) {
    dst[i] = a[i] | b[i];
  }
}

static inline
void bit_array_xor(int len, uint64_t* dst, uint64_t* a, uint64_t* b)
{
  for( int i = 0; i < len; i++ ) {
    dst[i] = a[i] ^ b[i];
  }
}

static inline
int bit_array_cmp(int len, uint64_t* a, uint64_t* b)
{
  for( int i = 0; i < len; i++ ) {
    if( a[i] < b[i] ) return -1;
    if( a[i] > b[i] ) return 1;
  }
  return 0;
}

/**
  Returns 1 if a & b != 0, or 0 otherwise.
  */
static inline
int bit_array_check_mask(int len, uint64_t* a, uint64_t* b )
{
  for( int i = 0; i < len; i++ ) {
    if( a[i] & b[i] ) return 1;
  }
  return 0;
}


/**
  Returns 1 if it is empty, or 0 otherwise
 */
static inline
int bit_array_check_empty (int len, uint64_t* a)
{
  for( int i = 0; i < len; i++ ) {
    if( a[i] ) return 0;
  }
  return 1;
}


static inline
void fprint_bit_array (FILE* f, int len, uint64_t *array, int number)
{
  int loop = 64;
  for (int i = 0; i < len && number > 0; i++) {
    if (number < 64) {
      loop = number;  }
    for (int j = 0; j < loop; j++) {
      if ( 1 & (array[i] >> (63 - j)) ) {
	fprintf(f, " 1"); }
      else {
	fprintf(f, " 0"); }
    }
    number -= 64;
  }
}
static inline
void print_bit_array (int len, uint64_t *array, int number)
{
  fprint_bit_array(stdout, len, array, number);
}

static inline
int get_bit (int len, uint64_t *array, int element)
{
  int array_index = element / 64;
  element -= (array_index * 64);

  return 1 & (array[array_index] >> (63 - element));
}

static inline
void set_bit (int len, uint64_t *array, int element)
{
  uint64_t one = 1;
  
  int array_index = element / 64;
  element -= (array_index * 64);

  array[array_index] |= one << ( 63 - element );
}

static inline
void clear_bit (int len, uint64_t *array, int element)
{
  uint64_t one = 1;
  
  int array_index = element / 64;
  element -= (array_index * 64);

  array[array_index] &= ~( one << (63 - element) );
}

static inline
void assign_bit (int len, uint64_t* array, int element, int bit)
{
  if( bit ) set_bit(len, array, element);
  else clear_bit(len, array, element);
}

static inline
void invert_bit_array(int len, uint64_t *array)
{
  for(int i = 0; i < len; i++ ) {
    array[i] = ~ array[i];
  }
} 

#endif
