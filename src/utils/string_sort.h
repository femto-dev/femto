/*
  (*) 2007-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils/string_sort.h
*/


#ifndef _STRING_SORT_H_
#define _STRING_SORT_H_

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "error.h"

typedef unsigned char* string_t;

typedef string_t (*get_string_fun_t)(const void* context, const void* data_ptr);
typedef int (*compare_fun_t)(const void* context, const void * a, const void * b);

typedef struct string_sort_params_s {
  void* context;
  unsigned char* base;
  size_t n_memb;
  size_t memb_size;
  size_t str_len; // the number of bytes of string we are to sort.
                  // this many bytes must be readable for each string.
  size_t same_depth; // the number of bytes of string that are
                     // going to be equal for all strings
  get_string_fun_t get_string;
  compare_fun_t compare;
  int parallel;
} string_sort_params_t;

typedef error_t (*string_sort_fun_t) (string_sort_params_t* params);


// Swap n bytes between a and b.
static inline void swap_bytes(unsigned char *a, unsigned char *b, size_t n)
{
  // On a 32-bit machine, adding this loop DID help
  // performance quite a bit.
  while (n >= sizeof(unsigned long)) {
    unsigned long tmp = *(unsigned long *)a;
    *(unsigned long *)a = *(unsigned long *) b;
    *(unsigned long *)b = tmp;
    n -= sizeof(unsigned long);
    a += sizeof(unsigned long);
    b += sizeof(unsigned long);
  }
  
  while (n > 0) {
    unsigned char tmp = *(unsigned char*) a;
    *(unsigned char*) a = *(unsigned char*) b;
    *(unsigned char*) b = tmp;
    n--;
    a += 1;
    b += 1;
  }
}

void print_strings(const string_sort_params_t* params, void* base, size_t n_memb);

// string sorters:
//error_t insertion_sort (string_sort_params_t* params);
//error_t multikey_qsort (string_sort_params_t* params);
//error_t bucket_trie_sort(string_sort_params_t* params);

// defined in utils_cc/string_sort.cc in 
error_t bucket_sort(string_sort_params_t* params);

#define favorite_string_sort(params) bucket_sort(params)

#endif
