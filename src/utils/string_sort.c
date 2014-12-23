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

  femto/src/utils/string_sort.c
*/


#include <stdio.h>
#include <ctype.h>
#include "string_sort.h"

void print_strings(const string_sort_params_t* params, void* base, size_t n_memb) {
  int i,j;
  string_t s;
  const size_t memb_size = params->memb_size;
  void* p;
  for( i = 0; i < n_memb; i++ ) {
    p = base + i*memb_size;
    printf("@");
    printf("%p", p);
    printf("(");
    for( j = 0; j < memb_size; j++ ) {
      printf("%02x", *(unsigned char*)(p+j));
    }
    printf(") ");
    s = params->get_string(params->context, p);
    for( j = 0; j < params->str_len; j++ ) {
      printf("%02x", s[j]);
    }
    // now try to print in ASCII
    printf(" ");
    for( j = 0; j < params->str_len; j++ ) {
      if( s[j] && isalnum(s[j]) ) printf("%c", s[j]);
      else printf(".");
    }

    printf("\n");
  }
}

