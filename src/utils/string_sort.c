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

