#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "config.h"
#include "femto_internal.h"
#include "error.h"
#include "json.h"

// dst must have space for MAX_JSON per character.
int encode_ch_json(char* dst, int ch)
{
  int k;

  k = 0;
  switch( ch ) {
    case '"':
      dst[k++] = '\\';
      dst[k++] = '"';
      break;
    case '\\':
      dst[k++] = '\\';
      dst[k++] = '\\';
      break;
    default:
      if( isprint(ch) ) dst[k++] = ch;
      else {
        // encode with \u
        dst[k++] = '\\';
        dst[k++] = 'u';
        k += sprintf(&dst[k], "%04x", ch);
      }
      break;
  }
  dst[k] = '\0';
  return k;
}

int encode_str_json(char* dst, const char* str)
{
  int i,k;
  k = 0;
  for( i=0; str[i]; i++ ) {
    k += encode_ch_json(&dst[k], str[i]);
  }
  return k;
}

// dst must have space for MAX_JSON characters
int encode_alpha_json(char* dst, alpha_t alpha)
{
  char buf[MAX_ALPHATOS];
  alphatos(buf, alpha);
  return encode_str_json(dst, buf);
}

void fprint_alpha_json(FILE* f, int len, alpha_t* pat)
{
  char buf[MAX_JSON];
  int i;

  for( i = 0; i < len; i++ ) {
    encode_alpha_json(buf, pat[i]);
    fprintf(f, "%s", buf);
  }
}

void fprint_str_json(FILE* f, int len, const unsigned char* str)
{
  char buf[MAX_JSON];
  int i;
 
  for( i = 0; i < len; i++ ) {
    encode_ch_json(buf, str[i]);
    fprintf(f, "%s", buf);
  }
}

void fprint_cstr_json(FILE* f, const char* str)
{
  char buf[MAX_JSON];
  int i;
 
  for( i = 0; str[i]; i++ ) {
    encode_ch_json(buf, str[i]);
    fprintf(f, "%s", buf);
  }
}

