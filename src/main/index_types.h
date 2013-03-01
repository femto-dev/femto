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

  femto/src/main/index_types.h
*/
#ifndef _INDEX_TYPES_H_
#define _INDEX_TYPES_H_ 1

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>

typedef unsigned char uchar;

// The number of special characters we need during block sorting.

// The number of special characters we need. Escape character.
#define NUM_SPECIAL_CHARS 1
// These parts are used in preparing the documents for sorting
// the escape character must be 0, because it, followed by
// ESCAPE_CODE_EOF, must be the smallest string possible.
#define SPECIAL_CHAR_ESCAPE 0

// Escape codes (what follows \0).
enum {
  ESCAPE_CODE_EOF = NUM_SPECIAL_CHARS, // 1
  ESCAPE_CODE_SEOF, // 2
  ESCAPE_CODE_SOH, // 3
  ESCAPE_CODE_EOH, // 4
  NUM_ESCAPE_CODES
};

// a value for invalid when considering alpha_t sequence numbers.
// (ie.. this is the i'th character in use)
#define INVALID_SEQ 0xffff
#define INVALID_DOCUMENT_NUMBER (-1)
#define INVALID_DOCUMENT_OFFSET (-1)
#define INVALID_OFFSET (-1)
//#define HEADER_SECTION_MASK 0x80000000

// alphabet size...
// first ESCAPE_CODE_EOF...ESCAPE_CODE_MARK and ESCAPE_CODE_ESCAPE
// then it's ALPHA_OFFSET + char for each character.
#define ESCAPE_OFFSET 0
// as far as the MTF-encoded is concerned, we're encoding
// only NUM_ESCAPE_CODES extras.
#define CHARACTER_OFFSET (ESCAPE_OFFSET+NUM_ESCAPE_CODES)
#define ALPHA_SIZE (CHARACTER_OFFSET+256)
#define ALPHA_SIZE_DIV16 (CEILDIV(ALPHA_SIZE,16))
#define ALPHA_SIZE_DIV64 (CEILDIV(ALPHA_SIZE,64))
typedef uint16_t alpha_t;
#define ALPHA_SIZE_BITS 9
// A value which can be used for "not set yet" and is ALPHA_SIZE_BITS long
#define INVALID_ALPHA 0x1ff
// 0 and 1 are MTF-RUNA and MTF-RUNB.
#define MTF_ALPHA_SIZE (2+ALPHA_SIZE)

static inline
alpha_t toloweralpha(alpha_t c)
{
  return CHARACTER_OFFSET + tolower(c - CHARACTER_OFFSET);
}
static inline
alpha_t toupperalpha(alpha_t c)
{
  return CHARACTER_OFFSET + toupper(c - CHARACTER_OFFSET);
}

// for re-encoding strings between our 9-bit character set
// and the normal 8-bit one.
// Does not handle any escaping at all.
static inline
alpha_t* strtoalpha(int len, uchar* str)
{
  int i;
  alpha_t* ret = (alpha_t*) malloc(sizeof(alpha_t)*len);

  for( i = 0; i < len; i++ ) {
    ret[i] = CHARACTER_OFFSET + str[i];
  }

  return ret;
}

#define MAX_ALPHATOS 6
// dst must have room for at least 6 characters
// returns the number of characters printed.
static inline
int alphatos(char* dst, alpha_t alpha)
{
  int ch;
  int ret;
  ch = alpha;
  ch -= CHARACTER_OFFSET;
  if( ch < 0 ) ret = sprintf(dst, "\\x-%02x", -ch);
  else {
    if( ch == '\\' || ch == '"' ) ret = sprintf(dst, "\\%c", ch);
    else if( isprint(ch) ) ret = sprintf(dst, "%c", ch);
    else ret = sprintf(dst, "\\x%02x", ch);
  }
  return ret;
}

static inline
void fprint_alpha(FILE* f, int len, alpha_t* pat)
{
  char buf[MAX_ALPHATOS];
  int i;

  for( i = 0; i < len; i++ ) {
    alphatos(buf, pat[i]);
    fprintf(f, "%s", buf);
  }
}


static inline int should_mark(int64_t mark_period, int64_t doc_offset, int64_t doc_end)
{ 
  if( mark_period == 0 ) return 0;
  if( doc_offset == 0 || doc_offset == doc_end - 1 ) {
    return 1;
  } else if ( doc_offset % mark_period == 0 ) {
    return 1;
  } else {
    return 0;
  }
}

#define DEBUG 0

typedef struct {
  int cost_bound; // == cost_bound means not a match.
  int subst_cost;
  int delete_cost;
  int insert_cost;
} regexp_settings_t;

static inline
void set_default_regexp_settings(regexp_settings_t* s)
{
  s->cost_bound = 1; // exact matching only.
  s->subst_cost = 1;
  s->delete_cost = 1;
  s->insert_cost = 1;
}
#endif
