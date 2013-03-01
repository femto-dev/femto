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

  femto/src/main/bwt_creator.h
*/
#ifndef _BWT_CREATOR_H_
#define _BWT_CREATOR_H_ 1

#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

#include "index_types.h"
#include "error.h"
#include "buffer.h"

#include "bwt_writer.h"
#include "bwt_prepare.h"

typedef struct {
  buffer_t buf;
  void* extra;
} suffix_array_t;

typedef struct {
  // set up before call
  int64_t mark_period;
  prepared_text_t* p;
  suffix_array_t* sa;
  // NULL before call; set in init_sa_reader
  void* state;
  unsigned int idx;
  int64_t offset; // in bwt numbering
  int64_t offset_ssort; // in prepared_text numbering
  alpha_t L; // the character before the offset
  unsigned char mark; // 1 if marked, 0 if not.
  int64_t doc; // the document number.
} sa_reader_t;

// fixes a reader to be using the bwt-offset and fixes the L character
// to handle the SSORT_ONLY_CHARS.
// returns 0 if the row should be skipped, 1 if not.
static inline
char reader_to_bwt(sa_reader_t* r)
{
  if( r->L == ESCAPE_CODE_EOF ) {
    r->L = ESCAPE_CODE_SEOF;
  }
  r->offset_ssort = r->offset;

  return 1;
}


///// Functions that must be implemented elsewehere and linked in!
/*
   Allocates the required space and does the suffix sorting on the
   prepared text.
   */
extern error_t suffix_sort(prepared_text_t* p, suffix_array_t* sa);
/*
   Initializes an sa-reader. Already it will have prepared_text
   and suffix_array pointers set.
   */
extern error_t init_sa_reader(sa_reader_t* r);
/*
   Reads an entry from the suffix array, updating any state as
   necessary. Returns the character before that offset (L[i]) as
   well as SA[i].
   Returns 1 if we got a entry; 0 if we're done
   */
extern int read_sa_bwt(sa_reader_t* r);
extern void free_sa_reader(sa_reader_t* r);
extern void free_suffix_array(suffix_array_t* sa);

/*
   Handy interface function to do it all..
   */
error_t save_prepared_bwt(prepared_text_t* p,
                          int mark_period,
                          FILE* bwt_out,
                          int chunk_size, FILE* map_out,
                          int verbose);
#endif
