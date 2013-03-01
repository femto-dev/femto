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

  femto/src/main/bwt_ds_ssort.c
*/

/* This is an interface file to work with Deep-Shallow Suffix Sorter
 * by Giovanni Manzini. FEMTO does not currently use this file since
 * it includes its own suffix array construction.
 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "bwt.h"
#include "bwt_creator.h"
#include "ds_ssort.h"
#include "buffer.h"
#include "buffer_funcs.h"
#include "wtree_funcs.h"

// NOTE that the code here can only be used
// for indexing collections with a prepared length
// less than 2 GB, because of limitations in the ds_ssort
// implementation.

// However, besides that limitation, every effort is
// made here to use as little memory as possible.

// The prepared text has zeros in it; zero is the escape character.
#define ESCAPE_CODE 0
// Any time the ESCAPE_CODE appears, it must be followed by
// a second number. If it is NUM_ESCAPE_CODES, it is a literal 0.
// Otherwise, it is the escape code that appears as the second
// number.

typedef uint32_t offset_t;

// This would have to be 64-bit for > 4GB collection
typedef struct {
  offset_t prepared_offset;
  offset_t offset;
} mark_info_t;

typedef struct {
  offset_t max_marks;
  offset_t num_marks;
  mark_info_t* marks; // sorted in prepared_offset order.
  int64_t max_docs;
  int64_t num_docs;
  int64_t* doc_ends_prepared;
  int64_t* doc_ends_prepared_bwt;
  offset_t last_prepared_offset;
  offset_t prepared_offset;
  offset_t prepared_after_seof;
} prep_extra_t;

// returns a character code, or -1 if it's an invalid row.
static inline alpha_t get_L_char_from_offsets(int i,
    int64_t ndocs, int64_t* doc_ends,
    int* offsets, unsigned char* text, int64_t* doc_num)
{
  int el;
  int doc;
  int doc_start,doc_end;
  unsigned char buf[3];

  // get the character before offsets[i] taking
  // documents into account.
  el = offsets[i];

  // note that this document tracking isn't strictly necessary
  // since the last character is always EOF no matter which 
  // document it is.
  doc = bsearch_int64_doc(ndocs, doc_ends, el);
  if( doc > 0 ) doc_start = doc_ends[doc-1];
  else doc_start = 0;
  doc_end = doc_ends[doc];

  *doc_num = doc;

  // figure out the character at the offset as well as the
  // three previous characters and put them in buf.
  for( int i = 2; i >= 0; i-- ) {
    // grab the character.
    buf[i] = text[el];
    // go back one.
    if( el == doc_start ) {
      el = doc_end - 1;
    } else el--;
  } 

  // now we have a little buffer of context.
  // buf[2] contains the character at the offset.
  // We want the character before that one.


  if( buf[1] == ESCAPE_CODE ) {
    // If this row doesn't start on an even character,
    // return INVALID_ALPHA
    return INVALID_ALPHA;
  } else {
    // 1,2 are different characters.
    // that means that the character before is 1
    // but 1 could be escaped.
    if( buf[0] == ESCAPE_CODE ) {
      // 0,1 are a single character.
      // L[i] = (escaped) 1
      return buf[1];
    } else {
      // 1 is a character on its own.
      return NUM_ESCAPE_CODES+buf[1];
    }
  }
}

/*
   Appends a marking for p->num_chars and p->extra->prepared_offset
   if there's not already a marking for p->num_chars.
   */
error_t append_marking(prepared_text_t* p,
                       offset_t prepared_offset,
                       offset_t offset)
{
  prep_extra_t* extra = (prep_extra_t*) p->extra;


  // if the last mark is what this would be, do nothing.
  if( extra->num_marks > 0 &&
      extra->marks[extra->num_marks - 1].offset == offset ) {
    return ERR_NOERR;
  }

  // otherwise, we might need to realloc.
  if( extra->num_marks + 1 > extra->max_marks ) {
    extra->max_marks += 128 + extra->num_marks;
    extra->marks = (mark_info_t*) realloc(extra->marks,
                           extra->max_marks * sizeof(mark_info_t));
    if( ! extra->marks ) return ERR_MEM;
  }

  // Finally, store the marking data.
  extra->marks[extra->num_marks].prepared_offset = prepared_offset;
  extra->marks[extra->num_marks].offset = offset;

  extra->num_marks++;

  return ERR_NOERR;
}

// Given a prepared text offset in offset, do binary search to find the
// appropriate marking that has prepared_offset == target..
mark_info_t* get_mark_info(int num_marks, mark_info_t* marks, unsigned int target)
{
  int a, b, middle;

  // check the first and the last.
  if( num_marks == 0 ) return NULL;
  if( target < marks[0].prepared_offset ) return NULL;
  if( target == marks[0].prepared_offset ) return &marks[0];

  if( target > marks[num_marks-1].prepared_offset ) return NULL;
  if( target == marks[num_marks-1].prepared_offset ) return &marks[num_marks-1];

  a = 0;
  b = num_marks - 1;
  // always we have that arr[a] <= target < arr[b].

  // divide the search space in half
  while(b - a > 1) {
    middle = (a + b) / 2;
    if( target < marks[middle].prepared_offset ) b = middle;
    else if( target == marks[middle].prepared_offset ) {
      return &marks[middle];
    } else a = middle; // arr[middle] <= target
  }

  if( target == marks[b].prepared_offset ) return &marks[b];
  return NULL;
}

/*
   Appends the character ch to newtext. j is the "saved offset".
   Returns the new value for j.
   */
extern error_t prepare_put_char(prepared_text_t* p, alpha_t ch)
{
  prep_extra_t* extra;
  error_t err;

  if( ! p->extra ) {
    p->extra = malloc(sizeof(prep_extra_t));
    memset(p->extra, 0, sizeof(prep_extra_t));
  }
  extra = (prep_extra_t*) p->extra;

  err = buffer_extend(&p->buf, 2);
  if( err ) return err;

  // maybe append a marking?
  if( bwt_should_mark(p, p->num_chars_ssort) ) {
    err = append_marking(p, extra->prepared_offset,
                         p->num_chars_ssort);
    if( err ) return err;
  }

  extra->last_prepared_offset = extra->prepared_offset;

  // if it's less than or equal to NUM_ESCAPE_CODES, first put a 0.
  if( ch == INVALID_ALPHA ) return ERR_PARAM;
  if( ch <= NUM_ESCAPE_CODES ) { // put an escaped character
    p->buf.data[p->buf.len++] = ESCAPE_CODE;
    extra->prepared_offset++;
    p->buf.data[p->buf.len++] = ch; 
    extra->prepared_offset++;
  } else {
    alpha_t a;
    a = ch - NUM_ESCAPE_CODES;
    if( a > 0xff ) return ERR_PARAM;
    p->buf.data[p->buf.len++] = a;
    extra->prepared_offset++;
  }

  if( ch == ESCAPE_CODE_SEOF ) {
    extra->prepared_after_seof = extra->prepared_offset;
  }

  return ERR_NOERR;
}

extern void free_prepared_text_extra(prepared_text_t* p)
{
  prep_extra_t* extra = (prep_extra_t*) p->extra;

  if( extra ) {
    free(extra->marks);
    free(extra->doc_ends_prepared);
    free(extra->doc_ends_prepared_bwt);
    free(extra);
  }
}

extern error_t prepare_end_document(prepared_text_t* p) 
{
  prep_extra_t* extra = (prep_extra_t*) p->extra;

  assert(extra->num_docs == p->num_docs );

  if( extra->num_docs + 1 > extra->max_docs ) {
    extra->max_docs += 128 + extra->num_docs;
    extra->doc_ends_prepared = (int64_t*) realloc(extra->doc_ends_prepared,
                                       extra->max_docs * sizeof(int64_t));
    if( ! extra->doc_ends_prepared ) return ERR_MEM;
    extra->doc_ends_prepared_bwt = (int64_t*) realloc(extra->doc_ends_prepared_bwt,
                                       extra->max_docs * sizeof(int64_t));
    if( ! extra->doc_ends_prepared_bwt ) return ERR_MEM;
  }

  extra->doc_ends_prepared[extra->num_docs] = extra->prepared_offset;
  extra->doc_ends_prepared_bwt[extra->num_docs] = extra->prepared_after_seof;
  extra->num_docs++;

  return ERR_NOERR;

}

/*
   Allocates the required space and does the suffix sorting on the
   prepared text.
   */
extern error_t suffix_sort(prepared_text_t* p, suffix_array_t* sa, int64_t occs[ALPHA_SIZE])
{
  error_t err;
  int overshoot;
  int* offsets;
  long olen;

  overshoot = init_ds_ssort(500,2000);
  if( overshoot == 0 ) return ERR_INVALID_STR("Could not init_ds_ssort");
  // make sure there's room for overshoot at the end of the text.
  err = buffer_extend(&p->buf, overshoot);
  if( err ) return err;

  olen = p->buf.len * sizeof(int);
  offsets = (int*) malloc(olen);
  if( ! offsets ) return ERR_MEM;

  ds_ssort(p->buf.data, offsets, p->buf.len);

  sa->buf = build_buffer(olen, (unsigned char*) offsets);
  sa->buf.len = olen;

  return ERR_NOERR;
}
/*
   Initializes an sa-reader. Already it will have prepared_text
   and suffix_array pointers set.
   */
extern error_t init_sa_reader(sa_reader_t* r)
{
  // the suffix array is actually an array of bytes, some of which
  // are 2-byte escape sequences.
  r->idx = 0; // index into prepared sequence.
  return ERR_NOERR;
}

/*
   Reads an entry from the suffix array, updating any state as
   necessary. Returns the character before that offset (L[i]) as
   well as the value of the offset.
   Returns 1 if there's more, 0 if we're done.
   */
extern int read_sa_bwt(sa_reader_t* r)
{
  int* sa = (int*) r->sa->buf.data;
  unsigned char* text = r->p->buf.data;
  prep_extra_t* extra = (prep_extra_t*) r->p->extra;
  mark_info_t* m;
  unsigned int offset;
  int ret_ch;
  int64_t doc_num = -1;

  do {
    // no results if we're at the end!
    if( 4 * r->idx >= r->sa->buf.len ) return 0;

    offset = sa[r->idx];
    ret_ch = get_L_char_from_offsets(r->idx, r->p->num_docs, extra->doc_ends_prepared, sa, text, &doc_num);
    r->idx++;
  } while ( ret_ch == INVALID_ALPHA ||
            offset >= extra->doc_ends_prepared_bwt[doc_num] );

  r->L = ret_ch;
  r->doc = doc_num;

  // figure out if we should mark, and also what the offset
  // should be

  m = get_mark_info(extra->num_marks, extra->marks, offset);
  if( m ) {
    assert( m->prepared_offset == offset );
    r->offset = m->offset;
    r->mark = 1;
  } else {
    r->offset = -1;
    r->mark = 0;
  }

  ret_ch = reader_to_bwt(r);
  assert(ret_ch);
  return 1;

}

extern void free_sa_reader(sa_reader_t* r)
{
  
}
extern void free_suffix_array(suffix_array_t* sa)
{
}

