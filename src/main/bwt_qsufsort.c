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

  femto/src/main/bwt_qsufsort.c
*/

/* This file is a suffix-sorting interface for qsufsort by Jesper Larsson.
 * FEMTO includes its own suffix sorting implementation, but this code
 * is used for some unit tests. */

#include <stdio.h>
#include <stdlib.h>

#include <inttypes.h>

#include "bswap.h"
#include "bwt.h"
#include "bwt_creator.h"

//typedef long int qsuf_int;
typedef int qsuf_int;

//typedef int prepared_alpha_t;
// just like 
typedef uint16_t prepared_alpha_t;
// always store prepared_alpha in big-endian (network) byte
// order so that byte-oriented suffix sorting methods will work.
//#define hton_palpha( XX ) ( hton_16( XX ) )
//#define ntoh_palpha( XX ) ( ntoh_16( XX ) )
#define hton_palpha( XX ) ( XX )
#define ntoh_palpha( XX ) ( XX )


static inline alpha_t get_F_char_from_offsets(int i,
    qsuf_int* offsets, prepared_alpha_t* text)
{
  qsuf_int el;
  qsuf_int ch;

  el = offsets[i];
  ch = ntoh_palpha(text[el]);

  return ch;
}

// returns a character code, or -1 if it's an invalid row.
static inline alpha_t get_L_char_from_offsets(
    qsuf_int i, qsuf_int* offsets, prepared_alpha_t* text
    )
{
  qsuf_int el;
  alpha_t ch;

  // get the character before offsets[i] taking
  // documents into account. Since all documents
  // end with the same character, we can just put
  // the character before..
  el = offsets[i];

  if( el == 0 ) {
    ch = ESCAPE_OFFSET + ESCAPE_CODE_SEOF;
  } else {
    ch = ntoh_palpha(text[el-1]);
  }

  return ch;
}



// return the number of prepared_alpha_t entries
// needed in the text array passed
// to build_index given the text length.
int get_text_space_extra(int text_length)
{
  return text_length + 3;
}

#define QSUFSORT
//#define QSORT

#ifdef BZ_SORT
#include "blocksort.c"
error_t bz_block_sort(int* offsets, int text_length, unsigned char* text)
{
  EState* s;

  s = malloc(sizeof(EState));
  if( ! s ) return ERR_MEM;

  memset(&s, 0, sizeof(EState));
  s->ftab = malloc( 65537 * sizeof(UInt32) );
  s->nblock = text_length;
  s->verbosity = 10;
  s->workFactor = 30;
  s->arr1 = offsets;
  s->ptr = (UInt32*) s->arr1; // offsets
  s->arr2 = (UInt32*) text; // (n+BZ_N_OVERSHOOT)*4
  s->block = (UChar*) s->arr2; // text
  s->origPtr = 0;

  if( ! s->ftab ) {
    free( s );
    return ERR_MEM;
  }

  BZ2_blockSort( s );

  free(s->ftab);

  return ERR_NOERR;
}
#endif 

#ifdef QSORT
//static prepared_alpha_t* qsort_text;
static unsigned char* qsort_text;
static int qsort_text_length;

int qsort_compar_suffix(const void* aptr, const void* bptr) 
{
  int a = * (int*) aptr;
  int b = * (int*) bptr;
  int lenA, lenB, minlen;
  int ret;

  lenA = qsort_text_length - a;
  lenB = qsort_text_length - b;

  minlen = (lenA < lenB) ? (lenA) : (lenB);

  ret = memcmp( &qsort_text[a], &qsort_text[b], minlen );
  if( ret == 0 ) {
    if( lenA < lenB ) ret = -1;
    else if( lenA > lenB ) ret = 1;
    else ret = 0;
  }

  return ret;

}

error_t qsort_block_sort(int* offsets, int prepared_length, int text_length, unsigned char* text)
{
  qsort_text = text;
  qsort_text_length = text_length;

  // try with qsort
  // first, create the offsets.
  for( int i = 0; i < prepared_length; i++ )
    offsets[i] = sizeof(prepared_alpha_t)*i;

  // next, sort the offsets.
  qsort(offsets, prepared_length, sizeof(int), qsort_compar_suffix);

  for( int i = 0; i < prepared_length; i++ ) {
    offsets[i] /= sizeof(prepared_alpha_t);
  }
  return ERR_NOERR;
}
#endif

#ifdef QSUFSORT
#include "qsufsort.c"
#endif

// allocates offsets as appropriate.
error_t block_sort(qsuf_int** offsetsPtr, qsuf_int text_length, prepared_alpha_t* text, int64_t occs[ALPHA_SIZE])
{
  error_t err;
  qsuf_int* offsets;

  err = ERR_NOERR;

  // only handles 32-bit texts.
  //if( text_length > 0x7fffffff ) return ERR_PARAM;

#ifdef QSUFSORT
  {
    prepared_alpha_t temp;
    qsuf_int k,l;
    qsuf_int* x;
   
    if( occs ) {
      k = -1;
      //l = ALPHA_SIZE + 1;
      l = ESCAPE_CODE_EOF; // always the smallest..
      // but isn't counted in Occs because it's the "ghost" end character..
      // find k and l.
      for(int i = 0; i < ALPHA_SIZE; i++ ) {
        if( occs[i] > 0 ) {
          if( i < l ) l = i; // l is the minimum
          if( i > k ) k = i; // k is the maximum
        }
      }
      if( k == -1 ) return ERR_PARAM; // bad occs data!
      // k is the maximum value attained - add one for our call.
      k++;
    } else {
      k = ALPHA_SIZE;
      l = 0;
    }
    x = (qsuf_int*) malloc(get_text_space_extra(text_length+1)*sizeof(qsuf_int));
    if( ! x ) return ERR_MEM;
    offsets = (qsuf_int*) malloc(get_text_space_extra(text_length+1)*sizeof(qsuf_int));
    if( ! offsets ) {
      //free(x);
      return ERR_MEM;
    }

    // set up x.
    for(qsuf_int i = 0; i < text_length; i++ ) {
      temp = ntoh_palpha(text[i]);
      x[i] = temp;
    }
    x[text_length] = 0; // special end marker we're going to ignore.

    if( ! k > l ) return ERR_INVALID;
    // let's do larsson's qsufsort!
    suffixsort(x, offsets, text_length, k, l);
    // take out the end marker, which must show up as the first char.
    for( qsuf_int i = 0; i < text_length; i++ ) {
      offsets[i] = offsets[i+1];
    }

    free(x);

    *offsetsPtr = offsets;
  }
#endif

#ifdef QSORT
  {
    int byte_length;
    offsets = malloc(text_length * sizeof(int));
    if( ! offsets ) return ERR_MEM;

    byte_length = sizeof(prepared_alpha_t) * text_length;
    err = qsort_block_sort(offsets, text_length, byte_length, (unsigned char*) text);

    *offsetsPtr = offsets;
  }
#endif

#ifdef BZ_SORT
  {
    offsets = malloc((text_length * sizeof(prepared_alpha_t) + BZ_N_OVERSHOOT) * sizeof(int));
    if( ! offsets ) return ERR_MEM;

    byte_length = sizeof(prepared_alpha_t) * text_length;

    err = bz_block_sort(offsets, byte_length, (unsigned char*) text);

    { int i,j;
      // remove the non-aligned ones.
      for( j = 0, i = 0; i < byte_length; i++ ) {
        if( offsets[i] % sizeof(prepared_alpha_t) == 0 ) {
          // only care about ones that are at an even multiple..
          offsets[j++] = offsets[i] / sizeof(prepared_alpha_t);
        }
      }

      if( j != text_length ) {
        free(offsets);
        return ERR_INVALID;
      }
    }

    *offsetsPtr = realloc(offsets, text_length * sizeof(int));
    }
#endif

  return err;
}


/*
   Appends the character ch to newtext. j is the "saved offset".
   Returns the new value for j.
   */
extern error_t prepare_put_char(prepared_text_t* p, alpha_t ch)
{
  prepared_alpha_t* dst;

  if( p->buf.len + sizeof(prepared_alpha_t) > p->buf.max ) {
    p->buf.max += p->buf.len + sizeof(prepared_alpha_t)*(128 + get_text_space_extra(p->buf.len/sizeof(prepared_alpha_t)));
    p->buf.data = (unsigned char*) realloc(p->buf.data, p->buf.max);
    if( ! p->buf.data ) return ERR_MEM;
  }
  dst = (prepared_alpha_t*) &p->buf.data[p->buf.len];
  *dst = hton_palpha(ch);
  p->buf.len += sizeof(prepared_alpha_t);

  return ERR_NOERR;
}
extern error_t prepare_end_document(prepared_text_t* p)
{
  return ERR_NOERR;
}
extern void free_prepared_text_extra(prepared_text_t* p)
{
}

alpha_t prepared_char_at(prepared_text_t* p, int64_t idx)
{
  prepared_alpha_t* text;
  text = (prepared_alpha_t*) p->buf.data;
  if( sizeof(prepared_alpha_t) * idx >= p->buf.len ) return INVALID_ALPHA;
  else return ntoh_palpha(text[idx]);
}
/*
   Allocates the required space and does the suffix sorting on the
   prepared text.
   */
extern error_t suffix_sort(prepared_text_t* p, suffix_array_t* sa)
{
  error_t err;
  prepared_alpha_t* arr;
  qsuf_int text_length;
  qsuf_int* offsets=NULL;

  text_length = p->buf.len / sizeof(prepared_alpha_t);
  arr = (prepared_alpha_t*) p->buf.data;
  err = block_sort(&offsets, text_length, arr, NULL);
  if( err ) return err;

  sa->buf.data = (unsigned char*) offsets;
  sa->buf.max = sizeof(qsuf_int)*text_length;
  sa->buf.len = sizeof(qsuf_int)*text_length;

  return ERR_NOERR;
}
/*
   Initializes an sa-reader. Already it will have prepared_text
   and suffix_array pointers set.
   */
extern error_t init_sa_reader(sa_reader_t* r)
{
  // the suffix array is actually an array of 4-byte integers.
  r->state = r->sa->buf.data;
  return ERR_NOERR;
}

/*
   Reads an entry from the suffix array, updating any state as
   necessary. Returns the character before that offset (L[i]) as
   well as the value of the offset.
   Returns 1 we returned a character, 0 if we're at the end.
   */
extern int read_sa_bwt(sa_reader_t* r)
{
  qsuf_int* sa;
  prepared_alpha_t* text = (prepared_alpha_t*) r->p->buf.data;
  int64_t doc_num = -1;
  error_t err;
  unsigned char mark = 0;

  do {
    sa = (qsuf_int*) r->state;
    if( ((uintptr_t)r->state - (uintptr_t)r->sa->buf.data) >= r->sa->buf.len ) {
      return 0;
    } 

    r->L = get_L_char_from_offsets(0, sa, text);
   
    r->state = (void*) ((uintptr_t) r->state + sizeof(qsuf_int));
    //r->state += sizeof(qsuf_int); // move to the next one.

    // Find the document number.
    err = prepared_find_doc( r->p, *sa, &doc_num );
    die_if_err(err);
    
    // now decide - do we give the offset or set it to -1 because
    // this character shouldn't be marked?
    // first - compute which document we're in.
    err = bwt_should_mark_doc( r->p, r->mark_period, *sa, doc_num, &mark);
    die_if_err(err);

    r->mark = mark;
    r->offset = *sa;
    r->doc = doc_num;
    //printf("Moving %i %i to bwt\n", r->offset, r->L);
  } while ( ! reader_to_bwt(r) );
  return 1;

}

extern void free_sa_reader(sa_reader_t* r)
{
  
}
extern void free_suffix_array(suffix_array_t* sa)
{
  free(sa->buf.data);
}

