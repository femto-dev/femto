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

  femto/src/main/wtree_funcs.h
*/
#ifndef _WTREE_FUNCS_H_
#define _WTREE_FUNCS_H_ 1

#include <assert.h>

#include "error.h"
#include "buffer.h"
#include "bswap.h"
#include "wtree.h"
#include "buffer_funcs.h"

// ie.. 128 bytes per segment.
#define SEGMENT_WORDS 8
#define SEGMENT_BITS (SEGMENT_WORDS*64)

typedef struct { uint64_t words[SEGMENT_WORDS]; } seg_t;

static inline seg_t ntoh_seg(seg_t s)
{
  seg_t ret;
  for( int i = 0; i < SEGMENT_WORDS; i++ ) {
    ret.words[i] = ntoh_64(s.words[i]);
  }
  return ret;
}

static inline seg_t hton_seg(seg_t s)
{
  seg_t ret;
  for( int i = 0; i < SEGMENT_WORDS; i++ ) {
    ret.words[i] = hton_64(s.words[i]);
  }
  return ret;
}

// returns the number of bits decoded.
// decodes a value from the upper bits of segment.
static inline
int decode_gamma(uint64_t word, unsigned int* restrict out)
{
  int k;
  int value;

  // first, decode the unary code. Zeros followed by a 1.
  // where is the first 1 in the sequence?
  k = leadz64(word);
  k = 2*k + 1;
  value = word >> (64 - k);
  
  *out = value;

  return k;
}

// returns the number of bits encoded.
// saves the encoded value in the lower bits of *out.
static inline
int encode_gamma(uint64_t* restrict out, unsigned int run)
{
  unsigned char log;
  unsigned char need;
  uint64_t word;

  // save off run.
  log = log2i(run);
  need = 1 + 2 * log;

  if( EXTRA_CHECKS ) assert( need < 64 );

  word = run;
  *out = word;

  return need;

}


struct segs_reader {
  uint64_t cur_word; // the currently active buffer
  seg_t seg;
  int bit_idx; // where in seg does cur_word start?
};

static inline
void init_segs_reader(struct segs_reader* r, seg_t seg)
{
  r->seg = seg;
  r->cur_word = r->seg.words[0];
  r->bit_idx = 0;
}

static inline
void advance_segs_reader(struct segs_reader* r, int num_bits)
{
  uint64_t part;
  int start_idx, end_idx;
  int start_word, end_word;
  int start_bits, end_bits;

  if( EXTRA_CHECKS ) assert( num_bits < 64 );

  // We need to set cur_word to the 64-bits from start_idx to end_idx.
  start_idx = r->bit_idx + num_bits;
  end_idx = start_idx + 64;

  // now read the 64 bits from [start_idx, end_idx)
  // and place them in cur_word.

  start_word = start_idx / 64;
  start_bits = 64 - (start_idx % 64);
  end_word = end_idx / 64;
  end_bits = end_idx % 64;

  part = 0;
  if( start_word < SEGMENT_WORDS && start_bits > 0 ) {
    part |= r->seg.words[start_word] << (64 - start_bits);
  }

  if( end_word < SEGMENT_WORDS && end_bits > 0 ) {
    part |= r->seg.words[end_word] >> (64 - end_bits);
  }

  r->cur_word = part;

  r->bit_idx = start_idx;

  //printf("cur_word is %llx\n", r->cur_word);
}

static inline
int segs_reader_read_bit(struct segs_reader* r)
{
  int bit = r->cur_word >> 63;
  advance_segs_reader(r, 1);
  return bit;
}

static inline
int segs_reader_has_gamma(struct segs_reader* r)
{
  return (r->cur_word > 0);
}

static inline
int segs_reader_read_gamma(struct segs_reader* r)
{
  unsigned int ret;
  int num_bits;
  num_bits = decode_gamma(r->cur_word, &ret);
  advance_segs_reader(r, num_bits);
  return ret;
}

struct segs_writer {
  uint64_t cur_word; // the number we'll add to seg once it's full.
  int cur_word_left; // how many bits left in cur_seg?
  seg_t seg;
  int seg_words_used; // which segment will cur_seg store to?
  int num_appends;
};

static inline
int seg_used(struct segs_writer* s)
{
  return 64*s->seg_words_used + 64 - s->cur_word_left;
}

static inline
int seg_num_appends(struct segs_writer* s)
{
  return s->num_appends;
}

static inline
int has_room(struct segs_writer* s, int need)
{
  // How many bits have we used up?
  int bits_used = seg_used(s);
  int bits_available = SEGMENT_BITS - bits_used;
  if( need <= bits_available ) return 1;
  else return 0;
}

static inline
void append_segment(struct segs_writer* s, uint64_t append, int append_bits)
{
  //printf("Before -- %llx %i %llx %llx\n", s->cur_word, s->cur_word_left, s->seg.words[0], s->seg.words[1]);
  s->num_appends++;
  if( append_bits <= s->cur_word_left ) {
    s->cur_word <<= append_bits;
    s->cur_word |= append;
    s->cur_word_left -= append_bits;
  } else {
    // Do we have another segment word we can use?
    assert( s->seg_words_used < SEGMENT_WORDS );
    // OK, we have another segment word we can use.
    // Put the top *cur_word_left bits of append_bits into the current segment.
    s->cur_word <<= s->cur_word_left;
    s->cur_word |= append >> (append_bits - s->cur_word_left);
    // now store it to the segment proper.
    s->seg.words[s->seg_words_used] = s->cur_word;
    // Move on to the next segment word.
    s->seg_words_used++;
    // Put the bottom cur_word_left bits of append into cur_word.
    s->cur_word = append; // the other bits will fall off later..
    s->cur_word_left = 64 - (append_bits - s->cur_word_left);
  }
  //printf("After -- %llx %i %llx %llx\n", s->cur_word, s->cur_word_left, s->seg.words[0], s->seg.words[1]);
}

static inline
void start_segment(struct segs_writer* s)
{
  s->cur_word = 0;
  s->cur_word_left = 64;
  for( int i = 0; i < SEGMENT_WORDS; i++ ) {
    s->seg.words[i] = 0;
  }
  s->seg_words_used = 0;
  s->num_appends = 0;
}

static inline
void start_segment_bseq(struct segs_writer* s, int rle, int firstbit)
{
  start_segment(s);

  // put a bit for RLE enable/disable bit, which is set in save_segment.
  append_segment(s, rle, 1);

  if( rle ) {
    // put a bit for the firstbit if we're doing rle.
    append_segment(s, firstbit, 1);
  }

  s->num_appends = 0; // don't count these init bits as appends.
}


static inline
void finish_segment(struct segs_writer* s)
{
  assert( s->seg_words_used < SEGMENT_WORDS );
  assert( s->cur_word_left >= 0 );

  s->cur_word <<= s->cur_word_left; // pad with zeros.
  s->seg.words[s->seg_words_used] = s->cur_word;
  for( int i = s->seg_words_used + 1; i < SEGMENT_WORDS; i++ ) {
    s->seg.words[i] = 0;
  }
  s->seg_words_used = SEGMENT_WORDS;
  s->cur_word = 0;
  s->cur_word_left = 0;
}



typedef struct bseq_query {
  int occs[2]; // occurences of zeros and ones, includes bit.
               // select will set occs[!bit].
               // rank will set both.
  unsigned char bit; // 0 or 1. The bit at index. Set by rank,
                     // used by select.
  int index; // used by rank. set by select.
} bseq_query_t;


void bseq_rank(const unsigned char* zdata, bseq_query_t* q);
void bseq_select(const unsigned char* zdata, bseq_query_t* q);
error_t bseq_construct(int* zlen, unsigned char** zdata, int bitlen, unsigned char* data, bseq_stats_t* stats);
error_t bseq_construct_forcetype(int* zlen, unsigned char** zdata, int bitlen, unsigned char* data, bseq_stats_t* stats, int type);

enum {
  MUST_BE_ZERO_OFFSET,
  NUM_GROUPS_OFFSET,
  TOTAL_SEGMENT_WORDS_OFFSET,
  D_OFFSET,
  NUM_SEQ_OFFS
};


static inline
int bseq_A0_offset(void)
{
  return sizeof(int)*NUM_SEQ_OFFS;
}

static inline
int bseq_A_size(const unsigned char* zdata)
{
  const int* offsets = (int*) zdata;
  return ntoh_32(offsets[NUM_GROUPS_OFFSET]); // length in ints of A0/A1/AP
}

static inline
int bseq_segment_words(const unsigned char* zdata)
{
  const int* offsets = (int*) zdata;
  return ntoh_32(offsets[TOTAL_SEGMENT_WORDS_OFFSET]);
}

static inline
int bseq_S_offset(const unsigned char* zdata)
{
  return bseq_A0_offset() +
         3*sizeof(int)*bseq_A_size(zdata);
}

static inline
int bseq_D_offset(const unsigned char* zdata)
{
  const int* offsets = (int*) zdata;
  int offset;
  offset = ntoh_32(offsets[D_OFFSET]);
  return offset;
}


static inline
const seg_t* bseq_D_data(const unsigned char* zdata)
{
  const seg_t* ret;
  int offset;
  offset = bseq_D_offset(zdata);
  ret = (seg_t*) & zdata[offset];
  return ret;
}

static inline
const unsigned char* bseq_S_data(const unsigned char* zdata)
{
  const unsigned char* ret;
  int offset;
  offset = bseq_S_offset(zdata);
  ret = & zdata[offset];
  return ret;
}

static inline
const int* bseq_A0_data(const unsigned char* zdata)
{
  const int* ret;
  int offset;
  offset = bseq_A0_offset();
  ret = (int*) & zdata[offset];
  return ret;
}

static inline
int bseq_A0_get(const unsigned char* zdata, int group)
{
  const int* A0 = bseq_A0_data(zdata);
  return ntoh_32(A0[group]);
}
/*
static inline
void bseq_A0_set(const unsigned char* zdata, int group, int value)
{
  int* A0 = bseq_A0_data(zdata);
  A0[group] = hton_32(value);
}*/

static inline
const int* bseq_A1_data(const unsigned char* zdata)
{
  const int* ret;
  int offset;
  offset = bseq_A0_offset();
  offset += sizeof(int) * bseq_A_size(zdata);
  ret = (int*) & zdata[offset];
  return ret;
}

static inline
int bseq_A1_get(const unsigned char* zdata, int group)
{
  const int* A1 = bseq_A1_data(zdata);
  return ntoh_32(A1[group]);
}
/*
static inline
void bseq_A1_set(const unsigned char* zdata, int group, int value)
{
  int* A1 = bseq_A1_data(zdata);
  A1[group] = hton_32(value);
}*/

static inline
const int* bseq_AP_data(const unsigned char* zdata)
{
  const int* ret;
  int offset, a_size;
  a_size = sizeof(int) * bseq_A_size(zdata); // length of A0
  offset = bseq_A0_offset();
  offset += a_size; // A0
  offset += a_size; // A1
  ret = (int*) & zdata[offset];
  return ret;
}

static inline
int bseq_AP_get(const unsigned char* zdata, int group)
{
  const int* AP = bseq_AP_data(zdata);
  return ntoh_32(AP[group]);
}
/*
static inline
void bseq_AP_set(const unsigned char* zdata, int group, int value)
{
  int* AP = bseq_AP_data(zdata);
  AP[group] = hton_32(value);
}*/

// returns the encoded length.
static inline
int encode_varbyte(unsigned char* zdata, unsigned int value)
{
  unsigned char more;
  unsigned char word;
  int i;

  i = 0;
  do {
    word = value & 0x7f; // take the bottom 7-bits.
    value >>= 7; // shift right 7 bits.
    more = (value > 0);
    if( ! more ) word |= 0x80; // last one gets top bit set.
    zdata[i++] = word;
  } while(more);

  return i;
}

// returns the decoded length.
static inline
int decode_varbyte(const unsigned char* zdata, unsigned int* restrict out)
{
  int i;
  unsigned char word;
  unsigned int done;
  unsigned int value = 0;
 
  i = 0; 
  // read one byte at a time until we're done reading
  do {
    word = zdata[i];
    done = word & 0x80; // the top bit is set if this is the last byte.
    word &= 0x7f;
    // OR in the contents of the byte.
    value |= word << (7 * i);
    i++;
    assert( i <= 5); // there can be only 5
  } while( ! done );

  *out = value;
  return i;
}

static inline
seg_t bseq_segment(const unsigned char* zdata, int index)
{
  const seg_t* seg;
  seg_t ret;
  int total_words;
  int start, end, read_words;
  int i;

  total_words = bseq_segment_words(zdata);
  seg = bseq_D_data(zdata);

  start = SEGMENT_WORDS * index;
  end = SEGMENT_WORDS * (index+1);
  if( end > total_words ) {
    end = total_words;
  }
  read_words = end - start;

  for( i = 0; i < read_words; i++ ) {
    ret.words[i] = seg[index].words[i];
  }
  for( ; i < SEGMENT_WORDS; i++ ) {
    ret.words[i] = 0;
  }

  ret = ntoh_seg(ret);

  //printf("reading segment %i %lx\n", index, ret);
  return ret;
}

// return the S sums section for the group.
static inline
const unsigned char* bseq_S_section(const unsigned char* zdata, int group)
{
  const int* ap = bseq_AP_data(zdata);
  const unsigned char* sums = bseq_S_data(zdata);

  return sums + ntoh_32(ap[group]);
}
// For each character in the alphabet.. 

// RLE encoded b-sequence:

// RLE and gamma encoded into "double-words" which
// are 64-bits long. These are called "segments".
// This is a 0-1 sequence.
// For each segment, store the sum of runs of 0s and the
// sum of runs of 1s in a separate array.
// For each group, made up of 16 segments (say), store the
// start, in RLE-terms, of each group, 0 and 1.


/*
   Assume each wavelet-tree will encode at a maximum 2^32 bits
   (ie 2^29 bytes or < 500 MB)
   Worst case is 0-1-0-1-0-1. 2 bits for each gamma value.
   that's 2-bits for each bit. 2n bits.
   That's 2n/64 S entries.
   If 16 S in each group, there are (2n/64)/16 groups,
   and the A arrays use 12 bits per group - that's 
   12*(2n/64)/16 -- which is 0.02n. So in the end, we use 2.02n bits.

   Best case is 0-0-0-0. n bits of 0s.
   Then we get 1 entry for RLE-encoding and it's O(1).
   and we get arbitrarily good compression. But this compression will need
    at least 64+log(n)+12 bits. So there's an overhead of 76 bits
    for each entry; there will be alphabet_size entries = 19456 = 
    2 K overhead per wavelet tree -> we'll want them each to be more
    like 200 K.

   D segment - data - RLE and then gamma encoded
     -- D will use at least log(sum(run_lengths)) bits
   S occurence information for 0 and 1 within each segment (variable-length bytes)
     -- uses something like log(sum(run_lengths)) bits also.
     -- but it'll hopefully be smaller... 
     -- <number 0s> <number 1s>; <number 0s> <number 1s> ...
   A0 occurence information for 0s before each group (4-byte int)
   A1 occurence information for 1s before each group (4-byte int)
   AP offset of each group's start in S (4-byte int)
*/

/* A wavelet tree contains:
   alphabet_size offsets of nodes in the buffer in heap ordering, storing
                 only internal nodes. Heap ordering means 
                       1
                     2   3
                  4 
         ie            1
                    10    11
                100    
   bseq_ encoded sequences at each offset
*/

typedef struct {
  int node;
  int offset;
} node_offset_t;

// returns the index of the stored num or -1 if it was not
// found. Assumes arr is sorted.
static inline
int stored_num_for_node_num(int num_internal, node_offset_t* arr, unsigned int target)
{
  int a,b, middle;

#define VALUE(xxx) ( ntoh_32(arr[xxx].node) )

  a = 0; 
  b = num_internal - 1;
  if( target < VALUE(a) ) return -1;
  else if ( target == VALUE(a) ) return a;
  else if( target > VALUE(b) ) return -1;
  else if( target == VALUE(b) ) return b;

  // invariant is that VALUE(a) < target < VALUE(b).
  while( b - a > 1 ) {
    middle = (a + b) / 2;
    if( target < VALUE(middle) ) b = middle;
    else if ( target == VALUE(middle) ) return middle;
    else a = middle; // VALUE(middle) < target.
  }

#undef VALUE

  return -1;
}

// node_numbers count up from 1.
static inline
unsigned char* wtree_bseq(unsigned char* wtree, int node_number)
{
  // the first part of the wavelet tree is a directory.
  node_offset_t* dir = (node_offset_t*) (wtree+sizeof(int));
  int num_internal;
  int stored_num;
  int off;
 
  num_internal = ntoh_32(* ((int*) wtree));
  stored_num = stored_num_for_node_num(num_internal, dir, node_number);
  if( stored_num < 0 ) return NULL;

  off = ntoh_32(dir[stored_num].offset);
  return &wtree[off];
}


#endif
