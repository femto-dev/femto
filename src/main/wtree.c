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

  femto/src/main/wtree.c
*/

#include "wtree.h"
#include "wtree_funcs.h"
#include "buffer_funcs.h"
#include "util.h"

#include <assert.h>

typedef unsigned char uchar;

// how many segments per group?
// Represents a tradeoff between compression and speed.
/* 0.9 versions used 10. We did some experiments with etext90.
 * Queries were 'a' and 'A' asking for offsets.
 *   4 -> 11136864 bytes; 'a' 5.9 s; 'A' 60.5 s
 *   8 -> 10139408 bytes; 'a' 5.7 s; 'A' 58.2 s
 *  10 ->  9939592 bytes; 'a' 5.6 s; 'A' 58.5 s
 *  16 ->  9646432 bytes; 'a' 5.7 s; 'A' 58.5 s
 *  20 ->  9544360 bytes; 'a' 5.7 s; 'A' 58.5 s
 *  31 ->  9408880 bytes; 'a' 5.82 s; 'A' 58.8 s
 *  32 ->  9401456 bytes; 'a' 5.84 s; 'A' 59.4 s
 *  49 ->  9317160 bytes; 'a' 6.1 s; 'A' 61.1 s
 *  63 ->  9282672 bytes; 'a' 6.3 s; 'A' 62.7 s
 *  64 ->  9281072 bytes; 'a' 6.3 s; 'A' 62.2 s
 * 128 ->  9223488 bytes; 'a' 7.0 s; 'A' 65.0 s
 */
#define GROUP_SIZE 31

int wtree_settings_number(void)
{
  return GROUP_SIZE + 0x1000 * SEGMENT_WORDS;
}

struct construct_info {
  struct segs_writer rle_segs_writer; // run-length encoded.
  struct segs_writer unc_segs_writer; // uncompressed.
  int seg_rle; // are we doing RLE on this segment?
               // when 1, we must do RLE
               // when -1, we must do uncompressed
               // when 0, we havn't decided yet.
  int initial_seg_rle; // set seg_rle to this when setting up a new segment

  int segnum;
  buffer_t segments_buf;
  buffer_t segment_sums;
  buffer_t a0;
  buffer_t a1;
  buffer_t ap;
  // construct a binary sequence RLE-and then gamma-encoded
  int segment_occs_rle[2]; // number of occurences in this segment
  int segment_occs_unc[2]; // number of occurences in this segment
  int occs[2]; // number of occurences total
  int seg_in_group;
  int num_groups;
  int segment_ap;
  unsigned char lastbit; // run always on lastbit
  unsigned char bit;
  bseq_stats_t* stats;

  int last_segment_words_used;
};

static inline 
error_t save_group( struct construct_info* s)
{
  error_t err;
  int* arr;

  //printf("Saving group with A0 %i A1 %i AP %i\n", s->occs[0], s->occs[1], s->segment_ap);
  // store the occs for the group!
  err = buffer_extend(&s->a0, sizeof(int));
  if( err ) return err;
  arr = (int*) &s->a0.data[s->a0.len];
  s->a0.len += sizeof(int);
  *arr = hton_32(s->occs[0]);
  err = buffer_extend(&s->a1, sizeof(int));
  if( err ) return err;
  arr = (int*) &s->a1.data[s->a1.len];
  s->a1.len += sizeof(int);
  *arr = hton_32(s->occs[1]);
  err = buffer_extend(&s->ap, sizeof(int));
  if( err ) return err;
  arr = (int*) &s->ap.data[s->ap.len];
  s->ap.len += sizeof(int);
  *arr = hton_32(s->segment_ap);

  s->num_groups++;

  return ERR_NOERR;
}

static inline
struct segs_writer* get_active_writer( struct construct_info* s )
{
  if( s->seg_rle >= 0 ) {
    return &s->rle_segs_writer;
  } else {
    return &s->unc_segs_writer;
  }
}

static inline
error_t save_segment( struct construct_info* s )
{
  error_t err;
  seg_t *sb;
  seg_t src_seg;
  int bits_used;
  int words_used;
  int segment_occs[2];

  bits_used = seg_used(get_active_writer(s));
  words_used = CEILDIV(bits_used, 64);
  s->last_segment_words_used = words_used;

  // Finish the segment, adding zero padding
  if( s->seg_rle >= 0 ) {
    finish_segment(&s->rle_segs_writer);
    src_seg = s->rle_segs_writer.seg;
    segment_occs[0] = s->segment_occs_rle[0];
    segment_occs[1] = s->segment_occs_rle[1];
  } else {
    finish_segment(&s->unc_segs_writer);
    src_seg = s->unc_segs_writer.seg;
    segment_occs[0] = s->segment_occs_unc[0];
    segment_occs[1] = s->segment_occs_unc[1];
  }
 
  if( s->stats ) {
    s->stats->segment_bits += SEGMENT_BITS;
    s->stats->segment_bits_used += bits_used;
    s->stats->num_rle_segments++;
    s->stats->num_unc_segments++;
  }
 
  err = buffer_extend(&s->segments_buf, sizeof(seg_t));
  if( err ) return err;
  sb = (seg_t*) s->segments_buf.data;
  sb[s->segnum] = hton_seg(src_seg);
  //printf("saving segment %i %lx\n", s->segnum, s->seg);
  /*printf("segment %i: ", s->segnum);
  for( int k = 0; k < 64; k++ ) {
    printf("%i", (int) (s->seg >> (63 - k))&1);
  }
  printf("\n");
  */
  s->segnum++;
  s->segments_buf.len += sizeof(seg_t);

  // save the segment_sums entries.
  // make sure there's room for the 5 bytes needed to store a 32-bit
  // int in 7-byte chunks. for 0 occs and 1 occs.
  err = buffer_extend(&s->segment_sums, 2*5);
  if( err ) return err;
  s->segment_sums.len += encode_varbyte(
      &s->segment_sums.data[s->segment_sums.len], segment_occs[0]);
  s->segment_sums.len += encode_varbyte(
      &s->segment_sums.data[s->segment_sums.len], segment_occs[1]);

  // if this is the start of a group.
  if( s->seg_in_group == 0 ) {
    err = save_group(s);
    if( err ) return err;
  }
  s->seg_in_group++;
  if( s->seg_in_group >= GROUP_SIZE ) s->seg_in_group = 0;

  s->occs[0] += segment_occs[0];
  s->occs[1] += segment_occs[1];

  s->segment_occs_rle[0] = 0;
  s->segment_occs_unc[0] = 0;
  s->segment_occs_rle[1] = 0;
  s->segment_occs_unc[1] = 0;


  // bit is the new bit, will be saving run on lastbit
  // get the next segment ready - it'll start with a bit
  // indicating the first bit of the next segment.
  //printf("Starting new segment; bit is %i, lastbit is %i\n", s->bit, s->lastbit);
  
  s->seg_rle = s->initial_seg_rle; // havn't decided yet.
  start_segment_bseq(&s->rle_segs_writer, 1, s->lastbit);
  start_segment_bseq(&s->unc_segs_writer, 0, s->lastbit);
                        // at this point, we're saving a run that didn't fit
                        // and the run is on lastbit; so the new segment
                        // should start with lastbit.

  // ap is a pointer into the segments sums section.
  s->segment_ap = s->segment_sums.len;

  return ERR_NOERR;
} 

static inline
error_t save_run( struct construct_info* s, int bit, int run ) 
{
  error_t err;
  int encoded_bits;
  uint64_t encoded;
  int saved;
  int init_rle_has_room, init_unc_has_room;

  if( s->stats ) {
    if( run < TRACK_RUNS_DIST ) {
      s->stats->run_count[run]++;
    }
    if( run > s->stats->max_run ) s->stats->max_run = run;
    s->stats->num_runs++;
  }

  assert( run > 0 );

  //printf("Encoding run %i to %lx in %i bits segleft is %i\n", run, (long) segment, encoded_bits, s->segleft);
  

  // Encode to both the rle and the unc.
  encoded_bits = encode_gamma(&encoded, run);

  while ( 1 ) {
    init_rle_has_room = has_room(&s->rle_segs_writer, encoded_bits);
    init_unc_has_room = has_room(&s->unc_segs_writer, run);
    
    if( ! init_rle_has_room ) {
      if( s->seg_rle == 0 ) {
        // Now we decide -- did the segment store the bits efficiently?
        // if not, we'll use the segment stored without compression.
        int nbits_stored = s->segment_occs_rle[0] + s->segment_occs_rle[1];
        int encoded_size = SEGMENT_BITS - 1; // 1 bit is whether to use rle.
        if( nbits_stored < encoded_size ) {
          //printf("Will store uncompressed 1\n");
          s->seg_rle = -1; // store uncompressed.
        } else {
          //printf("Will store compressed 1\n");
          s->seg_rle = 1; // store rle compressed.
        }
      }
    }
    if( ! init_unc_has_room ) {
      if( s->seg_rle == 0 ) {
        //printf("Will store compressed 2\n");
        s->seg_rle = 1; // store rle compressed.
      }
    }
    // If neither of them has room, save and reset.
    if( ((! init_rle_has_room) && (! init_unc_has_room)) ||
        ( s->seg_rle == 1 && ! init_rle_has_room ) ||
        ( s->seg_rle == -1 && ! init_unc_has_room ) ) {
      // If we're on a segment that is not compressed,
      // cram a few more bits on there (reducing the run size).
      // We don't know how we'll store the next one.
      if( s->seg_rle == -1 && ! init_unc_has_room ) {
        while( run > 0 ) {
          int unc_has_room;

          unc_has_room = has_room(&s->unc_segs_writer, 1);

          if( ! unc_has_room ) break;

          append_segment(&s->unc_segs_writer, bit, 1);
          // run is on lastbit - increment segment_occs
          s->segment_occs_unc[bit] += 1;
          run--;
        }
        // We have to re-encode what's left in the run!
        if( run != 0 ) encoded_bits = encode_gamma(&encoded, run);
      }
      err = save_segment(s);
      if( err ) return err;

      if( run == 0 ) return ERR_NOERR; // nothing else to do!

      // loop again!
      
    } else {
      if( (! init_rle_has_room) || (! init_unc_has_room) ) {
        assert(s->seg_rle != 0);
      }
      break; // there's room somewhere.
    }
  }

  //printf("s->seg_rle=%i\n", s->seg_rle);

  assert( run > 0 );

  saved = 0;
  if( s->seg_rle >= 0 ) { // unknown or RLE only.
    int rle_has_room;

    rle_has_room = has_room(&s->rle_segs_writer, encoded_bits);
    // Save the segment if there's no room using the selected compression.
    if( ! rle_has_room ) {
      err = save_segment(s);
      if( err ) return err;
    }

    append_segment(&s->rle_segs_writer, encoded, encoded_bits);
    // run is on lastbit - increment segment_occs
    s->segment_occs_rle[bit] += run;

    saved = 1;
  }

  if( s->seg_rle <= 0 ) { // unknown or uncompressed only
    // We have to possibly save multiple segments for the run.
    for( int i = 0; i < run; i++ ) {
      int unc_has_room;

      unc_has_room = has_room(&s->unc_segs_writer, 1);
      // Save the segment if there's no room using the selected compression.
      if( ! unc_has_room ) {
        assert( s->initial_seg_rle == -1 ); // only saving multiple when forced
        err = save_segment(s);
        if( err ) return err;
      }

      append_segment(&s->unc_segs_writer, bit, 1);
      // run is on lastbit - increment segment_occs
      s->segment_occs_unc[bit] += 1;
    }
    saved = 1;
  }

  assert(saved);

  return ERR_NOERR;
}

// zlen is the length in bytes.
// len is the length in bits.
// caller must free *zdata.
error_t bseq_construct(int* zlen, uchar** zdata, int bitlen, uchar* data, bseq_stats_t* stats)
{
  return bseq_construct_forcetype(zlen, zdata, bitlen, data, stats, 0);
}

error_t bseq_construct_forcetype(int* zlen, uchar** zdata, int bitlen, uchar* data, bseq_stats_t* stats, int type)
{
  error_t err;
  struct construct_info s;
  int len;
  int run;

  memset(&s, 0, sizeof(struct construct_info));

  s.initial_seg_rle = type;
  s.stats = stats;
  s.segments_buf = build_buffer(0, NULL);
  s.segment_sums = build_buffer(0, NULL);
  s.a0 = build_buffer(0, NULL); // int 
  s.a1 = build_buffer(0, NULL); // int
  s.ap = build_buffer(0, NULL); // int
  // s.segment_occs, s.occs, s.seg_in_group set to 0 by memset.
  s.segment_ap = 0;

  // construct a binary sequence RLE-and then gamma-encoded
  // and with extra structures for fast lookup.
  
  len = CEILDIV(bitlen, 8); //ceildiv8(bitlen);
  s.segnum = 0;
  s.bit = s.lastbit = (data[0] & 0x80) >> 7;
  s.seg_rle = s.initial_seg_rle; // havn't decided yet.
  start_segment_bseq(&s.rle_segs_writer, 1, s.lastbit);
  start_segment_bseq(&s.unc_segs_writer, 0, s.lastbit);

  run = 0;
  // go through the data, RLE-encoding.
  for( int i = 0; i < len; i++ ) {
    unsigned char byte = data[i];
    for( int j = 0; j < 8 && 8*i + j < bitlen ; j++ ) {
      s.bit = (byte & 0x80) >> 7;
      byte <<= 1;
      //printf("Considering bit %i, run is %i, lastbit is %i\n", bit, run, lastbit);
      // now - is bit the same as lastbit?
      if( s.bit == s.lastbit ) run++;
      else {
        err = save_run(&s, s.lastbit, run); // bit is the new bit, run is on lastbit
        if( err ) goto error;
        run = 1; // we have to encode the bit we just read.
        s.lastbit = s.bit;
      }
    }
  }

  // run is on lastbit; if run!=0, bit == lastbit.
  s.bit = ! s.lastbit;

  if( run ) {
    // We have a run we need to encode...
    err = save_run(&s, s.lastbit, run); // run is on lastbit.
    if( err ) goto error;
    run = 0;
  }
  if( seg_num_appends(get_active_writer(&s)) > 0 ) {
    // We've appended some since last save... save segment.
    err = save_segment(&s);
    if( err ) goto error;
  }


  { // copy the data into a single buffer.
    int i;
    int mark;
    int segment_words;
    unsigned char* b = (unsigned char*)
                       malloc(NUM_SEQ_OFFS*sizeof(uint32_t) +
                              s.segments_buf.len + 
                              s.segment_sums.len + 
                              s.a0.len +
                              s.a1.len +
                              s.ap.len +
                              6*8 // room for aligning each chunk
                              );

    if( ! b ) {
      err = ERR_MEM;
      goto error;
    }

    // i is the start of the data section.
    i = NUM_SEQ_OFFS*sizeof(uint32_t);
    assert( ! (i & ALIGN_MASK) );

    ((int*) b)[MUST_BE_ZERO_OFFSET] = 0;

    // encode the number of groups
    ((int*) b)[NUM_GROUPS_OFFSET] = hton_32(s.num_groups);

    // encode the A0,A1, and AP arrays.
    // encode A0 section. This must be 4-byte aligned.
    mark = i;
    assert( bseq_A0_offset() == mark );
    assert( s.a0.len == sizeof(int) * bseq_A_size(b) );
    assert( (i & 3) == 0 );
    memcpy(&b[i], s.a0.data, s.a0.len);
    i += s.a0.len;
    if( stats ) stats->group_zeros += i - mark;

    // encode A1 section 4-byte aligned.
    mark = i;
    assert( s.a1.len == sizeof(int) * bseq_A_size(b) );
    assert( (i & 3) == 0 );
    memcpy(&b[i], s.a1.data, s.a1.len);
    i += s.a1.len;
    if( stats ) stats->group_ones += i - mark;

    // encode A_Ptr section. 4-byte aligned.
    mark = i;
    assert( s.ap.len == sizeof(int) * bseq_A_size(b) );
    assert( (i & 3) == 0 );
    memcpy(&b[i], s.ap.data, s.ap.len);
    i += s.ap.len;
    if( stats ) stats->group_ptrs += i - mark;

    // i is the start of the S section..
    // encode S section
    mark = i;
    assert( bseq_S_offset(b) == mark );
    memcpy(&b[i], s.segment_sums.data, s.segment_sums.len);
    i += s.segment_sums.len;
    // Pad to align
    while( i & ALIGN_MASK ) {
      b[i++] = 0;
    }
    if( stats ) stats->segment_sums += i - mark;
 
    // encode the D section. This must be 64-bit aligned.
    segment_words = SEGMENT_WORDS * (s.segnum - 1) + s.last_segment_words_used;
    assert( s.segments_buf.len == 8 * SEGMENT_WORDS * s.segnum );

    mark = i;
    ((int*) b)[TOTAL_SEGMENT_WORDS_OFFSET] = hton_32(segment_words);
    ((int*) b)[D_OFFSET] = hton_32(i);
    assert( bseq_segment_words(b) == segment_words );
    assert( bseq_D_offset(b) == mark );
    memcpy(&b[i], s.segments_buf.data, 8 * segment_words );
    i += 8 * segment_words;
    if( stats ) stats->segments += i - mark;

    *zdata = b;
    *zlen = i;
  }

  err = ERR_NOERR;

error:
  free(s.segments_buf.data);
  free(s.segment_sums.data);
  free(s.a0.data);
  free(s.a1.data);
  free(s.ap.data);
  return err;
  
}

// returns the index of the group that contains index.
// The index g of the right group will have the property that
// PREFIX_SUM(g) < target < PREFIX_SUM(g+1)
static inline
int bsearch_A0A1(const unsigned char* zdata, int target, int useA0, int useA1)
{
  int a,b, middle;

#define VALUE(xxx) ( ((useA0) ? (bseq_A0_get(zdata,xxx)) : (0)) + \
                     ((useA1) ? (bseq_A1_get(zdata,xxx)) : (0)) )

  a = 0; 
  b = bseq_A_size(zdata) - 1;
  if( target < VALUE(a) ) return -1;
  else if( target >= VALUE(b) ) return b;

  // invariant is that VALUE(a) <= target < VALUE(b).
  while( b - a > 1 ) {
    middle = (a + b) / 2;
    if( target < VALUE(middle) ) b = middle;
    else a = middle; // VALUE(middle) <= target.
  }
#undef VALUE
  return a;
}


// returns the number of 1s or 0s at or before the
// passed index in the binary sequence. Also returns
// the value at index. index is counting from 1.
void bseq_rank(const unsigned char* zdata, bseq_query_t* q)
{
  int group;
  int segment;
  unsigned int index;

  assert(q->index > 0); // counting from 1.

  index = q->index - 1; // now index counts from 0.

  //printf("num groups is %i\n", bseq_A_size(zdata));

  // step 1: binary search to find the group we're looking for.
  // find a group g with A0[g] + A1[g] <= index < A0[g+1] + A1[g+1] 
  group = bsearch_A0A1(zdata, index, 1, 1);
  // remove the contribution from previous groups from the index value.
  q->occs[0] = bseq_A0_get(zdata, group);
  q->occs[1] = bseq_A1_get(zdata, group);

  // step 2: go through the segment sums in the group.
  {
    const unsigned char* sums;
    int i;
    unsigned int s0, s1;

    sums = bseq_S_section(zdata, group);
    segment = 0;
    i = 0;
    for( int j = 0; 1; j++ ) {
      assert(j < GROUP_SIZE);
      // decode an S0 and an S1.
      i += decode_varbyte(&sums[i], &s0);
      i += decode_varbyte(&sums[i], &s1);
      if( q->occs[0] + s0 + q->occs[1] + s1 <= index ) {
        // add in the contribution.
        q->occs[0] += s0;
        q->occs[1] += s1;
        segment++;
      } else { // index < sums
        break;
      }
    }

    // now which segment were we on?
    segment += GROUP_SIZE * group;

  }
  {
    // now go to that segment and decode until we get to the 
    // appropriate part.
    struct segs_reader sr;
    unsigned int value;
    unsigned char bit;
    unsigned char is_rle;

    init_segs_reader(&sr, bseq_segment(zdata, segment));

    // Read the top bit.
    is_rle = segs_reader_read_bit(&sr);

    if( is_rle ) {
      // we're at offset q->occs[0] + q->occs[1], and we want
      // to get to offset index.
      // decode the segment.
      bit = segs_reader_read_bit(&sr);
      //printf("Decoding segment %i (group %i); occs (%i,%i)\n", segment, group, q->occs[0], q->occs[1]);
      while( 1 ) {
        if( EXTRA_CHECKS ) assert( segs_reader_has_gamma(&sr) );
        value = segs_reader_read_gamma(&sr);
        //printf("Read bit %i run %i\n", bit, value);
        if( q->occs[0] + q->occs[1] + value <= index ) {
          q->occs[bit] += value;
          bit = ! bit;
        } else { // index < sums.
          q->occs[bit] += (1 + index - (q->occs[0] + q->occs[1]));
          break;
        }
      }
    } else {
      // adding 1 to account for top bit is_rle
      int num_bits_in = 1 + index - q->occs[0] - q->occs[1];
      int word_idx = num_bits_in / 64;
      int bit_idx = num_bits_in % 64;
      int num_bits;
      uint64_t tmp;

      if( EXTRA_CHECKS ) assert(num_bits_in < SEGMENT_BITS);

      // Count the zeros and ones in previous words.
      for( int i = 0; i < word_idx; i++ ) {
        uint64_t word;
        int ones, zeros;

        word = sr.seg.words[i];
        ones = popcnt64(word);
        zeros = 64 - ones;
        if( i == 0 ) zeros--; // don't count the zero for top bit is_rle

        q->occs[0] += zeros;
        q->occs[1] += ones;
      }

      // Now count the zeros and ones in our word, and get our bit.
      tmp = sr.seg.words[word_idx];
      if( word_idx == 0 ) {
        tmp <<= 1; // clear of the is_rle bit.
        bit_idx--; // we're not accounting for that bit anymore..
      }

      // Now, the top bit_idx+1 bits are of interest here.
      // Right shift them. Our bit will be in the 1s place.
      num_bits = bit_idx + 1;
      tmp >>= 64 - num_bits;

      // Compute the number of 1s in our pattern here.
      {
        int ones = popcnt64(tmp);
        int zeros = num_bits - ones;

        q->occs[0] += zeros;
        q->occs[1] += ones;

        bit = tmp & 1;
      }
    }

    q->bit = bit;
  }
}

// returns the index of the rank'th occurence of bit; index
// counts from 1, rank counts from 1.
// q->bit is the bit in question; q->occs[bit] is the rank in question.
// q->occs[!bit] will be set and the resulting index will be
// q->occs[0] + q->occs[1] (counting from 1).
void bseq_select(const unsigned char* zdata, bseq_query_t* q)
{
  // do a logarithmic binary search on A[bit]
  int group;
  int segment;
  unsigned int rank;
  unsigned char bit;
 
  bit = q->bit;
  rank = q->occs[bit] - 1;  // now rank counts from 0.

  // step 1: binary search to find the group we're looking for.
  // find a group g with Ax[g] <= rank < Ax[g+1]
  group = bsearch_A0A1(zdata, rank, bit == 0, bit == 1);
  // compute the index from the previous groups
  q->occs[0] = bseq_A0_get(zdata, group);
  q->occs[1] = bseq_A1_get(zdata, group);

  // step 2: go through the segment sums in the block.
  {
    int i;
    unsigned int s[2];
    const unsigned char* sums;

    sums = bseq_S_section(zdata, group);

    segment = 0;
    i = 0;
    while( 1 ) {
      // decode an S0 and an S1.
      i += decode_varbyte(&sums[i], &s[0]);
      i += decode_varbyte(&sums[i], &s[1]);
      if( q->occs[bit] + s[bit] <= rank ) {
        // add in the contribution.
        q->occs[0] += s[0];
        q->occs[1] += s[1];
        segment++;
      } else { // index < sums
        break;
      }
    }

    // now which segment were we on?
    segment += GROUP_SIZE * group;

  }
  {
    // now go to that segment and decode until we get to the 
    // appropriate part.
    struct segs_reader sr;
    unsigned int value;
    unsigned char is_rle;
    unsigned char read_bit;

    init_segs_reader(&sr, bseq_segment(zdata, segment));

    // Read the top bit.
    is_rle = segs_reader_read_bit(&sr);

    if( is_rle ) {
      // Read the top bit to find out how the run begins.
      read_bit = segs_reader_read_bit(&sr);
      while( 1 ) {
        if( EXTRA_CHECKS ) assert( segs_reader_has_gamma(&sr) );
        value = segs_reader_read_gamma(&sr);
        if( bit != read_bit || q->occs[bit] + value <= rank ) {
          q->occs[read_bit] += value;
          read_bit = ! read_bit;
        } else { // rank < q->occs[bit] + value.
          q->occs[bit] += 1 + rank - q->occs[bit]; // add in 1 for this bit.
          break;
        }
      }
    } else {
      // Find the appropriate segment.
      int word_idx;
      uint64_t tmp;

      for( word_idx = 0; word_idx < SEGMENT_WORDS; word_idx++ ) {
        uint64_t word;
        int word_cnt[2];

        word = sr.seg.words[word_idx];
        word_cnt[1] = popcnt64(word);
        word_cnt[0] = 64 - word_cnt[1];
        if( word_idx == 0 ) word_cnt[0]--; // don't count the zero for not rle.

        if( q->occs[bit] + word_cnt[bit] <= rank ) {
          q->occs[0] += word_cnt[0];
          q->occs[1] += word_cnt[1];
        } else {
          break;
        }
      }
      
      if( EXTRA_CHECKS ) assert( word_idx < SEGMENT_WORDS );

      // Now consider word word_idx. The ? : below gets rid of a compiler err;
      // word_idx should always be < SEGMENT_WORDS.
      tmp = (word_idx < SEGMENT_WORDS) ? (sr.seg.words[word_idx]) : 0;
      if( word_idx == 0 ) tmp <<= 1; // clear of the is_rle bit.

      for( int i = 0; i < 64; i++ ) {
        // Think about the top bit.
        unsigned char read_bit = tmp >> 63;
        unsigned char value = 1;
        if( bit != read_bit || q->occs[bit] + value <= rank ) {
          q->occs[read_bit] += value;
        } else { // rank < q->occs[bit] + value.
          q->occs[bit] += 1 + rank - q->occs[bit]; // add in 1 for this bit.
          break;
        }

        tmp <<= 1;
      }
    }
  }

  q->index = q->occs[0] + q->occs[1];
}

int offsets_cmp(const void* aIn, const void* bIn)
{
  node_offset_t* a = (node_offset_t*) aIn;
  node_offset_t* b = (node_offset_t*) bIn;

  return ntoh_32(a->node) - ntoh_32(b->node);
}

// leaf is alpha_size giving the leaf node number 
// (nodes are numbered from 1)
// of each alphabet letter. data is the prepared data.
// caller is responsible for freeing the malloced *zdata.
// num_nodes is the number of nodes in the entire coding
// table; for characters in data, leaf must have a value in
// 1..max_leaf. Unmapped characters have value 0.
// nInUse: data[i] < nInUse
error_t wtree_construct(int* zlen, uchar** zdata,
                        int nInUse, int* leaf_map,
                        int len, int* data, wtree_stats_t* stats)
{
  error_t err;
  buffer_t* ptrs = NULL;
  buffer_t output = build_buffer(0, NULL);
  int num_internal = 0;
  int max_offsets;
  int mark; 
  
  // leave room for num_internal.
  err = buffer_extend(&output, sizeof(int));
  if( err ) goto error;

  output.len += sizeof(int);
  mark = output.len;

  num_internal = nInUse - 1;
  max_offsets = 2 * num_internal;
  err = buffer_extend(&output, max_offsets * sizeof(node_offset_t) );
  if( err ) goto error;

  output.len += max_offsets * sizeof(node_offset_t);
  memset(output.data, 0, max_offsets * sizeof(node_offset_t));
#define offsets ((node_offset_t*)(output.data+sizeof(int)))

  // Create the offsets table - find the internal nodes in the tree..
  {
    int i;
    int num_offsets = 0;
    int leaf;
    int done = 0;

    // !!! There's got to be a better way!
    i = 0;
    for( int k = 0; k < nInUse; k++ ) {
      // skip any offsets with leaf 0.
      leaf = leaf_map[k];
      if( leaf > 0 ) offsets[i++].node = hton_32(leaf >> 1);
    }

    while( !done ) {
      done = 1;
      // make sure that we have all nonzero right-shifts
      // of things in the array.
      num_offsets = sort_dedup( offsets, i,
                                sizeof(node_offset_t), offsets_cmp );
      i = num_offsets;
      for( int k = 0; k < num_offsets && i < max_offsets; k++ ) {
        leaf = ntoh_32(offsets[k].node) >> 1;
        // check - do we have this one?
        if( leaf > 0 &&
            -1 == stored_num_for_node_num(num_offsets, offsets, leaf)) {
          // no -- add it.
          offsets[i++].node = hton_32(leaf);
          done = 0; // not done yet.
        }
      }

    }

    num_internal = num_offsets;

  }
  // update num_internal.
  *((int*) output.data) = hton_32(num_internal);
  // update output.len.
  output.len = mark + num_internal * sizeof(node_offset_t);
  buffer_align( &output, ALIGN_MASK );
  //if( stats ) stats->offsets += output.len;
  if( stats ) stats->offs += output.len;


  ptrs = (buffer_t*) malloc(num_internal*sizeof(buffer_t));
  if( ! ptrs ) {
    err = ERR_MEM;
    goto error;
  }
  memset(ptrs, 0, num_internal*sizeof(buffer_t));
  for( int k = 0; k < num_internal; k++ ) {
    bsInitWrite(&ptrs[k]);
  }


  // now go through the input data, appending to the appropriate buffers
  for( int i = 0; i < len; i++ ) {
    int target;
    unsigned int bit;
    int stored_num;

    if( data[i] >= nInUse ) {
      err = ERR_INVALID;
      goto error;
    }
    target = leaf_map[data[i]];
    if( target <= 0 ) {
      err = ERR_INVALID;
      goto error;
    }

    // go up from the bottom of the tree appending target.
    while( target > 1 ) { // figure out our bit and encode in parent.
      bit = target & 1;
      target >>= 1;
      assert( target > 0 );

      stored_num = stored_num_for_node_num(num_internal, offsets, target);
      if( stored_num < 0 ) {
        err = ERR_INVALID;
        goto error;
      }
      err = buffer_extend_W24(&ptrs[stored_num], 1, bit);
      if( err ) goto error;
    }
  }


  // now go through our buffers compressing with the gamma encoder.
  for( int k = 0; k < num_internal; k++ ) {
    int zlen;
    unsigned char* zdata=NULL;
    int bitlen = ptrs[k].bsLive + 8*ptrs[k].len;

    bsFinishWrite(&ptrs[k]);

    if( ptrs[k].len > 0 ) {
      // skip empty areas..
      err = bseq_construct(&zlen, &zdata, bitlen, ptrs[k].data,
                           (stats)?(&stats->bseq_stats):(NULL));
      if( err ) goto error;
    }

    if( zdata ) {
      // update our offset
      offsets[k].offset = hton_32(output.len);

      // save the compressed data.
      err = buffer_extend(&output, zlen);
      if( err ) return err;

      memcpy(&output.data[output.len], zdata, zlen);
      output.len += zlen;
      buffer_align( &output, ALIGN_MASK );
      if( stats ) stats->bseqs += zlen;

      // free the compressed data
      free(zdata);
    } else {
      // 0 indicates no data!
      offsets[k].offset = hton_32(0);
    }
  }
#undef offsets

  err = ERR_NOERR;

error:
  if( ptrs ) {
    for( int k = 0; k < num_internal; k++ ) {
      free(ptrs[k].data);
    }
    free(ptrs);
  }
  if( err ) free(output.data);
  else {
    *zlen = output.len;
    *zdata = output.data;
    // DONT free output.data
  }
  return err;
}


error_t wtree_occs(unsigned char* zdata,
                   wtree_query_t* q)
{
  int node;
  int index;
  unsigned char* bseq;
  bseq_query_t bq;
  int leaf_len; // does not count leading 1.

  if( q->index == 0 ) return ERR_PARAM;
  if( q->leaf <= 0 ) return ERR_PARAM;

  leaf_len = 31 - leadz32(q->leaf);

  // first, find the root node...
  node = 1;
  index = q->index;
  // go down the tree to find the leaf node number.
  for( int i = 1; 1; i++ ) {
    // grab the sequence for that node.
    bseq = wtree_bseq(zdata, node);
    if( bseq == NULL ) break; // bseq is null for leaf nodes.
    // extract the appropriate index.
    bq.index = index;
    bseq_rank(bseq, &bq);
    // the next node is the appropriate one from the leaf character..
    node = (q->leaf >> (leaf_len - i));
    // what happens to index? Remove the other bits from the count.
    index -= bq.occs[!(node & 1)];
    if( index == 0 ) break;
  }

  q->count = index;
  return ERR_NOERR;
}

error_t wtree_rank(unsigned char* zdata,
                   wtree_query_t* q)
{
  int node;
  int index;
  unsigned char* bseq;
  bseq_query_t bq;

  if( q->index == 0 ) return ERR_PARAM;

  // first, find the root node...
  node = 1;
  index = q->index;
  // go down the tree to find the leaf node number.
  for( int i = 1; 1; i++ ) {
    // grab the sequence for that node.
    bseq = wtree_bseq(zdata, node);
    if( bseq == NULL ) break; // bseq is null for leaf nodes.
    // extract the appropriate index.
    bq.index = index;
    bseq_rank(bseq, &bq);
    // the next node is the current one with rr.bit added
    node = (node << 1 ) | bq.bit;
    // what happens to index? Remove the other bits from the count.
    index -= bq.occs[!bq.bit];
  }

  q->leaf = node;
  q->count = index;

  return ERR_NOERR;
}

error_t wtree_select(unsigned char* zdata, wtree_query_t* q)
{
  int node;
  int rank;
  unsigned char* bseq;
  bseq_query_t bq;

  if( q->count == 0 || q->leaf == 0 ) return ERR_PARAM;

  // start from the leaf node, going upwards.
  rank = q->count;
  node = q->leaf;
  // move to the first internal node from the leaf.

  while( node > 1 ) {
    // move to the node above node.
    bq.bit = node & 1;
    bq.occs[bq.bit] = rank;
    node >>= 1;
    // grab the node's sequence.
    bseq = wtree_bseq(zdata, node);
    bseq_select(bseq, &bq);
    rank += bq.occs[!bq.bit];
  }

  q->index = rank;

  return ERR_NOERR;
}

