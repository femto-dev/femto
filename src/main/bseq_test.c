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

  femto/src/main/bseq_test.c
*/
#include <stdio.h>
#include <stdlib.h>

#include "index_types.h"
#include "buffer.h"
#include "bwt_reader.h"
#include "timing.h"
#include "wtree_funcs.h"

#define AREAS_PER_GROUP 1025
#define SEGS_PER_AREA 8
// A single area can only have this 
// many zeros or ones; if it needs more, 
// the run is split across multiple areas.
#define MAX_AREA_COUNT (0xffffffff/AREAS_PER_GROUP)
#define AI_PER_GROUP (AREAS_PER_GROUP-1)
#define AREA_BITS (64*SEGS_PER_AREA)

typedef struct {
  // Number of zeros or ones before the group started.
  int64_t totals[2]; // total number of zeros or ones 
                     // when this group started
} group_info_t;

typedef struct {
  // Number of zeros or ones since the group but before the area started
  // Dealing with a run of > MAX_AREA_COUNT bits:
  //  - fill each area in the group with MAX_AREA_COUNT
  //  - fill up the last area in the group with the rest (since its total
  //    is not stored in an area_info.
  int32_t totals[2]; // total number of zeros or ones 
                     // when the area started (but within the group)
} area_info_t;

typedef struct {
  // The first bit of each area says whether we start with 
  // a 0 or a 1.
  uint64_t segs[SEGS_PER_AREA];
} area_t;

typedef struct {
  area_t areas[AREAS_PER_GROUP];
} group_t;

typedef struct {
  int64_t area_in_group;
  int64_t max_groups;
  int arealeft;
  uint64_t group_totals[2]; // total count in group.
  uint64_t area_totals[2];
  uint64_t within_area[2]; // within just 1 area
  int last_area_bit;
  int last_bit;
  uint64_t run;
} encode_info_t;

typedef struct {
  encode_info_t e;
  int64_t num_groups;
  int64_t num_areas;
  group_info_t* gi; // gi[0] refers to the start info for the
                    // group numbered 1; there are num_groups-1 of these.
  area_info_t* ai; // arranged so that the first group 
                   // goes from 0 .. (AREAS_PER_GROUP-1)
                   // (since the first one is always 0, it is not stored)
  group_t* groups;
} simpler_bseq_t;

static inline
int area_nonzero(const uint64_t segs[SEGS_PER_AREA])
{
  for( int i = 0; i < SEGS_PER_AREA; i++ ) {
    if( segs[i] ) return 1;
  }
  return 0;
}

static inline
int area_leadz(const uint64_t segs[SEGS_PER_AREA])
{
  for( int i = 0; i < SEGS_PER_AREA; i++ ) {
    if( segs[i] ) return 64*i + leadz64(segs[i]);
  }
  return AREA_BITS;
}

// Shifts left and returns the bottom ret_bits bits of the
// value destroyed in the process.
// Can only shift up to 127 bits.
static inline
uint64_t area_left_shift_read(uint64_t segs[SEGS_PER_AREA], int amt, int ret_bits)
{
  uint64_t ret = 0;

  if( amt == 0 ) return 0;

  // figure out what to return.
  // ret at most will be in 2 segments.
  if( amt >= 64 ) {
    amt -= 64;
    ret = segs[0] << amt;
    for(int i = 0; i < SEGS_PER_AREA - 1; i++ ) {
      segs[i] = segs[i+1];
    }
    segs[SEGS_PER_AREA - 1] = 0;
  }
  // OR in the data to ret_bits (could be a value from the if above)
  ret |= (segs[0] >> (64 - amt));

  // finally, make sure we only return ret_bits of ret.
  ret <<= (64 - ret_bits);
  ret >>= (64 - ret_bits);

  // shift starting from the left.
  for(int i = 0; i < SEGS_PER_AREA - 1; i++ ) {
    // what are we going to drop?
    segs[i] <<= amt;
    // bring in that many bits from the next reg.
    segs[i] |= (segs[i+1] >> (64 - amt) );
  }
  // left shift the last one.
  segs[SEGS_PER_AREA-1] <<= amt;

  return ret;
}

// "shift in" some data;
// do the shift and then OR in the data.
// amt can be greater than 64 bits - value is put in the least significant
// 64 bits. This routine assumes that value is only amt bits.
static inline
void area_left_shift_write(uint64_t segs[SEGS_PER_AREA], int amt, uint64_t value)
{
  if( amt == 0 ) return;

  if( amt >= 64 ) {
    amt -= 64;
    for(int i = 0; i < SEGS_PER_AREA - 1; i++ ) {
      segs[i] = segs[i+1];
    }
    segs[SEGS_PER_AREA-1] = 0;
  }

  // shift starting from the left.
  for(int i = 0; i < SEGS_PER_AREA - 1; i++ ) {
    // what are we going to drop?
    segs[i] <<= amt;
    // bring in that many bits from the next reg.
    segs[i] |= (segs[i+1] >> (64 - amt) );
  }
  // left shift the last one.
  segs[SEGS_PER_AREA-1] <<= amt;
  segs[SEGS_PER_AREA-1] |= value;

}

static inline
int area_read_top(uint64_t segs[SEGS_PER_AREA])
{
  // read the top bit and then shift left.
  return area_left_shift_read(segs, 1, 1);
}

static inline
uint64_t area_read_gamma(uint64_t segs[SEGS_PER_AREA])
{
  uint64_t value;
  int k, j;

  k = area_leadz(segs);
  j = 2*k + 1;
  value = area_left_shift_read(segs, j, k+1);

  return value;
}

/* returns the index of the matching group-info, or -1.
   Returns the group such that VALUE(g) <= target < VALUE(g+1)
   */
static inline
int64_t bsearch_gi(int64_t num_gi, const group_info_t* gi, int64_t target, unsigned char useA0, unsigned char useA1)
{
  int64_t a,b, middle;

#define VALUE(xxx) ( ((useA0) ? (gi[xxx].totals[0]) : (0)) + \
                     ((useA1) ? (gi[xxx].totals[1]) : (0)) )

  if( num_gi == 0 ) return -1;
  a = 0;
  b = num_gi - 1;
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

static inline
int lsearch_ai(int num_ai, const area_info_t* ai, int target, unsigned char useA0, unsigned char useA1)
{
  int a;


#define VALUE(xxx) ( ((useA0) ? (ai[xxx].totals[0]) : (0)) + \
                     ((useA1) ? (ai[xxx].totals[1]) : (0)) )

  if( num_ai == 0 ) return -1;

  a = 0;
  if( target < VALUE(a) ) return -1;
  for( a = 1; a < num_ai; a++ ) {
    if( target < VALUE(a) ) return a - 1;
  }
  return num_ai - 1;
#undef VALUE

}

static inline
int bsearch_ai(int num_ai, const area_info_t* ai, int target, unsigned char useA0, unsigned char useA1)
{
  int a,b, middle;


#define VALUE(xxx) ( ((useA0) ? (ai[xxx].totals[0]) : (0)) + \
                     ((useA1) ? (ai[xxx].totals[1]) : (0)) )

  if( num_ai == 0 ) return -1;
  a = 0;
  b = num_ai - 1;
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

void simpler_bseq_rank(const simpler_bseq_t* s, bseq_query_t* q)
{
  int64_t index;
  int64_t group;
  int area;
  int bit;
  int value;

  // index counts from 1.
  assert(q->index > 0);
  index = q->index - 1;

  // find the right group.
  // We didn't encode the first group; deal with that here.
  group = bsearch_gi(s->num_groups - 1, s->gi,
                     index, 1, 1);
  if( group >= 0 ) {
    q->occs[0] = s->gi[group].totals[0];
    q->occs[1] = s->gi[group].totals[1];
  } else {
    q->occs[0] = 0;
    q->occs[1] = 0;
  }
  group++; // back to normal numbering


  // Make the index just an index within the group

  if( AI_PER_GROUP > 0 ) {
    int64_t num_areas;
    area_info_t* ai;
    // find the right area.
    ai = & s->ai[group * AI_PER_GROUP];
    // num_areas = s->num_areas - group*AREAS_PER_GROUP; AREAS_PER_GROUP=AI_PER_GORUP+1
    num_areas = s->num_areas - (group * AI_PER_GROUP + group) ;
    if( num_areas > AREAS_PER_GROUP ) num_areas = AREAS_PER_GROUP;
    area = bsearch_ai(num_areas - 1, ai,
                      index - q->occs[0] - q->occs[1], 1, 1);
    if( area >= 0 ) {
      q->occs[0] += ai[area].totals[0];
      q->occs[1] += ai[area].totals[1];
    }
    area++; // back to normal numbering
  } else {
    area = 0;
  }

  // step 3: go through the segments.
  {
    uint64_t segs[SEGS_PER_AREA];
    memcpy(segs, s->groups[group].areas[area].segs, SEGS_PER_AREA*sizeof(uint64_t));
    //for( int i = 0; i < SEGS_PER_AREA; i++ ) {
    //  segs[i] = s->groups[group].areas[area].segs[i];
    //}
    //  segs[i] = s->groups[group].areas[area].segs[i];

    // grab the segments!
    bit = area_read_top(segs);
    while( area_nonzero(segs) ) {
      value = area_read_gamma(segs);
      if( q->occs[0] + q->occs[1] + value <= index ) {
        q->occs[bit] += value;
        bit = ! bit;
      } else { // index < sums.
        //printf("index < sums bit %i (%i,%i)\n", bit, q->occs[0], q->occs[1]);
        q->occs[bit] += 1 + index - q->occs[0] - q->occs[1];
        q->bit = bit;
        return;
      }
    }
  }
}

error_t simpler_bseq_setup_group(simpler_bseq_t* s)
{
  // start a new group.
  if( s->num_groups >= s->e.max_groups ) {
    s->e.max_groups += s->num_groups + 128;
    s->gi = (group_info_t*) realloc(s->gi, s->e.max_groups * sizeof(group_info_t));
    if( !s->gi ) return ERR_MEM;
    if( AI_PER_GROUP > 0 ) {
      s->ai = (area_info_t*) realloc(s->ai, s->e.max_groups * AI_PER_GROUP * sizeof(area_info_t));
      if( ! s->ai ) return ERR_MEM;
    }
    s->groups = (group_t*) realloc(s->groups, s->e.max_groups * sizeof(group_t));
    if( !s->groups ) return ERR_MEM;
  }

  s->e.area_in_group = 0;
  // clear the area totals.
  s->e.area_totals[0] = 0;
  s->e.area_totals[1] = 0;

  // zero out this new group.
  memset(&s->groups[s->num_groups], 0, sizeof(group_t));

  // if it's not the first group, write the totals we've got
  // on the way.
  if( s->num_groups > 0 ) {
    s->gi[s->num_groups-1].totals[0] = s->e.group_totals[0];
    s->gi[s->num_groups-1].totals[1] = s->e.group_totals[1];
  }
  return ERR_NOERR;
}

error_t simpler_bseq_start(simpler_bseq_t* s)
{
  memset(s, 0, sizeof(simpler_bseq_t));

  s->num_groups = 0; // we start on the current group
  s->num_areas = 0;
  s->e.last_bit = -1;
  s->e.last_area_bit = -2;
  s->e.arealeft = AREA_BITS;

  // allocate memory and set up the new group
  return simpler_bseq_setup_group(s);
}

error_t simpler_bseq_encode_run(simpler_bseq_t* s, int bit, uint64_t run)
{
  area_info_t* ai;
  uint64_t* segs;
  int bits;
  error_t err;

  //printf("Encoding index %i bit %i run %i\n", s->e.group_totals[0] + s->e.group_totals[1], bit, (int) run);
  //printf("Encoding run %i (%i)\n", bit, (int) run);
  // find the gamma encoding of our run.
  bits = log2lli(run);
  bits = 1 + 2 * bits;
  // to gamma encode, just store run in bits bits.

  // finish the current area if:
  //  - the encodeded value won't fit
  //  - we're encoding two of the same bit in a row
  //  - we were passed a -1 to indicate we're to finish.
  if( bits > s->e.arealeft ||
      bit == s->e.last_area_bit ||
      bit == -1 ) {
    // Finish the current area.
    segs = s->groups[s->num_groups].areas[s->e.area_in_group].segs;

    // finish off the area by padding it with zeros.
    while( s->e.arealeft > 64 ) {
      area_left_shift_write(segs, 64, 0);
      s->e.arealeft -= 64;
    }
    area_left_shift_write(segs, s->e.arealeft, 0);

    s->num_areas++;
    s->e.area_in_group++;
    s->e.last_area_bit = -2;
    s->e.arealeft = AREA_BITS;

    if( s->e.area_in_group == AREAS_PER_GROUP || bit == -1) {
      s->num_groups++;
      err = simpler_bseq_setup_group(s);
      if( err ) return err;
    }

    // Set the area-info
    if( s->e.area_in_group > 0 && AI_PER_GROUP > 0 ) {
      // first group doesn't get saved because its always 0.
      ai = & s->ai[(s->num_groups)*AI_PER_GROUP + s->e.area_in_group - 1];
      // write out the area info.
      ai->totals[0] = s->e.area_totals[0];
      ai->totals[1] = s->e.area_totals[1];
    }

    s->e.area_totals[0] = 0;
    s->e.area_totals[1] = 0;

  }

  if( bit != -1 ) {
    segs = s->groups[s->num_groups].areas[s->e.area_in_group].segs;
    if( s->e.arealeft == AREA_BITS ) {
      area_left_shift_write(segs, 1, bit);
      s->e.arealeft -= 1;
    }
    area_left_shift_write(segs, bits, run);
    s->e.arealeft -= bits;
    s->e.last_area_bit = bit;
    s->e.area_totals[bit] += run;
    s->e.group_totals[bit] += run;
  }

  return ERR_NOERR;
}

error_t simpler_bseq_append(simpler_bseq_t* s, int bit, int run)
{
  error_t err;
  int64_t amt;

  if( bit == s->e.last_bit ) {
    s->e.run += run;
  } else {
    // encode the last run
    if( s->e.last_bit != -1 ) {
      // encode the MAX_AREA_COUNT chunks
      // (unless we're on the last area in a group, in which case
      // we can store the whole thing.
      while( s->e.run > MAX_AREA_COUNT ) {
        if( s->e.area_in_group == AREAS_PER_GROUP - 1 ) {
          // We're in the last area; it's totals won't need updating.
          // Store the whole thing.
          amt = s->e.run;
        } else {
          amt = MAX_AREA_COUNT;
        }
        err = simpler_bseq_encode_run(s, s->e.last_bit, amt);
        if( err ) return err;
        s->e.run -= amt;
      }
      if( s->e.run > 0 ) {
        err = simpler_bseq_encode_run(s, s->e.last_bit, s->e.run);
        if( err ) return err;
      }
    }
    if( bit == -1 ) {
      // finish up!
      err = simpler_bseq_encode_run(s, -1, 0);
      if( err ) return err;
    }
    s->e.run = 1;
    s->e.last_bit = bit; 
  }
  return ERR_NOERR;
}

error_t simpler_bseq_finish(simpler_bseq_t* s)
{
  error_t err;
  // append a -1 to clear out the last run.
  err = simpler_bseq_append(s, -1, 0);
  if( err ) return err;

  return ERR_NOERR;
}

void test_areas(void)
{
  uint64_t segs[SEGS_PER_AREA];
  uint64_t seg_data[] = {0x001834b2001bea33LL, 0x228a3000001bea33LL};
  uint64_t got, expect;
  int i,j;
  if( SEGS_PER_AREA > 1 ) {
    segs[0] = seg_data[0];
    segs[1] = seg_data[1];
    for( i = 2; i < SEGS_PER_AREA; i++ ) {
      segs[i] = 0;
    }
    // 1 - try reading it all one bit at a time.
    // try out area_left_shift_read.
    for( i = 0; i < 64; i++ ) {
      got = area_left_shift_read(segs, 1, 1);
      expect = (seg_data[0] >> (64 - i - 1)) & 1;
      assert( got == expect );
    }
    for( i = 0; i < 64; i++ ) {
      got = area_left_shift_read(segs, 1, 1);
      expect = (seg_data[1] >> (64 - i - 1)) & 1;
      assert( got == expect );
    }
    // try reading something that spans 2 words.
    segs[0] = seg_data[0];
    segs[1] = seg_data[1];

    // check that read/write with 0 shift do nothing.
    area_left_shift_read(segs, 0, 0);
    assert( segs[0] == seg_data[0] );
    assert( segs[1] == seg_data[1] );
    area_left_shift_write(segs, 0, 0);
    assert( segs[0] == seg_data[0] );
    assert( segs[1] == seg_data[1] );

    // get the top 5 bits of the 2nd word.
    got = area_left_shift_read(segs, 64+5, 5);
    assert( got == 4);

    segs[0] = seg_data[0];
    segs[1] = seg_data[1];
    // get the next 10 bits of the 2nd word.
    got = area_left_shift_read(segs, 64+15, 10);
    assert( got == 0x145);

    if( SEGS_PER_AREA > 2 ) {
      uint64_t chunks[] = {0x1b, 0x1, 0x5, 0x0, 0x1234567812345678LL, 0x102};
      int sizes[] = {10, 1, 7, 15, 96, 15};
      int num = sizeof(sizes)/sizeof(int);

      // try doing some writing.
      for( i = 0; i < SEGS_PER_AREA; i++ ) segs[i] = 0;

      for( i = 0, j = 0; i < num && j < 64 * SEGS_PER_AREA; i++ ) {
        area_left_shift_write(segs, sizes[i], chunks[i]);
        j += sizes[i];
      }
      // write the rest.
      while( j < 64*SEGS_PER_AREA ) {
        area_left_shift_write(segs, 1, 0);
        j++;
      }


      for( i = 0, j = 0; i < num && j < 64 * SEGS_PER_AREA; i++ ) {
        got = area_left_shift_read(segs, sizes[i], 64);
        j += sizes[i];
        expect = chunks[i];
        assert( got == expect );
      }
    }
  }
}

/* Experiments with binary sequences!
   */
int main( int argc, char** argv )
{
  FILE* f;
  error_t err;
  buffer_t buf;
  bwt_reader_t bwt;
  bwt_entry_t ent;
  int bit;
  int count[2];

  test_areas();

  printf("All binary sequence tests passed\n");

  if( argc == 1 ) return 0;

  f = fopen(argv[1], "r");
  assert(f);

  printf("Reading least-significant bits of BWT to create sequence\n");
  err = bwt_reader_open(&bwt, f);
  die_if_err(err);

  buf = build_buffer(0, NULL);

  bsInitWrite( &buf );
  do {
    err = bwt_reader_read(&bwt, &ent);
    die_if_err(err);
    if( ent.ch != INVALID_ALPHA ) {
      // just put the least significant bit.
      bit = ent.ch & 1;
      for( int j = 0; j < ent.run; j++ ) {
        err = buffer_extend(&buf, 8);
        die_if_err(err);
        //printf("bit %i\n", bit);
        bsW24( &buf, 1, bit );
      }
    }
  } while( ent.ch != INVALID_ALPHA );

  bsFinishWrite( & buf );
  bwt_reader_close( &bwt );
  fclose(f);

  {
    int bit;
    int num = 8*buf.len;
    simpler_bseq_t s;
    bseq_query_t q;

    memset(&s, 0, sizeof(simpler_bseq_t));
    err = simpler_bseq_start(&s);
    die_if_err(err);
    // try the "simple" impl.
    buf.pos = 0;
    bsInitRead( &buf );
    for( int i = 0; i < num; i++ ) {
      bit = bsR24( &buf, 1 );
      //if( i >= 53900 && i < 54200 ) {
      //  printf("i is %i bit is %i\n", i, bit);
      //}
      err = simpler_bseq_append(&s, bit, 1);
      die_if_err(err);
    }
    err = simpler_bseq_finish(&s);
    die_if_err(err);
    bsFinishRead( &buf );

    {
      int size;
      size = s.num_groups * (sizeof(group_info_t)+sizeof(group_t));
      if( AI_PER_GROUP > 0 ) {
        size += s.num_areas * sizeof(area_info_t);
      }
      printf("simpler bseq is %i/%i bytes (%f)\n", size, 
          (int) buf.len,
          (double) size / (double) buf.len);
    }

    printf("Running rank queries\n");
    count[0] = 0;
    count[1] = 0;
    buf.pos = 0;
    bsInitRead( &buf );
    start_clock();
    for( int i = 0; i < num; i++ ) {
      q.index = i+1; // count from 1.
      simpler_bseq_rank(&s, &q);
//#define CHECK
#ifdef CHECK
      bit = bsR24(&buf,1);
      count[bit]++;
      //if( i >= 53900 ) {
       // printf("i is %i bit is %i count is (%i,%i)\n", i, bit, count[0], count[1]);
      //}
      if( i > 54200 ) {
      assert(count[0]==q.occs[0]);
      assert(count[1]==q.occs[1]);
      }
#endif
    }
    stop_clock();
    print_timings("simpler_bseq_rank", num);

  }


  // now we have an array of alpha_ts.
  { // try the bseq impl.
    int zlen = 0;
    unsigned char* zdata = NULL;
    int num = 8*buf.len;
    bseq_query_t q;

    printf("Constructing bseq\n");
    err = bseq_construct(&zlen, &zdata, 8*buf.len, buf.data, NULL);
    die_if_err(err);

    printf("bseq is %i/%i bytes\n", zlen, (int) (buf.len));

    printf("Running rank queries\n");
    count[0] = 0;
    count[1] = 0;
    buf.pos = 0;
    bsInitRead( &buf );
    start_clock();
    for( int i = 0; i < num; i++ ) {
      q.index = i+1; // count from 1.
      bseq_rank(zdata, &q);
#ifdef CHECK
      count[bsR24(&buf,1)]++;
      assert(count[0]==q.occs[0]);
      assert(count[1]==q.occs[1]);
#endif
    }
    stop_clock();
    print_timings("bseq_rank", num);

    free(zdata);
  }

  free(buf.data);

  printf("All bseq tests PASSED\n");
}
