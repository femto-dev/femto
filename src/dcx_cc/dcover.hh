/*
  (*) 2008-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/dcover.hh
*/
#ifndef _DCOVER_HH_
#define _DCOVER_HH_

#include "compile_time.hh"
#include "varint.hh"

#include <cstdlib>
#include <cassert>
#include <cstdio>
#include "error.hh"


static const int cover3[]  = {0,1};		//alternately, {1,2}
static const int cover7[]  = {0,1,3};
static const int cover13[] = {0,1,3,9};
static const int cover21[] = {0,1,6,8,18};
static const int cover31[] = {0,1,3,8,12,18};
static const int cover39[] = {0,1,16,20,22,27,30};
static const int cover57[] = {0,1,9,11,14,35,39,51};
static const int cover73[] = {0,1,3,7,15,31,36,54,63};
static const int cover91[] = {0,1,7,16,27,56,60,68,70,73};
static const int cover95[] = {0,1,5,8,18,20,29,31,45,61,67};
static const int cover133[] = {0,1,32,42,44,48,51,59,72,77,97,111};
static const int cover1024[] = {0, 1, 2, 3, 4, 5, 6, 13, 26, 39, 52, 65, 78, 91, 118, 145, 172, 199, 226, 253, 280, 307, 334, 361, 388, 415, 442, 456, 470, 484, 498, 512, 526, 540, 541, 542, 543, 544, 545, 546};
static const int cover2048[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 19, 38, 57, 76, 95, 114, 133, 152, 171, 190, 229, 268, 307, 346, 385, 424, 463, 502, 541, 580, 619, 658, 697, 736, 775, 814, 853, 892, 931, 951, 971, 991, 1011, 1031, 1051, 1071, 1091, 1111, 1131, 1132, 1133, 1134, 1135, 1136, 1137, 1138, 1139, 1140};
static const int cover4096[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 27, 54, 81, 108, 135, 162, 189, 216, 243, 270, 297, 324, 351, 378, 433, 488, 543, 598, 653, 708, 763, 818, 873, 928, 983, 1038, 1093, 1148, 1203, 1258, 1313, 1368, 1423, 1478, 1533, 1588, 1643, 1698, 1753, 1808, 1863, 1891, 1919, 1947, 1975, 2003, 2031, 2059, 2087, 2115, 2143, 2171, 2199, 2227, 2255, 2256, 2257, 2258, 2259, 2260, 2261, 2262, 2263, 2264, 2265, 2266, 2267, 2268};
static const int cover8192[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 37, 74, 111, 148, 185, 222, 259, 296, 333, 370, 407, 444, 481, 518, 555, 592, 629, 666, 703, 778, 853, 928, 1003, 1078, 1153, 1228, 1303, 1378, 1453, 1528, 1603, 1678, 1753, 1828, 1903, 1978, 2053, 2128, 2203, 2278, 2353, 2428, 2503, 2578, 2653, 2728, 2803, 2878, 2953, 3028, 3103, 3178, 3253, 3328, 3403, 3478, 3516, 3554, 3592, 3630, 3668, 3706, 3744, 3782, 3820, 3858, 3896, 3934, 3972, 4010, 4048, 4086, 4124, 4162, 4200, 4201, 4202, 4203, 4204, 4205, 4206, 4207, 4208, 4209, 4210, 4211, 4212, 4213, 4214, 4215, 4216, 4217, 4218};

template<int Period>
struct DcoverTable
{
  // unknown table!
};

#define DECLARE_COVER(p) \
  template<> struct DcoverTable<p> { \
    enum { \
      period = p, \
      sample_size = sizeof(cover##p)/sizeof(int), \
      nonsample_size = period - sample_size \
    }; \
    const int (&cover)[sample_size]; \
    DcoverTable() : cover(cover##p) { } \
  };

DECLARE_COVER(3);
DECLARE_COVER(7);
DECLARE_COVER(13);
DECLARE_COVER(21);
DECLARE_COVER(31);
DECLARE_COVER(39);
DECLARE_COVER(57);
DECLARE_COVER(73);
DECLARE_COVER(91);
DECLARE_COVER(95);
DECLARE_COVER(133);
DECLARE_COVER(1024);
DECLARE_COVER(2048);
DECLARE_COVER(4096);
DECLARE_COVER(8192);

#undef DECLARE_COVER

template<int Period>
class Dcover
{
 public:
  typedef DcoverTable<Period> cover_table_t;
  enum {
    period = cover_table_t::period,
    sample_size = cover_table_t::sample_size,
    nonsample_size = cover_table_t::nonsample_size 
  };

  static const int num_bytes_period_t = CompileTimeNumBytes<period>::num_bytes;
  typedef typename min_word_getter<num_bytes_period_t>::register_type period_t;

 private:
  cover_table_t cover_table;
  const int (&cover)[sample_size];


  int sample[period];    // sample[i mod v]=index s.t. cover[index]=i, else -1
  int nonsample[period]; // nonsample[i mod v]=number of nonsample < i.
  int table[period][period];
                          // table[i][j] == i_amt
                          // where i_amt is the number of sample
                          //   suffixes to skip of those immediately after i
                          // table[j][i] == j_amt
                          // where j_amt is the number of sample
                          //   suffixes to skip of those immediately after j
 public:

  Dcover() : cover(cover_table.cover)
  {
    // double-check to be sure a valid difference cover is being used
    if (period <= 0)
          throw error(ERR_INVALID_STR("Dcover err: bad cover definition"));
    for (int i=0; i<sample_size; i++) {
      if (cover[i]<0 || cover[i] >= period)
          throw error(ERR_INVALID_STR("Dcover err: bad cover definition")); 
    }
   
    // make useful look-up tables
    dc_make_sample();
    dc_make_nonsample();
    dc_make_table();
  }


  ~Dcover()
  {
  }

  // used in unit tester.
  int get_table(int i, int j)
  {
    return table[i][j];
  }

  const static Dcover<Period> g;

  // Get the instance for this one
  /*static const Dcover<Period> get(void)
  {
    static Dcover<Period> mine;
    return mine;
  }*/

  void which_samples_to_use(int i, int j, int* i_off, int* j_off) const
  {
    if( EXTRA_CHECKS ) {
      assert( i>=0 && i< period );
      assert( j>=0 && j< period );
    }
    *i_off = table[i][j];
    *j_off = table[j][i];
    if( EXTRA_CHECKS ) {
      assert( 0 <= *i_off && *i_off < sample_size );
      assert( 0 <= *j_off && *j_off < sample_size );
    }
  }


  int get_sample(int i) const // return index j s.t. cover[j] == i, or -1.
  {
    if( EXTRA_CHECKS ) assert( i>=0 && i< period );
    return sample[i];
  }

  int get_nonsample(int i) const // return number of nonsample < i
  {
    if( EXTRA_CHECKS ) assert( i>=0 && i< period );
    return nonsample[i];
  }


  bool in_cover(int i) const // returns true if i is in the cover.
  {
    if( EXTRA_CHECKS ) assert( i>=0 && i< period );
    return sample[i] != -1;
  }

  int get_cover(int i) const // returns cover[i]
  {
    if( EXTRA_CHECKS ) assert( i>=0 && i< sample_size );
    return cover[i];
  }

 private:
  // Return the number of samples to skip after i to get to i+l.
  // i+l must be in the difference cover.
  int get_sample_offset(int i, int l)
  {
    int num_in_cover=0;
    for( int k = 0; k < l; k++ ) {
      if( in_cover( (i + k) % period ) ) num_in_cover++;
    }
    assert( in_cover( (i + l) % period ) );
    return num_in_cover;
  }

  // creates useful look-up tables
  void dc_make_table()
  {
    int i,j,l;

    assert(sample); // sample is used below.

    for( i = 0; i < period; i++ ) {
      for( j = 0; j <= i; j++ ) {
        // Compute the minimum l-distance
        // so that both i+l and j+l are in the cover.
        for( l = 0; l < period; l++ ) {
          if( in_cover((i+l)%period) && in_cover((j+l)%period) ) break; 
        }
        assert( l != period ); // it's not a difference cover!
        // OK, now compute the i_amt and j_amt
        // to get to that sample suffix.
        table[i][j] = get_sample_offset(i, l);
        table[j][i] = get_sample_offset(j, l);
      }
    }

    // Check the table.
    // First check i==j.
    for( i = 0; i < period; i++ ) {
      int amt = table[i][i];
      assert( amt == 0 ); // it's always just the next sample!
    }
  }

  void dc_make_sample()
  {
     for(int x=0; x<period; x++) {
       sample[x] = cover_reverse_lookup(x);
     }
  }


  void dc_make_nonsample()
  {
     int num = 0;
     for(int x=0; x<period; x++) {
       nonsample[x] = num;
       if( ! is_in_dc(x) ) num++;
     }
  }

  // slow routines
  int cover_reverse_lookup(int x)
  {
    for(int i=0; i<sample_size; i++)
    {
      if (cover[i]==x) return i;
    }
    return -1;
  }

  int is_in_dc(int x)
  {
    return (cover_reverse_lookup(x) != -1);
  }
};



#endif
