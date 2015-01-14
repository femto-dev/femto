/*
  (*) 2009-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/bucket_sort.hh
*/
/* Implementation of counting-bucket-sort routines.
 *
 */

/* PERFORMANCE NOTES:
 *   -- the prefetching in the distribute_inplace gives about 50% boost
 *   -- it is key to compile with -O4 on gcc
 *   -- with these things, we see about 200 MB/s serial on an 8-core Xeon
 *      compare with   .......... about 100 MB/s for std::sort
 *     
 *   -- for first pass in 1GB, count takes 0.375s; distribute 1.04 s.
 *   -- adding more prefetching (in count, or 2nd spot in distribute)
 *      does not seem to help.
 *
 *   -- The file versions of these routines seem to work the
 *      best with mmap and madvise.... in any case, that is a simpler
 *      interface.
 *
 */

/* 
 * Records:
 * must have fixed_size_record_tag and complete_record_tag
 * records are copied without being 'encoded'
 *
 * Criterions:
 * return_key_parts_and_compare_criterion_tag.
 * get_num_key_parts is equal for all records.
 *
 * Lastly, the records must support a default constructor
 *   and = (assignment) operator in order for us to compute
 *   the min/max records.
 *
 * compareAfterKey is not implemented.
 */

#ifndef _BUCKET_SORT_HH_
#define _BUCKET_SORT_HH_


extern "C" {
  // for pread/pwrite
  #ifndef _XOPEN_SOURCE
  #define _XOPEN_SOURCE 600
  #endif

  #include <unistd.h>
  #include <fcntl.h>

  #include <stdlib.h> // quicksort

  //#include <aio.h>

  #include "page_utils.h" // allocate/free pages
  #include "bit_funcs.h" // log2lli
}
#include <climits>
#include <cstring>
#include <bitset>
#include <memory> // auto_ptr
#include <algorithm>
#include "distribute_utils.hh"
#include "utils.hh" //PTR_ADD
#include "compare_record.hh"
#include "mmap.hh"
#include "file_pipe_context.hh"


// Potentially tunabe parameters:

#define OMP_CHUNK_SIZE (0.5*1024*1024)
#define OMP_DIST_LIMIT 0
// (1.5*1024*1024*1024)
#define OMP_LIMIT (4*OMP_CHUNK_SIZE)
#define OMP_LIMIT_BINS 16

// to ... something like 3GB used.
// Note -- with hugepages, a page is 2 or 4 MB..
#define BUFFER_SIZE (4*1024*1024)
//#define BUFFER_SIZE (16*1024*1024)
//#define COUNT_CHUNK (2*1024*1024)
#define COUNT_CHUNK BUFFER_SIZE

#define DISTRIBUTE_BUFFER
#define DISTRIBUTE_BUFFER_SIZE 5
#define PERMUTE_BUFFER_SIZE 16

// How many bins to use? Algo will generally 1/2 to 1 times this number.
// Must be a power of 2 (other values would work.. but there are bugs)
#define MAXBINS_DEFAULT 256

// With this or fewer records, switch to simple_sort
#define SORTSWITCH_DEFAULT 256

// With this or fewer records, when permuting, permute directly.
#define PERMUTESWITCH_DEFAULT (4*1024*1024)

#define SWITCH_STL

//#define PRINT_TIMING 1
//#define PRINT_TIMING   100000000
//#define PRINT_PROGRESS 10000000
#define PRINT_TIMING   0
#define PRINT_PROGRESS 0
#define PRINT_DEBUG 0

#define record_size (RecordTraits<Record>::record_size)
#define iterator_t typename RecordTraits<Record>::iterator_t

#define prefetch(data) (__builtin_prefetch(RecordTraits<Record>::getbase(data),1))
#define get_iter(data) (RecordTraits<Record>::getiter(data))
#define get_ptr(iter) (RecordTraits<Record>::getbase(iter))

template<typename Key>
struct MinMax {
  Key min;
  Key max;
};

template<typename Key, 
         typename Offset,
         int MaxBins>
struct Bins {
  Offset offsets[MaxBins]; // maybe heap-allocate these?
  MinMax<Key> minmax[MaxBins];
};

struct BucketSorterSettings {
  bool Parallel;
  bool Permute;
  int SortSwitch;
  int SwitchPermute;
  bool compareAfterKey; // if true, we will use compareAfter
                        // once we get down to a bin or a small enough
                        // number of records.
  BucketSorterSettings()
    : Parallel(false),
      Permute(false),
      SortSwitch(SORTSWITCH_DEFAULT),
      SwitchPermute(PERMUTESWITCH_DEFAULT),
      compareAfterKey(false)
  {
  }
};

/* Compute the min and max records. */
template<typename Record, /* Record type */
         typename Criterion, /* Comparison criterion */
         typename CompareAfter = CompareSameCriterion, /* Compare after */
         typename Size = uint64_t,  /* Type to count records */
         int MaxBins = MAXBINS_DEFAULT
         >
struct BucketSorter {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef CompareAfter after_criterion_t;
  typedef DoubleRecordSortingCriterion<Record,Criterion,CompareAfter> both_criterion_t;
  typedef Size offset_t;
  typedef DistributeUtils<Record,Criterion> DU;
  // Note -- it does not make sense to run the permutation
  // algorithm with 1-pass, since counting is trivial when permuting.
  typedef typename criterion_t::key_t key_t;
  typedef MinMax<key_t> minmax_t;
  typedef Bins<key_t,offset_t,MaxBins> bins_t;
  typedef typename DU::shift_t shift_t;
  typedef KeyTraits<key_t> key_traits_t;
  typedef BucketSorterSettings settings_t;

  static bool less_than(const key_t& a, const key_t& b)
  {
    return DU::less_than(a,b) ;
  }

  static bool less_than(both_criterion_t ctx, Record a, Record b)
  {
    return ctx.less(a,b);
  }

  static ssize_t bin_for_key(const key_t& key, shift_t shift)
  {
    return DU::bin_for_key(key, shift);
  }
  static ssize_t bin_for_record(Criterion ctx, const Record& r, shift_t shift)
  {
    return DU::bin_for_record(ctx, r, shift);
  }
  static size_t num_bits(key_t k)
  {
    return DU::num_bits(k);
  }
  static size_t num_bits_in_common(key_t min, key_t max)
  {
    return DU::num_bits_in_common(min,max);
  }

  static shift_t compute_shift(key_t min, key_t max)
  {
    return DU::compute_shift(min, max, MaxBins);
  }

  // Used only in permuting.
  static void set_key(key_t& a, offset_t num)
  {
    key_traits_t::set(a, num);
  }

  static void shell_sort(both_criterion_t ctx, iterator_t a, int n)
  {
    // Based on Sedgewick's Shell Sort -- see
    // Analysis of Shellsort and Related Algorithms 1996
    int i, j, k, h;
    Record v;
    //int incs[] = { 701, 301, 132, 57, 23, 10, 4, 1 };
    int incs[] = { 126, 85, 24, 9, 4, 1 };
    //int incs[] = { 85, 24, 9, 4, 1 };
    //int incs[] = { 38, 9, 4, 1 };
    //int incs[] = { 17, 4, 1 };
    //int incs[] = { 9, 1 };
    //int incs[] = { 1 };
    int num_incs = sizeof(incs)/sizeof(int);
    for ( k = 0; k < num_incs; k++) {
      h = incs[k];
      for (i = h; i < n; i++)
      {
        v = a[i];
        j = i;

        while (j >= h && less_than(ctx,v,a[j-h]) ) {
          a[j] = a[j-h];
          j -= h;
        }
        a[j] = v;
      }
    }
  }
  static void insertion_sort(both_criterion_t ctx, iterator_t a, int n)
  {
    int i,j;
    Record v,tmp;
    for( i = 1; i < n; i++ ) {
      // v = A[i]
      v = a[i];
      j = i - 1;
      while( j>=0 && less_than(ctx,v,a[j]) ) {
        a[j+1] = a[j];
        a[j] = v;
        j--;
      }
    }
  }
  static void stl_sort(both_criterion_t ctx, iterator_t a, int n)
  {
    std::sort(a, a+n, ctx);
  }
  static void simple_sort(both_criterion_t ctx, iterator_t a, int n)
  {
#ifdef SWITCH_STL
    stl_sort(ctx,a,n);
#else
    shell_sort(ctx,a,n);
    //insertion_sort(ctx,a,n);
#endif
  }

  static void compute_min_max(Criterion ctx,
                       /*output:*/ key_t& min_out, key_t& max_out,
                       /*input: */ offset_t n, const iterator_t data)
  {
    offset_t i;
    key_t min;
    key_t max;

    for(i = 0; i < n; i++ ) {
      Record r = data[i];
      key_t key = ctx.get_key(r);
      if( i > 0 ) {
        if( less_than(key,min) ) min = key;
        if( less_than(max,key) ) max = key;
      } else {
        min = key;
        max = key;
      }
    }

    min_out = min;
    max_out = max;
  }

  static void check_sorted_ptrs_impl(Criterion ctx, iterator_t data, offset_t start_n, offset_t end_n, offset_t& cur, Record& last, const settings_t* settings)
  {
    for( offset_t i = start_n; i < end_n; i++ ) {
      Record r = data[i];
      if( cur > 0 ) {
        int cmp = DU::compare(ctx, last, r);
        if( settings->Permute ) {
          assert( cmp < 0 ); // last < r since no duplicates allowed.
        } else {
          assert( cmp <= 0 ); // last <= r; duplicates allowed
        }
      }
      last = r;
    }
  }

  /* Count for a distribution pass.
   * Counts into the bins of this object (output)
   * sets bins[i].num */
  static void count_ptrs_impl(Criterion ctx,
                              /*input: */ iterator_t data,
                              offset_t start_n, offset_t end_n,
                              shift_t shift,
                              bins_t* restrict bins)
  {
    for(offset_t i = start_n; i < end_n; i++ ) {
      Record r = data[i];
      int bin = bin_for_record(ctx,r, shift);
      key_t key = ctx.get_key(r);
      // adjust min and max.
      if( bins->offsets[bin] > 0 ) {
        if( less_than(key, bins->minmax[bin].min) ) {
          bins->minmax[bin].min = key;
        }
        if( less_than(bins->minmax[bin].max, key) ) {
          bins->minmax[bin].max = key;
        }
      } else {
        bins->minmax[bin].min = key;
        bins->minmax[bin].max = key;
      } 
      bins->offsets[bin]++;
    }
  }
  static void count_ptrs(Criterion ctx,
                         key_t min, key_t max,
                         /*input: */ iterator_t data,
                         offset_t start_n, offset_t end_n,
                         shift_t shift,
                         bins_t* restrict bins, // stores offsets and minmax.
                         const settings_t* settings)
  {
    if( can_permute(ctx, min, max, start_n, end_n, settings) ) {
      // Instead of counting the array, just assign counts
      // based on the known distribution.
      count_for_permute(ctx,min,max,shift,bins);

      if( EXTRA_CHECKS ) {
        bins_t other_bins;
        memset(&other_bins, 0, sizeof(bins_t));
        count_ptrs_impl(ctx, data, start_n, end_n, shift, &other_bins);
        for( int bin = 0; bin < MaxBins; bin++ ) {
          assert(other_bins.offsets[bin] == bins->offsets[bin]);
          if( other_bins.offsets[bin] > 0 ) {
            assert(0 == DU::compare(other_bins.minmax[bin].min,bins->minmax[bin].min));
            assert(0 == DU::compare(other_bins.minmax[bin].max,bins->minmax[bin].max));
          }
        }
      }
      return;
    }

    count_ptrs_impl(ctx, data, start_n, end_n, shift, bins);
  }

  // Used in permutation code ONLY!
  static offset_t offset_for_key(key_t min, const key_t& key,
                                 offset_t min_idx)
  {
    if( EXTRA_CHECKS ) assert(num_bits(min) <= 8*sizeof(offset_t));

    offset_t min_key = DU::right_shift(min, 0);
    offset_t dst_key = DU::right_shift(key, 0);
    offset_t dst_idx = min_idx + dst_key - min_key;
    return dst_idx;
  }

  static offset_t offset_for_record(Criterion ctx,
                                    key_t min, const Record& r,
                                    offset_t min_idx)
  {
    return offset_for_key(min, ctx.get_key(r), min_idx);
  }
                                         

  static void permute_inplace(Criterion ctx,
                              key_t min, key_t max,
                              iterator_t data,
                              offset_t start_n, offset_t end_n)
  {
    offset_t i;
    Record tmp;
    static const int max_buf = PERMUTE_BUFFER_SIZE;
    Record buf[max_buf];
    int used_buf;

    if( PRINT_DEBUG ) printf("Permuting!!!\n");

    assert(num_bits(min) <= 8*sizeof(offset_t));
    assert(num_bits(max) <= 8*sizeof(offset_t));

#define SWAP(a,b) {tmp = a; a = b; b = tmp;}
    i = start_n;
    used_buf = 0;
    // Fill up our list with some records
    // we need to put in the right place.
    while( used_buf < max_buf && i < end_n ) {
      buf[used_buf] = data[i];
      used_buf++;
      i++;
    }

    while( used_buf > 0 ) {
      // Now go through the records in our buf,
      // looking for the right home.
      for( int j = 0; j < used_buf; j++ ) {
        offset_t dst_idx = offset_for_record(ctx, min, buf[j], start_n);
        // prefetch next does not seem to help
        //unsigned long next_idx = (j+1<used_buf)?(buf[j+1].key):(dst_idx);
        //__builtin_prefetch(&values[next_idx], 1);

        // swap r with its final home, values[r.key]
        SWAP(buf[j],data[dst_idx]);
        if( EXTRA_CHECKS && ! (dst_idx < i) ) {
          // No two records should have the same offset.
          offset_t other_idx = offset_for_record(ctx, min, buf[j], start_n);
          assert(other_idx != dst_idx);
        }

        if( dst_idx < i ) {
          // We just put it where it belongs and finished our cycle.
          // Get the next element from the input.
          
          // Advance i to next out-of-place one.
          while( i < end_n ) {
            tmp = data[i];
            offset_t dst_idx = offset_for_record(ctx, min, tmp, start_n);
            if( dst_idx != i ) break;
            i++;
          }

          // Now, put an empty at the end, or put the value in buf.
          if( i < end_n ) {
            buf[j] = data[i];
            i++;
          } else {
            // Move the last one to this position.
            SWAP(buf[j],buf[used_buf-1]);
            // Reduce used_buf.
            used_buf--;
          }
        }
      }
    }

#undef SWAP

  }

  /* distribute_inplace */
  static void distribute_inplace(Criterion ctx,
                                 iterator_t data,
                                 shift_t shift,
                                 bins_t* restrict bins,
                                 offset_t end_offsets[MaxBins],
                                 int firstbin, int lastbin
                                 )
  {
    Record tmp;
    int bin, curbin, stopbin;

    curbin = firstbin;
    stopbin = lastbin - 1; // when all but last are distributed,
                           // last must also be in place.

    while(1) {
      // Find the next bin that isn't totally in place.
      while( curbin < stopbin &&
             bins->offsets[curbin] == end_offsets[curbin] ) curbin++;
      // Exit the loop if we're on stopbin.
      if( curbin == stopbin ) break;

#ifdef DISTRIBUTE_BUFFER
      // This is an alternative way to implement that would
      // do better on architectures that need loop unrolling.
      static const int max_buf = DISTRIBUTE_BUFFER_SIZE;
      Record buf[max_buf];
      int used_buf = 0;
      offset_t i = bins->offsets[curbin];
      offset_t end = end_offsets[curbin];

      // Fill buf with records from this bin.
      while(used_buf < max_buf && i < end ) {
        buf[used_buf] = data[i];
        //std::cout << "Loading buf" << used_buf << " with " << buf[used_buf].to_string(ctx) << std::endl;
        used_buf++;
        i++;
      }

      while( used_buf > 0 ) {
        int j=0;
        // Now go through the records in buf
        // putting them in their right home.
        if( used_buf == max_buf ) {
          for( j = 0; j < max_buf; j++ ) {
            // Put buf[j] in its right home.
            bin = bin_for_record(ctx,buf[j], shift);
            tmp = data[bins->offsets[bin]];
            data[bins->offsets[bin]] = buf[j];
            buf[j] = tmp;
            if( EXTRA_CHECKS ) assert(bins->offsets[bin]<end_offsets[bin]);
            bins->offsets[bin]++;
            //__builtin_prefetch(&data[bins->offsets[bin]],1);
            prefetch(data + bins->offsets[bin]);
            // If buf[j] is in curbin, what is in buf[j] is invalid.
            if( bin == curbin ) {
              if( EXTRA_CHECKS ) assert(bins->offsets[bin]<=i);
              // Either shrink buf or get next.
              if( i < end ) {
                buf[j] = data[i];
                i++;
              } else {
                // handled below
                break;
              }
            }
          }
          if( j != max_buf ) {
            buf[j] = buf[used_buf-1];
            used_buf--;
          }
        }
        for( ; j < used_buf; j++ ) {
          // Put buf[j] in its right home.
          bin = bin_for_record(ctx,buf[j], shift);
          tmp = data[bins->offsets[bin]];
          data[bins->offsets[bin]] = buf[j];
          buf[j] = tmp;
          if( EXTRA_CHECKS ) assert(bins->offsets[bin]<end_offsets[bin]);
          bins->offsets[bin]++;
          //__builtin_prefetch(&data[bins->offsets[bin]],1);
          prefetch(data + bins->offsets[bin]);
          // If buf[j] is in curbin, what is in buf[j] is invalid.
          if( bin == curbin ) {
            if( EXTRA_CHECKS ) assert(bins->offsets[bin]<=i);
            // Either shrink buf or get next.
            if( i < end ) {
              buf[j] = data[i];
              i++;
            } else {
              // Move the empty last one to this position.
              buf[j] = buf[used_buf-1];
              used_buf--;
            }
          }
        }
      }
#else
      Record r;
      // Take the first record from that bin.
      r = data[bins->offsets[curbin]];
      // Find a home for r; exit if that home is in curbin.
      while(1) {
        bin = bin_for_record(ctx,r, shift);
        // given r in bin, place r in the correct spot.
        if( bin == curbin ) break;
        // swap data[bins[bin].offset] and r.
        tmp = data[bins->offsets[bin]];
        data[bins->offsets[bin]] = r;
        r = tmp;
        // now we have used that spot.
        bins->offsets[bin]++;
        // prefetch for write..
        //__builtin_prefetch(&data[bins->offsets[bin]],1);
        prefetch(data + bins->offsets[bin]);
        if(EXTRA_CHECKS) assert(bins->offsets[bin] <= end_offsets[bin]);
      }
      // on exiting the loop, bin == curbin.
      if(EXTRA_CHECKS) assert(bin == curbin);

      // we place r in the spot left by data[k].
      data[bins->offsets[bin]] = r;
      // now we have used that spot.
      bins->offsets[bin]++;
      // this one actually seems to degrade performance.
      //__builtin_prefetch(&data[offsets[bin]]);
      if(EXTRA_CHECKS) assert(bins->offsets[bin] <= end_offsets[bin]);
#endif
    }

    // Note that the offset number for the last bin may not
    // be correctly updated.

  }

  static bool can_permute(Criterion ctx,
                        key_t min, key_t max,
                        offset_t start_n, offset_t end_n,
                        const settings_t* settings)
  {
    /*return( KeysAreUnique && 
            end_n - 1 == offset_for_key(min, max, start_n) );
            */
    if( settings->Permute ) {
      if( end_n > 0 ) assert(end_n - 1 == offset_for_key(min, max, start_n));
      return true;
    } else {
      return false;
    }
  }
 
  static bool will_permute(Criterion ctx,
                        key_t min, key_t max,
                        offset_t start_n, offset_t end_n,
                        const settings_t* settings)
  {
    return( end_n-start_n <= (offset_t) settings->SwitchPermute &&
            can_permute(ctx,min,max,start_n,end_n, settings) );
  }

 
  static void count_for_permute(key_t min, key_t max,
                                shift_t shift,
                                offset_t* restrict offsets, // can be NULL
                                minmax_t* restrict minmax,
                                int MaxNumBins)
  {
    // This could be done more cleverly but this'll get the
    // job done for now.
    if( EXTRA_CHECKS ) assert(num_bits(min) <= 8*sizeof(offset_t));
    if( EXTRA_CHECKS ) assert(num_bits(max) <= 8*sizeof(offset_t));

    {
      key_t empty = key_traits_t::get_zero();
      for( int i = 0; i < MaxNumBins; i++ ) {
        minmax[i].min = empty;
        minmax[i].max = empty;
      }
    }

    // Bin is calculated with bin = ((offset>>common_shift)-min)>>shift_amt
    // That is ((offset/common_div)-(min_offset/common_div))/shift_div
    // (offset-min_offset)/common_div /shift_div
    // (offset-min_offset)/(common_div*shift_div)
    // bin = diff/div
    // so then #per bin = div.

    offset_t one = 1;
    offset_t common_div = one << shift.common_shift;
    offset_t shift_div = one << shift.shift_amt;
    offset_t div = common_div * shift_div;
    offset_t bin_min, bin_max;
    int maxbin = bin_for_key(max, shift);
    assert( maxbin < MaxNumBins );

    offset_t cur = DU::right_shift(min,0);
    for( int bin = 0; bin < maxbin; bin++ ) {
      bin_min = cur;
      bin_max = cur + div - 1;
      set_key(minmax[bin].min, bin_min);
      set_key(minmax[bin].max, bin_max);
      if( offsets ) offsets[bin] = 1 + bin_max - bin_min;
      // Check that we computed this correctly.
      if( EXTRA_CHECKS ) {
        assert(bin == DU::bin_for_offset(bin_min, shift));
        assert(bin == DU::bin_for_offset(bin_max, shift));
      }
      cur += div;
    }

    // figure out maxbin.
    {
      int bin = maxbin;
      bin_min = cur;
      bin_max = DU::right_shift(max,0);
      set_key(minmax[bin].min, bin_min);
      set_key(minmax[bin].max, bin_max);
      if( offsets ) offsets[bin] = 1 + bin_max - bin_min;
      if( EXTRA_CHECKS ) {
        assert(bin == DU::bin_for_offset(bin_min, shift));
        assert(bin == DU::bin_for_offset(bin_max, shift));
      }
    }
  }

  static void count_for_permute(Criterion ctx,
                                key_t min, key_t max,
                                shift_t shift,
                                bins_t* restrict bins)
  {
    count_for_permute(min, max, shift, 
                      bins->offsets, bins->minmax, MaxBins);
  }
 
  /* Sorts in place. Note -- bins must be filled in with the
   * count for the data before this is called.
   **/
  static void sort_impl(Criterion ctx,
                        both_criterion_t bctx,
                        key_t min, key_t max,
                        /* in and out */ iterator_t data,
                        offset_t start_n,
                        offset_t end_n,
                        bool parallel,
                        const settings_t* settings)
  {
    if( PRINT_PROGRESS && end_n-start_n > PRINT_PROGRESS ) printf("sort_impl [%li,%li)\n", (long) start_n, (long) end_n);

    if( EXTRA_CHECKS ) {
      // Make sure that all data is within min/max.
      for( offset_t i = start_n; i < end_n; i++ ) {
        Record r = data[i];
        key_t key = ctx.get_key(r);
        assert(!less_than(key, min));
        assert(!less_than(max, key));
      }
    }

    if( end_n - start_n <= 1 ) {
      // No data? No problem.
      return;
    } else if(0 == DU::compare(min, max) ) {
      // Comparison sort if it was requested.
      if( settings->compareAfterKey ) {
        simple_sort(bctx, data + start_n, end_n - start_n);
      }
      return;
    } else if( will_permute(ctx, min, max, start_n, end_n, settings ) ) {
      permute_inplace(ctx, min, max, data, start_n, end_n);
      return;
    } else if( end_n-start_n <= (offset_t) settings->SortSwitch ) {
      simple_sort(bctx, data + start_n, end_n-start_n);
      return;
    } else {
      int firstbin, lastbin;
      shift_t shift;
      bins_t bins_storage;
      //std::auto_ptr<bins_t> auto_bins(new bins_t);
      //bins_t* bins = auto_bins.get();
      bins_t* bins = &bins_storage;

      bool go_parallel = parallel;

      // Clear out the bins.
      {
        //key_t empty = key_traits_t::get_zero();

        for(int i = 0; i < MaxBins; i++ ) {
          bins->offsets[i] = 0;
          //bins->minmax[i].min = empty;
          //bins->minmax[i].max = empty;
        }
      }

      // Compute the shift.
      shift = compute_shift(min, max);

      if(PRINT_TIMING && end_n-start_n > PRINT_TIMING) start_clock();

      // Count.
      count_ptrs(ctx, min, max, data, start_n, end_n, shift, bins, settings);

      if(PRINT_TIMING && end_n-start_n > PRINT_TIMING) {
        stop_clock();
        print_timings("count MB", (end_n-start_n)*record_size*1.0/(1024*1024));
      }

      {
        // Compute the offsets.
        offset_t end_offsets[MaxBins];
        offset_t sum = 0;
        offset_t num;

        /*
        for( int bin = 0; bin < MaxBins; bin++ ) {
          end_offsets[bin] = 0;
        }
        */
       
        // compute firstbin and lastbin.
        firstbin = 0;
        while( firstbin < MaxBins && bins->offsets[firstbin] == 0) {
          firstbin++;
        }
        // compute lastbin.
        lastbin = MaxBins - 1;
        while( (bins->offsets[lastbin] == 0) && lastbin > 0 ) {
          lastbin--;
        }
        lastbin++; // so it makes a good bound. ie. i = firstbin; i < lastbin

        assert(lastbin >= firstbin);

        for( int bin = firstbin; bin < lastbin; bin++ ) {
          num = bins->offsets[bin];
          bins->offsets[bin] = sum + start_n;
          sum += num;
          end_offsets[bin] = sum + start_n;
        }

        if(PRINT_TIMING && end_n-start_n > PRINT_TIMING) start_clock();

        // Distribute.
        distribute_inplace(ctx, data, shift, bins,
                           end_offsets, firstbin, lastbin);

        if(PRINT_TIMING && end_n-start_n > PRINT_TIMING) {
          stop_clock();
          print_timings("distribute MB", (end_n-start_n)*record_size*1.0/(1024*1024));
        }

        // OK, now data in out is sorted, according to the bins.
        // Note that distribute changed 'offset' in each bin
        // so that it should now be the same as end_offsets.
        // Here we set it back to point to bin starts.
        bins->offsets[firstbin] = start_n;
        for( int bin = firstbin+1; bin < lastbin; bin++ ) {
          bins->offsets[bin] = end_offsets[bin-1];
        }
        assert(end_offsets[lastbin-1] == end_n);
      }

      // OK, now data in out is sorted, according to the bins.

      go_parallel = settings->Parallel &&
                    parallel &&
                    end_n - start_n > OMP_LIMIT &&
                    lastbin - firstbin > OMP_LIMIT_BINS;

      // Go through each bin, recursively sorting anything in it.
      // Note that at this point, the bin offsets are ENDS of bins.
      #pragma omp parallel for schedule(dynamic,1) if (go_parallel)
      for( int bin = firstbin; bin < lastbin; bin++ ) {
        offset_t bin_start = bins->offsets[bin];
        offset_t bin_end = (bin+1<lastbin)?(bins->offsets[bin+1]):end_n;
        offset_t num = bin_end - bin_start;
        key_t bin_min = bins->minmax[bin].min;
        key_t bin_max = bins->minmax[bin].max;
        // Does the bin contain more than 1 record?
        // Are all the records in the bin identical?
        
        // do nothing if the bin has 1 record
        if( num <= 1 ) continue;
        // only comparison sort (if necessary) if the keys are the same
        if( 0 == DU::compare(bin_min, bin_max) ) {
          if( settings->compareAfterKey ) {
            simple_sort(bctx, data + bin_start, num);
          }
          continue;
        }
        // Otherwise, recurse
        // Now sort the recursive problem (serially).
        sort_impl(ctx, bctx, bin_min, bin_max,
                  data, bin_start, bin_end, parallel && !go_parallel, settings);
      }
    }

    if( EXTRA_CHECKS ) {
      offset_t cur = 0;
      Record last;
      check_sorted_ptrs_impl(ctx, data, start_n, end_n, cur, last, settings);
    }
  }

  static void sort( Criterion ctx,
                    key_t min, key_t max,
                    /* in and out */ iterator_t data,
                    offset_t start_n,
                    offset_t end_n,
                    const settings_t* settings )
  {
    after_criterion_t after;
    both_criterion_t both(ctx, after);
    BucketSorterSettings default_settings;
    sort_impl(ctx,both,min,max,data,start_n,end_n, true,
              (settings==NULL)?(&default_settings):(settings) );
  }

  static void sort_compare_after( Criterion ctx, CompareAfter actx,
                    key_t min, key_t max,
                    /* in and out */ iterator_t data,
                    offset_t start_n,
                    offset_t end_n,
                    bool parallel,
                    const settings_t* settings )
  {
    both_criterion_t both(ctx, actx);
    BucketSorterSettings use_settings;
    if( settings ) use_settings = *settings;
    use_settings.compareAfterKey = true;
    use_settings.Parallel = parallel;
    sort_impl(ctx,both,min,max,data,start_n,end_n, true,
              &use_settings);
  }


  static void sort_file_mmap_easy(Criterion ctx,
                              key_t min, key_t max,
                              int fd,
                              offset_t start_n,
                              offset_t end_n,
                              const settings_t* settings
                             )
  {
    if( start_n == end_n ) return;
    
    FileMMap memory;
    Pages pgs = get_pages_for_records(get_file_page_size(fd),start_n,end_n,record_size);
    memory.map( NULL, pgs.outer_length, PROT_READ|PROT_WRITE, MAP_SHARED, fd, pgs.outer_start);
    void* ptr = memory.data;
    ptr = PTR_ADD(ptr,pgs.start - pgs.outer_start);
    iterator_t data = get_iter(ptr);

    //int rc;
    //rc = madvise(memory.data, memory.len, MADV_SEQUENTIAL);
    //if( rc ) throw error(ERR_MEM_STR_NUM("madvise failed", errno));
    //error_t err;
    //err = advise_sequential_pages(memory.data, memory.len);
    //if( err ) throw error(err);
    start_clock();
    memory.advise_willneed();
    stop_clock();
    print_timings("willneed MB", pgs.outer_length*1.0/(1024*1024));

    // Now that the file is mmap'd, just sort the data
    // with the normal call and hope for the best.
    start_clock();
    sort(ctx,min,max, data, 0, end_n-start_n, settings);
    stop_clock();
    print_timings("sort MB", (end_n-start_n)*record_size*1.0/(1024*1024));

    memory.unmap();
  }




  /* This comment records info about a version
   * that was out-of-place.
   *
   * Sorts data using scratch space.
   * Input/output is always in data.
   *  
   *  NOTES - on an 8-core Xeon, we get, with initial algo,
   *          (parallel count, distribute, serial recurse)
   *          1GB   1 thread  - 14 s
   *                2 threads - 13 s
   *                8 threads - 13 s
   *          using fast serial sub-sorts
   *                2 threads - 6 s
   *                4 threads - 6 s
   *
   *          parallel count+distribute
   *          1GB 1 thread - 3s
   *          1GB 2 thread - 1.7s
   *          1GB 4 thread - 1.27s
   *          4GB 1 thread - 11.8s
   *          4GB 2 thread - 6.9s
   *          4GB 4 thread - 5.5
   *          serial count+distribute
   *          1GB 1 thread - 1.2s
   *          4GB 1 thread - 4.8s
   **/
};
 
template<typename Record, // Record type
         typename Criterion // Comparison criterion
         >
void sort_array(Criterion ctx, Record min, Record max, Record* data, size_t n_records )
{
  typedef BucketSorter<Record, Criterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  start_n = 0;
  end_n = n_records;

  MySorter::sort(ctx, ctx.get_key(min), ctx.get_key(max), data, start_n, end_n, NULL);
}

template<typename Record, // Record type
         typename Criterion // Comparison criterion
         >
void sort_array(Criterion ctx, typename Criterion::key_t min, typename Criterion::key_t max, Record* data, size_t n_records )
{
  typedef BucketSorter<Record, Criterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  start_n = 0;
  end_n = n_records;

  MySorter::sort(ctx, min, max, data, start_n, end_n, NULL);
}


template<typename Record, // Record type
         typename Criterion, // Comparison criterion
         typename AfterCriterion // Comparison criterion
         >
void sort_array_compare_after(Criterion ctx, AfterCriterion actx, typename Criterion::key_t min, typename Criterion::key_t max, Record* data, size_t n_records, bool parallel=false)
{
  typedef BucketSorter<Record, Criterion, AfterCriterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  start_n = 0;
  end_n = n_records;

  MySorter::sort_compare_after(ctx, actx, min, max, data, start_n, end_n, parallel, NULL);
}


template<typename Record, /* Record type */
         typename Criterion /* Comparison criterion */
         >
void sort_file(Criterion ctx, Record min, Record max, int fd, uint64_t num_records)
{
  typedef BucketSorter<Record, Criterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  // Check that there are that many records in the file at all!
  assert((uint64_t) file_len(fd) <= num_records * record_size);

  start_n = 0;
  end_n = num_records;

  if(PRINT_TIMING && num_records > PRINT_TIMING) start_clock();

  MySorter::sort_file_mmap_easy(ctx, ctx.get_key(min), ctx.get_key(max), fd, start_n, end_n, NULL);

  if(PRINT_TIMING && num_records > PRINT_TIMING) {
    stop_clock();
    print_timings("sort file MB", num_records*record_size*1.0/(1024*1024));
  }
}



template<typename Record, /* Record type */
         typename Criterion /* Comparison criterion */
         >
void sort_file(Criterion ctx, Record min, Record max, file_pipe_context& fctx, uint64_t num_records)
{
  fctx.read = true;
  fctx.write = true;
  fctx.create = false;
  fctx.open_file_if_needed();

  if(PRINT_PROGRESS && num_records > PRINT_PROGRESS) {
    printf("sorting file '%s' with %li %li-byte records\n",
           fctx.fname.c_str(), (long int) num_records, (long int) record_size);
  }

  sort_file(ctx, min, max, fctx.fd, num_records);

  fctx.close_file_if_needed();
}

template<typename Record, // Record type
         typename Criterion // Comparison criterion
         >
void permute_array(Criterion ctx, Record min, Record max, Record* data, size_t n_records)
{
  typedef BucketSorter<Record, Criterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  start_n = 0;
  end_n = n_records;

  BucketSorterSettings settings;
  settings.Permute = true;
  MySorter::sort(ctx, ctx.get_key(min), ctx.get_key(max), data, start_n, end_n, &settings);
}

template<typename Record, /* Record type */
         typename Criterion /* Comparison criterion */
         >
void permute_file(Criterion ctx, Record min, Record max, int fd, uint64_t num_records)
{
  typedef BucketSorter<Record, Criterion> MySorter;
  typename MySorter::offset_t start_n, end_n;

  // Check that there are that many records in the file at all!
  assert((uint64_t) file_len(fd) <= num_records * record_size);

  start_n = 0;
  end_n = num_records;

  if(PRINT_TIMING && num_records > PRINT_TIMING) start_clock();

  BucketSorterSettings settings;
  settings.Permute = true;
  MySorter::sort_file_mmap_easy(ctx, ctx.get_key(min), ctx.get_key(max), fd, start_n, end_n, &settings);

  if(PRINT_TIMING && num_records > PRINT_TIMING) {
    stop_clock();
    print_timings("permute file MB", num_records*record_size*1.0/(1024*1024));
  }
}


template<typename Record, /* Record type */
         typename Criterion /* Comparison criterion */
         >
void permute_file(Criterion ctx, Record min, Record max, file_pipe_context& fctx, uint64_t num_records)
{
  fctx.read = true;
  fctx.write = true;
  fctx.create = false;
  fctx.open_file_if_needed();

  if(PRINT_PROGRESS && num_records > PRINT_PROGRESS) {
    printf("permuting file '%s' with %li %li-byte records\n",
           fctx.fname.c_str(), (long int) num_records, (long int) record_size);
  }

  permute_file(ctx, min, max, fctx.fd, num_records);

  fctx.close_file_if_needed();
}


#undef record_size
#undef iterator_t
#undef get_iter
#undef get_ptr
#undef prefetch

#endif
