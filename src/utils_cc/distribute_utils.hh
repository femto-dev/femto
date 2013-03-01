/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/distribute_utils.hh
*/
#ifndef _DISTRIBUTE_UTILS_HH_
#define _DISTRIBUTE_UTILS_HH_

extern "C" {
  #include "bit_funcs.h" // log2lli
}

#include <climits>
#include <cstdlib>

#include "compare_record.hh"

template<typename Record,
         typename Criterion,
         typename Size = uint64_t >
struct DistributeUtils {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef Size offset_t;

  struct Shift {
    size_t common_shift; // how many bits to shift to past common stuff.
    offset_t min; // minimum common_shifted value the part can have.
    size_t shift_amt; // how many bits to shift after subtracting.
    ssize_t MaxBins;
  };
  typedef Shift shift_t;
  typedef typename criterion_t::key_t key_t;
  typedef KeyTraits<key_t> key_traits_t;

  static int compare(const key_t & a, const key_t & b)
  {
    return key_traits_t::compare(a, b);
  }

  static bool less_than(const key_t & a, const key_t & b)
  {
    return compare(a,b) < 0;
  }
  static bool less_than(Criterion ctx, const Record & a, const Record & b)
  {
    return less_than(ctx.get_key(a),ctx.get_key(b));
  }
  static int compare(Criterion ctx, const Record & a, const Record & b)
  {
    return compare(ctx.get_key(a),ctx.get_key(b));
  }

  static const size_t chunk_bits = 8*sizeof(offset_t);
  static offset_t right_shift(const key_t & k, size_t shift_amt)
  {
    offset_t ret;
    key_traits_t::right_shift(k, shift_amt, ret);
    return ret;
  }
  static size_t num_bits(key_t k)
  {
    return key_traits_t::num_bits(k);
  }

  static size_t num_bits_in_common(key_t min, key_t max)
  {
    return num_bits_in_common_impl<offset_t,key_t>(min, max);
  }
  
  /* Computes the number of ranks per bin and converts it to a shift. */
  static shift_t compute_shift(key_t min, key_t max, ssize_t MaxBins)
  {
    shift_t ret;
    size_t a_num_bits = num_bits(min);
    size_t b_num_bits = num_bits(max);
    if( EXTRA_CHECKS ) assert(a_num_bits == b_num_bits );
    if( EXTRA_CHECKS ) {
      // check that offset_t is unsigned.
      offset_t n = -1;
      assert(n>0);
    }
    size_t num_bits = a_num_bits; // not supporting variable length records!
    size_t num_common_bits = num_bits_in_common(min, max);
    size_t num_differ = num_bits - num_common_bits;
    size_t common_shift;
    if( num_differ > chunk_bits ) common_shift = num_differ - chunk_bits;
    else common_shift = 0;

    offset_t min_bits = right_shift(min, common_shift);
    offset_t max_bits = right_shift(max, common_shift);

    // We would do ranks_per_bin = ceildiv(1+max-min,MaxBins))
    // or          ranks_per_bin = ceildiv(max-min,MaxBins+1))
    // but we also want to round to the nearest power of 2,
    // so that we can use shifting to get to the key elements.
    // We add one so that we are computing the number per bin
    // of the value above max_part that is never attained
    // since we need max_part to fit into a bin < MaxBins.
    uint64_t one = 1;
    uint64_t min_u = min_bits;
    uint64_t max_u = max_bits;
    uint64_t diff = max_u - min_u;


    uint64_t max_bins_u = MaxBins;

    int shift_amt;

    shift_amt = 0;
    while( (diff >> shift_amt) >= max_bins_u ) {
      shift_amt++;
    }

    // check that we're not using more than MaxBins bins.
    // check that maximum will be mapped to < MaxBins.
    assert( (max_bits-min_bits)/(one<<shift_amt) < (offset_t) MaxBins );
    assert( ((max_bits-min_bits) >> shift_amt) < (offset_t) MaxBins );
    // It should not be the case that if we half the 
    // num_per_bin (ie.. subtract one from shift_amt)
    // we get a suitable function.
    assert( (shift_amt == 0) || (max_bits-min_bits) < (offset_t) MaxBins || ((max_bits-min_bits) >> (shift_amt-1)) >= (offset_t) MaxBins );

    // Set up the return values!
    // Now include the key parts in common in the shift amount.
    ret.common_shift = common_shift;
    ret.min = min_bits;
    ret.shift_amt = shift_amt;
    ret.MaxBins = MaxBins;


    assert(bin_for_key(min,ret) == 0);
    assert(bin_for_key(max,ret) < MaxBins );

    return ret;
  }

  static ssize_t bin_for_key(const key_t & key, shift_t shift)
  {
    ssize_t ret;

    //ret = (part-shift.min_part) >> shift.shift_amt;
    ret = (right_shift(key, shift.common_shift) - shift.min) >> shift.shift_amt;

    if(EXTRA_CHECKS) {
      assert(0 <= ret);
      assert(ret < shift.MaxBins);
    }

    return ret;
  }

  // Note -- only usable in permutation code!
  static ssize_t bin_for_offset(offset_t offset, shift_t shift)
  {
    ssize_t ret;

    ret = ((offset >> shift.common_shift) - shift.min) >> shift.shift_amt;

    if(EXTRA_CHECKS) {
      assert(0 <= ret);
      assert(ret < shift.MaxBins);
    }

    return ret;
  }

  static ssize_t bin_for_record(Criterion ctx, const Record& r, shift_t shift)
  {
    // figure out part of interest
    return bin_for_key(ctx.get_key(r), shift);
  }
};

#endif

