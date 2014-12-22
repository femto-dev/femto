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

  femto/src/utils_cc/compare_record.hh
*/
#ifndef _COMPARE_RECORD_HH_
#define _COMPARE_RECORD_HH_

#include <cstddef>
#include <string>
#include <sstream>

extern "C" {
  #include <stdint.h>
  // get ceildiv
  #include "bit_funcs.h"
}

#include "criterion.hh"
#include "pipelining.hh"
#include "varint.hh"

// forward declare KeyTraits template.
template<typename KeyType>
struct KeyTraits;

template<typename NumType>
static inline NumType safe_shift_left(NumType n, size_t shift)
{
  if( shift >= 8*sizeof(NumType) ) return 0;
  else return n << shift;
}

template<typename NumType,
         typename KeyType>
static inline NumType right_shift_key_parts(const KeyType & k, size_t shift_amt) {
  typedef KeyTraits<KeyType> key_traits_t;
  ssize_t sz = key_traits_t::get_num_key_parts(k);
  typedef typename key_traits_t::key_part_t key_part_t;
  static const ssize_t key_part_bits = 8*sizeof(key_part_t);
  static const ssize_t ret_bits = 8*sizeof(NumType);
  if( EXTRA_CHECKS ) {
    // check that key_part_t and NumType are both unsigned.
    key_part_t part = -1;
    assert( part > 0 ); // should be unsigned!
    NumType num = -1;
    assert( num > 0 ); // should be unsigned!
    // check that we won't run out of room in our bits.
    ssize_t s = sz*key_part_bits;
    assert( s >= 0 ); // no overflow!
  }
  // start_bit_from_right is inclusive.
  ssize_t start_bit_from_right = shift_amt;
  ssize_t end_bit_from_right = shift_amt + ret_bits;
  ssize_t start_part_from_right = start_bit_from_right/key_part_bits;
  ssize_t end_part_from_right = end_bit_from_right/key_part_bits;
  if( end_part_from_right >= sz ) end_part_from_right = sz - 1;
  ssize_t start_part_shift = start_bit_from_right - key_part_bits*start_part_from_right;
  //ssize_t end_part_bits = end_bit_from_right - key_part_bits*start_part_from_right;

  NumType ret = 0;
  key_part_t part;

  for( ssize_t i = end_part_from_right;
       i > start_part_from_right;
       i-- ) {
    ssize_t part_idx = sz - 1 - i;
    // entire part
    // (note -- end_part_from right will just
    //  have some top bits that 'fall off'.)
    part = key_traits_t::get_key_part(k, part_idx);
    //ret <<= key_part_bits;
    ret = safe_shift_left(ret, key_part_bits);
    ret |= part;
  }

  // handle start_part_from_right.
  {
    ssize_t part_idx = sz - 1 - start_part_from_right;
    // rightmost partial part.
    part = key_traits_t::get_key_part(k, part_idx);
    // take only the top bits...
    part >>= start_part_shift;
    //ret <<= key_part_bits - start_part_shift;
    ret = safe_shift_left(ret, key_part_bits - start_part_shift);
    ret |= part;
  }

  return ret;
}

#define DECLARE_NUMBER_SHIFTER(KEY_TYPE) \
template<> \
struct KeyTraits<KEY_TYPE> { \
  static KEY_TYPE get_zero() \
  { \
    return 0; \
  } \
  template<typename NumType> \
  static void right_shift(const KEY_TYPE& k, size_t shift_amt, NumType& ret ) \
  { \
    ret = k >> shift_amt; \
  } \
  static size_t num_bits(const KEY_TYPE& k) \
  { \
    return sizeof(KEY_TYPE)*8; \
  } \
  static int compare(const KEY_TYPE& a, const KEY_TYPE& b) { \
    if( a < b ) return -1; \
    else if( a > b ) return 1; \
    else return 0; \
  } \
  template<typename NumType> \
  static void set(KEY_TYPE & a, NumType num) { \
    a = num; \
  } \
};

DECLARE_NUMBER_SHIFTER(int8_t);
DECLARE_NUMBER_SHIFTER(uint8_t);
DECLARE_NUMBER_SHIFTER(int16_t);
DECLARE_NUMBER_SHIFTER(uint16_t);
DECLARE_NUMBER_SHIFTER(int32_t);
DECLARE_NUMBER_SHIFTER(uint32_t);
DECLARE_NUMBER_SHIFTER(int64_t);
DECLARE_NUMBER_SHIFTER(uint64_t);

#undef DECLARE_NUMBER_SHIFTER

// Key type for std::string
template<typename CharT, typename Traits, typename Alloc>
struct KeyTraits<std::basic_string<CharT, Traits, Alloc> > {
  typedef std::basic_string<CharT, Traits, Alloc> mystring;
  typedef typename min_word_getter<sizeof(CharT)>::register_type key_part_t;
  static key_part_t get_key_part(const mystring & k, ssize_t i) {
    return k[i];
  }
  static size_t get_num_key_parts(const mystring & k) {
    return k.size();
  }
  static mystring get_zero() {
    mystring empty;
    return empty;
  }
  template<typename NumType>
  static void right_shift(const mystring & k, size_t shift_amt, NumType& ret)
  {
    ret = right_shift_key_parts<NumType,mystring>(k, shift_amt);
  }
  static size_t num_bits(const mystring & k)
  {
    size_t sz = get_num_key_parts(k);
    static const size_t key_part_bits = 8*sizeof(key_part_t);
    return sz*key_part_bits;
  }
  static int compare(const mystring & a, const mystring & b)
  {
    return a.compare(b);
  }
  template<typename NumType>
  static void set(mystring& a, NumType num) {
    assert(0);
  }
};

template<typename KeyType>
struct KeyTraits {
  typedef typename KeyType::key_part_t key_part_t;
  static key_part_t get_key_part(const KeyType & k, ssize_t i) {
    return k.get_key_part(i);
  }
  static size_t get_num_key_parts(const KeyType & k) {
    return k.get_num_key_parts();
  }
  static KeyType get_zero()
  {
    KeyType empty;
    return empty;
  }
  template<typename NumType>
  static void right_shift(const KeyType & k, size_t shift_amt, NumType& ret)
  {
    ret = right_shift_key_parts<NumType,KeyType>(k, shift_amt);
  }
  static size_t num_bits(const KeyType & k)
  {
    size_t sz = get_num_key_parts(k);
    static const size_t key_part_bits = 8*sizeof(key_part_t);
    return sz*key_part_bits;
  }
  static int compare(const KeyType & a, const KeyType & b)
  {
    size_t a_length=get_num_key_parts(a);
    size_t b_length=get_num_key_parts(b);

    // Compare them one long at a time.
    for(size_t i=0;i<a_length && i<b_length;i++) {
      key_part_t a_part;
      key_part_t b_part;
      a_part = get_key_part(a,i);
      b_part = get_key_part(b,i);

      if( a_part < b_part ) return -1;
      else if( a_part > b_part ) return 1;
    }

    return 0;
  }
  template<typename NumType>
  static void set(KeyType& a, NumType num) {
    assert(0);
  }
};

/* Given two keys, compute the number of bits they have in common.
 */
template<typename NumType,
         typename KeyType>
static size_t num_bits_in_common_impl(const KeyType &a, const KeyType &b)
{
  typedef KeyTraits<KeyType> key_traits_t;
  size_t a_num_bits = key_traits_t::num_bits(a);
  size_t b_num_bits = key_traits_t::num_bits(b);
  ssize_t num_bits;
  ssize_t chunk_bits = 8*sizeof(NumType);
  size_t ret_common;

  // We don't support variable length records!
  num_bits = std::min(a_num_bits, b_num_bits);
  if( EXTRA_CHECKS ) assert( num_bits >= 0 ); // no overflow!

  {
    ssize_t top;
    ssize_t n_common;
    ssize_t last_common;
    NumType a_bits, b_bits;

    n_common = 0;
    for( top = num_bits;
         top > 0;
         top -= chunk_bits) {
      ssize_t shift;
      ssize_t amt;
      if( top > chunk_bits ) {
        amt = chunk_bits;
        shift = top - amt;
      } else {
        shift = 0;
        amt = top;
      }
      key_traits_t::right_shift(a, shift, a_bits);
      key_traits_t::right_shift(b, shift, b_bits);
      if( a_bits == b_bits ) {
        // OK, they are the same.
        n_common += amt;
      } else {
        last_common = leadz64(a_bits ^ b_bits);
        // We maybe didn't need to do leadz64...
        last_common -= 64 - 8*sizeof(NumType);
        if( top <= chunk_bits ) {
          // we know that there are
          // chunk_bits - top bits in common.
          // (that we checked before).
          last_common -= chunk_bits - top;
        }
        n_common += last_common;
        break;
      }
    }

    ret_common = n_common;
  }

  if( EXTRA_CHECKS ) {
    ssize_t shift;
    size_t n_common;
    NumType a_bits, b_bits, a_bit, b_bit;

    // Do it the easy way.
    n_common = 0;
    for( shift = num_bits-1;
         shift >= 0;
         shift-- )
    {
      key_traits_t::right_shift(a, shift, a_bits);
      key_traits_t::right_shift(b, shift, b_bits);
      a_bit = a_bits & 1;
      b_bit = b_bits & 1;
      if( a_bit == b_bit ) n_common++;
      else break;
    }
    assert(ret_common == n_common);
  }

  return ret_common;
}



template<typename Record,
         typename Criterion>
struct CompareRecord {
  static int compare_record_impl(Criterion crit,
                                 const Record& a,
                                 const Record& b,
                                 return_key_criterion_tag)
  {
    typedef typename Criterion::key_t key_t;
    return KeyTraits<key_t>::compare(crit.get_key(a), crit.get_key(b));
  }

  static int compare_record_impl(Criterion crit,
                                 const Record& a,
                                 const Record& b,
                                 compare_criterion_tag)
  {
    return crit.compare(a,b);
  }

  /*
  static int compare_record_impl(Criterion crit,
                                 const Record& a,
                                 const Record& b,
                                 return_key_and_compare_criterion_tag)
  {
    int ret = compare_record_impl(crit, a, b, return_key_criterion_tag());
    if( ret != 0 ) return ret;
    else return crit.compare(a,b);
  }
  */

  static int compare(Criterion crit,
                     const Record& a,
                     const Record& b)
  {
    return compare_record_impl(crit, a, b, typename Criterion::criterion_category());
  }
};


template<typename Key>
struct KeySortingCriterion {
  template<typename Criterion>
  explicit
  KeySortingCriterion(Criterion ctx)
  {
  }

  static int compare(const Key& a, const Key& b)
  {
    return KeyTraits<Key>::compare(a, b);
  }

  bool operator()(const Key& a, const Key& b) const
  {
    return compare(a,b) < 0;
  }
};

// "Less than" for records, as a function, keeping the context as state
// This could possibly cache the result of compare(a,b) when
// answering less(a,b) in case less(b,a) is called next.
template<typename Record,
         typename Criterion>
struct RecordSortingCriterion {
    Criterion ctx;
    // Constructor saves the context.
    explicit RecordSortingCriterion(Criterion ctx)
      : ctx(ctx)
    {
    }

    // Compare two records with the appropriate context.
    int compare(const Record& a,
                const Record& b) const
    {
      return CompareRecord<Record,Criterion>::compare(ctx, a, b);
    }

    bool equals(const Record& a, const Record& b) const
    {
      return compare(a,b) == 0;
    }

    bool less(const Record& a, const Record& b) const
    {
      return compare(a,b) < 0;
    }

    // Operator is less-than to be used as STL comparison function.
    bool operator()(const Record& a,
                    const Record& b) const
    {
      return compare(a, b) < 0;
    }
};

template<typename Record,
         typename Criterion,
         typename SecondCriterion>
struct DoubleRecordSortingCriterion {
    Criterion ctx;
    SecondCriterion actx;
    // Constructor saves the context.
    explicit DoubleRecordSortingCriterion(Criterion ctx, SecondCriterion actx)
      : ctx(ctx), actx(actx)
    {
    }

    // Compare two records with the appropriate context.
    int compare(const Record& a,
                const Record& b) const
    {
      int cmp = CompareRecord<Record,Criterion>::compare(ctx, a, b);
      if( cmp == 0 ) cmp =  CompareRecord<Record,SecondCriterion>::compare(actx, a, b);
      return cmp;
    }

    bool equals(const Record& a, const Record& b) const
    {
      return compare(a,b) == 0;
    }

    bool less(const Record& a, const Record& b) const
    {
      return compare(a,b) < 0;
    }

    // Operator is less-than to be used as STL comparison function.
    bool operator()(const Record& a,
                    const Record& b) const
    {
      return compare(a, b) < 0;
    }
};



template <typename Tag, typename Record, typename Criterion>
struct KeyCritTraitsReally {
  // Error!
};

template <typename Record, typename Criterion>
struct KeyCritTraitsReally<compare_criterion_tag, Record, Criterion> {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef Record key_t;
  typedef RecordSortingCriterion<Record,Criterion> ComparisonCriterion;
  static key_t get_key(Criterion crit, const Record & r) {
    return r;
  }
  // This is really here to make compiler errors faster.
  static int compare(Criterion crit, const key_t & a, const key_t & b) {
    ComparisonCriterion comp(crit);
    return comp.compare(a,b);
  }
};

template <typename Record, typename Criterion>
struct KeyCritTraitsReally<return_key_criterion_tag, Record, Criterion> {
  typedef Record record_t;
  typedef Criterion criterion_t;
  typedef typename Criterion::key_t key_t;
  typedef KeySortingCriterion<key_t> ComparisonCriterion;
  static key_t get_key(Criterion crit, const Record & r) {
    return crit.get_key(r);
  }
  // This is really here to make compiler errors faster.
  static int compare(Criterion crit, const key_t & a, const key_t & b) {
    ComparisonCriterion comp(crit);
    return comp.compare(a,b);
  }

};

template<typename Record, typename Criterion>
struct KeyCritTraits : KeyCritTraitsReally<typename Criterion::criterion_category, Record, Criterion>
{
};



#endif
