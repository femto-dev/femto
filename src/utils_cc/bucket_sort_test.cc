/*
  (*) 2009-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/bucket_sort_test.cc
*/
#include <cstdlib>
#include <cassert>

#include <algorithm>

#include "sort_tests.hh"
#include "bucket_sort.hh"

#define NUM_BINS MAXBINS_DEFAULT

// num bits for packed record tests.
#define NUM_BITS 17

#define VERBOSE 0
#define PROGRESS 0
#define EXTRATESTS 0

#define TMP_FILE "/tmp/bucket_sort_test"
//#define TMP_FILE "/pvfs2-storage-space/bucket_sort_test"

void progress(const char* s)
{
  if( PROGRESS ) {
    printf("%s", s);
    fflush(stdout);
  }
}

template<int ExtraBytes>
void test_permute(size_t num, BucketSorterSettings in_settings)
{
  if( VERBOSE ) printf("test permute extrabytes=%i num=%i\n", ExtraBytes, (int) num);
  else progress("p");

  BucketSorterSettings settings;
  settings = in_settings;
  settings.Permute = true;

  {
    typedef BucketSorter<Record<ExtraBytes>,Criterion<ExtraBytes>,size_t,NUM_BINS> EzBucketSorter;


    std::vector<Record<ExtraBytes> > arr;
    typename Criterion<ExtraBytes>::key_t min_k;
    typename Criterion<ExtraBytes>::key_t max_k;
    Record<ExtraBytes> min;
    Record<ExtraBytes> max;
    size_t i;

    Criterion<ExtraBytes> ctx;


    arr.resize(num);

    min_k = 0;
    max_k = num-1;
    min.key = min_k;
    max.key = max_k;
    // Test permutation of in-order.
    for( i = 0; i < num; i++ ) {
      arr[i].key = i;
      arr[i].v = i;
    }

    //permute_array(ctx,min,max,&arr[0],arr.size());
    EzBucketSorter::sort(ctx,min.key,max.key,&arr[0],0,arr.size(),&settings);

    for( i = 0; i < num; i++ ) {
      assert(arr[i].v == i);
    }

    // Test reverse order.
    for( i = 0; i < num; i++ ) {
      arr[i].key = num-1-i;
      arr[i].v = num-1-i;
    }

    //permute_array(ctx,min,max,&arr[0],arr.size());
    EzBucketSorter::sort(ctx,min.key,max.key,&arr[0],0,arr.size(),&settings);

    for( i = 0; i < num; i++ ) {
      assert(arr[i].v == i);
    }

    // Test reverse order in part of an array.
    {
      std::vector<Record<ExtraBytes> > bigarr;
      bigarr.resize(2*num);

      for( i = 0; i < num; i++ ) {
        bigarr[num+i].key = num-1-i;
        bigarr[num+i].v = num-1-i;
      }

      //permute_array(ctx,min,max,&bigarr[0],bigarr.size());
      EzBucketSorter::sort(ctx,min.key,max.key,&bigarr[0],num,2*num,&settings);

      for( i = 0; i < num; i++ ) {
        assert(bigarr[num+i].v == i);
      }
    }

    // Test random order.
    for( i = 0; i < num; i++ ) {
      arr[i].key = i;
      arr[i].v = i;
    }

    std::random_shuffle(arr.begin(), arr.end());

    //permute_array(ctx,min,max,&arr[0],arr.size());
    EzBucketSorter::sort(ctx,min.key,max.key,&arr[0],0,arr.size(),&settings);

    for( i = 0; i < num; i++ ) {
      assert(arr[i].v == i);
    }
  }

  if ( num < ( 1 << NUM_BITS) ) {
    // Try it with packed records.
    typedef PackedRecord<NUM_BITS> record_t;
    typedef RecordTraits<record_t> record_traits_t;
    typedef BucketSorter<record_t,PackedRecordCriterion<NUM_BITS>,size_t,NUM_BINS> EzBucketSorter;

    std::vector<char> arr;
    PackedRecordCriterion<NUM_BITS>::key_t min_k;
    PackedRecordCriterion<NUM_BITS>::key_t max_k;
    record_t min;
    record_t max;
    size_t i;
    size_t record_size = record_traits_t::record_size;
    std::vector<size_t> keys;

    PackedRecordCriterion<NUM_BITS> ctx;


    keys.resize(num);
    for( i = 0; i < num; i++ ) {
      keys[i] = i;
    }
    std::random_shuffle(keys.begin(), keys.end());

    arr.resize(num*record_size);

    min_k = 0;
    max_k = num-1;
    min.key = min_k;
    max.key = max_k;
    // Test permutation of random order.
    for( i = 0; i < num; i++ ) {
      record_t r;
      r.key = keys[i];
      r.v = keys[i];
      record_traits_t::encode(r, &arr[0] + i*record_size);
      record_t tmp;
      record_traits_t::decode(tmp, &arr[0] + i*record_size);
      assert(tmp.key == r.key);
      assert(tmp.v == r.v);
    }

    record_traits_t::iterator_t iter;
    iter = record_traits_t::getiter(&arr[0]);

    //permute_array(ctx,min,max,&arr[0],arr.size());
    EzBucketSorter::sort(ctx,min.key,max.key,iter,0,num,&settings);

    for( i = 0; i < num; i++ ) {
      record_t r;
      record_traits_t::decode(r, &arr[0] + i*record_size);
      assert(r.key == i);
    }
  }
}

template<int ExtraBytes, int MaxBins>
void test_bucket_sort_simple_impl(size_t num, BucketSorterSettings in_settings)
{
  typedef BucketSorter<Record<ExtraBytes>,Criterion<ExtraBytes>,size_t,MaxBins> EzBucketSorter;
  typedef BucketSorter<Record<ExtraBytes>,PartCriterion<ExtraBytes>,size_t,MaxBins> EzPartBucketSorter;
  Criterion<ExtraBytes> ctx;
  PartCriterion<ExtraBytes> part_ctx;
  std::vector<Record<ExtraBytes> > arr;
  typename Criterion<ExtraBytes>::key_t min;
  typename Criterion<ExtraBytes>::key_t max;
  typedef typename PartCriterion<ExtraBytes>::key_t part_key_t;
  typename PartCriterion<ExtraBytes>::key_t p_min;
  typename PartCriterion<ExtraBytes>::key_t p_max;
  typename EzBucketSorter::shift_t shift;
  size_t i;

  BucketSorterSettings settings = in_settings;
  settings.Permute = false;

  if( VERBOSE ) printf("test simple Parallel=%i swapdepth=%i extrabytes=%i MaxBins=%i num=%i\n", settings.Parallel, settings.SortSwitch, ExtraBytes, MaxBins, (int) num);
  else progress("s");

  arr.resize(num);

  for( i = 0; i < num; i++ ) {
    arr[i].key = i;
    arr[i].v = i;
  }

  EzBucketSorter::compute_min_max(ctx, min, max, num, &arr[0]);
  assert(min == 0);
  assert(max == num - 1);

  if( num == 6 ) assert(EzBucketSorter::num_bits_in_common( min, max ) == 61 );

  EzPartBucketSorter::compute_min_max(part_ctx, p_min, p_max, num, &arr[0]);
  assert(p_min.key == 0);
  assert(p_max.key == num - 1);

  if( num == 6 ) {
    unsigned long ret = 0;
    KeyTraits<part_key_t>::right_shift(p_max, 0, ret);
    assert(ret == num - 1);
    KeyTraits<part_key_t>::right_shift(p_max, 1, ret);
    assert(ret == ((num - 1) >> 1));
    KeyTraits<part_key_t>::right_shift(p_max, 2, ret);
    assert(ret == ((num - 1) >> 2));
    assert(EzPartBucketSorter::num_bits_in_common( p_min, p_max ) == 125 );
  }

  shift = EzBucketSorter::compute_shift(min, max);
  assert(shift.min == 0);
  if( MaxBins==256 && num <= 11 ) assert(shift.shift_amt == 0);

  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == i);
  }
  for( i = 0; i < num; i++ ) {
    arr[i].key = i;
    arr[i].v = i;
  }
  EzPartBucketSorter::sort(part_ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == i);
  }
 
  // try with reverse sorting.
  for( i = 0; i < num; i++ ) {
    arr[i].key = num-1-i;
    arr[i].v = num-1-i;
  }
  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == i);
  }

  // try with reverse sorting.
  for( i = 0; i < num; i++ ) {
    arr[i].key = num-1-i;
    arr[i].v = num-1-i;
  }
  EzPartBucketSorter::sort(part_ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == i);
  }
  
  for( i = 0; i < num; i++ ) {
    arr[i].key = 1000000 + 256*(num-1-i);
    arr[i].v = num-1-i;
  }
  EzBucketSorter::compute_min_max(ctx, min, max, num, &arr[0]);
  assert(min == 1000000);
  assert(max == 1000000 + (256*(num-1)));

  shift = EzBucketSorter::compute_shift(min, max);
  assert(shift.common_shift == 0);
  assert(shift.min == 1000000);
  if( MaxBins==256 && num == 11 ) assert(shift.shift_amt == 4);

  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == i);
  }

  // Try all ones.
  for( i = 0; i < num; i++ ) {
    arr[i].key = 1;
    arr[i].v = 1;
  }

  EzBucketSorter::compute_min_max(ctx, min, max, num, &arr[0]);
  assert(min == 1);
  assert(max == 1);

  assert(EzBucketSorter::num_bits_in_common( min, max ) == 64 );

  shift = EzBucketSorter::compute_shift(min, max);
  //assert(shift.common_shift == 8*sizeof(unsigned long));
  assert(shift.min == 1);
  assert(shift.shift_amt == 0);

  // we'll force the implementation to count again.
  min = 0;
  max = -1L;

  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == 1);
  }
  // Try all ones.
  for( i = 0; i < num; i++ ) {
    arr[i].key = 1;
    arr[i].v = 1;
  }
  EzPartBucketSorter::compute_min_max(part_ctx, p_min, p_max, num, &arr[0]);
  assert(p_min.key == 1);
  assert(p_max.key == 1);

  assert(EzPartBucketSorter::num_bits_in_common( p_min, p_max ) == 128 );

  EzPartBucketSorter::sort(part_ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num; i++ ) {
    assert(arr[i].v == 1);
  }

  // Try n/2 same and then a run.
  for( i = num/2; i < num; i++ ) {
    arr[i].key = 1;
    arr[i].v = 1;
  }
  for( i = num/2; i < num; i++ ) {
    arr[i].key = num + i;
    arr[i].v = num + i;
  }
  // reverse the order of the elements.
  std::reverse(arr.begin(), arr.end());

  // we'll force the implementation to count again.
  min = 0;
  max = -1L;

  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num/2; i++ ) {
    assert(arr[i].key == 1);
    assert(arr[i].v == 1);
  }
  for( i = num/2; i < num; i++ ) {
    assert(arr[i].key == num + i);
    assert(arr[i].v == num + i);
  }

  // Test reverse order in part of an array.
  {
    std::vector<Record<ExtraBytes> > bigarr;
    bigarr.resize(2*num);

    for( i = 0; i < num; i++ ) {
      bigarr[num+i].key = num-1-i;
      bigarr[num+i].v = num-1-i;
    }

    min = 0;
    max = -1L;
    //permute_array(ctx,min,max,&bigarr[0],bigarr.size());
    EzBucketSorter::sort(ctx,min,max,&bigarr[0],num,2*num,&settings);

    for( i = 0; i < num; i++ ) {
      assert(bigarr[num+i].v == i);
    }
  }

 
  // Try again with 2-parts.
  // Try n/2 same and then a run.
  for( i = num/2; i < num; i++ ) {
    arr[i].key = 1;
    arr[i].v = 1;
  }
  for( i = num/2; i < num; i++ ) {
    arr[i].key = num + i;
    arr[i].v = num + i;
  }
  // reverse the order of the elements.
  std::reverse(arr.begin(), arr.end());

  // we'll force the implementation to count again.
  min = 0;
  max = -1L;

  EzPartBucketSorter::sort(part_ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num/2; i++ ) {
    assert(arr[i].key == 1);
    assert(arr[i].v == 1);
  }
  for( i = num/2; i < num; i++ ) {
    assert(arr[i].key == num + i);
    assert(arr[i].v == num + i);
  }

  // Try again with random shuffle.
  // Try n/2 same and then a run.
  for( i = num/2; i < num; i++ ) {
    arr[i].key = 1;
    arr[i].v = 1;
  }
  for( i = num/2; i < num; i++ ) {
    arr[i].key = num + i;
    arr[i].v = num + i;
  }
  // Randomly shuffle the order of the elements.
  std::random_shuffle(arr.begin(), arr.end());

  // we'll force the implementation to count again.
  min = 0;
  max = -1L;

  EzBucketSorter::sort(ctx,min,max,&arr[0],0,num,&settings);
  // check that we sorted correctly.
  for( i = 0; i < num/2; i++ ) {
    assert(arr[i].key == 1);
    assert(arr[i].v == 1);
  }
  for( i = num/2; i < num; i++ ) {
    assert(arr[i].key == num + i);
    assert(arr[i].v == num + i);
  }

}

template <int ExtraBytes, int MaxBins>
void test_bucket_sort_random_impl(size_t num, BucketSorterSettings in_settings)
{
  typedef BucketSorter<Record<ExtraBytes>,Criterion<ExtraBytes>,size_t,MaxBins> EzBucketSorter;
  Criterion<ExtraBytes> ctx;
  RecordSortingCriterion<Record<ExtraBytes> ,Criterion<ExtraBytes> > criterion(ctx);
  std::vector<Record<ExtraBytes> > values;
  BucketSorterSettings settings = in_settings;
  settings.Permute = false;

  if( VERBOSE ) printf("test random Parallel=%i swapdepth=%i extrabytes=%i MaxBins=%i num=%i\n", settings.Parallel, settings.SortSwitch, ExtraBytes, MaxBins, (int) num);
  else progress("r");

  values.reserve(num);
  for( size_t i = 0; i < num; i++ ) {
    Record<ExtraBytes> x;
    x.randomize();
    x.v = x.key;
    values.push_back(x);
  }
  std::vector<Record<ExtraBytes> > copy(values);

  typename Criterion<ExtraBytes>::key_t  min;
  typename Criterion<ExtraBytes>::key_t  max;
  typename Criterion<ExtraBytes>::key_t  gotmin;
  typename Criterion<ExtraBytes>::key_t  gotmax;
  min = 0;
  max = -1;

  EzBucketSorter::compute_min_max(ctx, gotmin, gotmax, values.size(), &values[0]);
  assert(gotmin >= min);
  assert(gotmax <= max);

  // sort the random data with bucket sort.
  EzBucketSorter::sort(ctx,min,max,&values[0],0,values.size(),&settings);

  // sort the random data with standard sort.
  std::sort(copy.begin(), copy.end(), criterion);

  // check that the data sorted the same.
  for( size_t i = 0; i < values.size(); i++ ) {
    assert(values[i].key == copy[i].key);
    assert(values[i].v == copy[i].v);
  }

  // Try randomly shuffling the data and then check it again.
  std::random_shuffle(values.begin(), values.end());

  // sort the reshuffled random data with bucket sort.
  EzBucketSorter::sort(ctx,min,max,&values[0],0,values.size(),&settings);

  // check that the data sorted the same.
  for( size_t i = 0; i < values.size(); i++ ) {
    assert(values[i].key == copy[i].key);
    assert(values[i].v == copy[i].v);
  }
}

#define MAX (10*1024*1024)
void test_bucket_sort_mem(size_t num)
{
  std::vector<BucketSorterSettings> settings;
  settings.resize(20);
  size_t i = 0;

  // Use one with default settings.
  i++;

  // Use parallel+default settings.
  settings[i].Parallel = true;
  i++;

  if( num < 10*1024 ) {
    // Try doing it all with the simple_sort
    settings[i].Parallel = false;
    settings[i].SortSwitch = MAX;
    settings[i].SwitchPermute = MAX;
    i++;

    settings[i].Parallel = false;
    settings[i].SortSwitch = 2;
    settings[i].SwitchPermute = 2;
    i++;

    settings[i].Parallel = false;
    settings[i].SortSwitch = 100;
    settings[i].SwitchPermute = 100;
    i++;

    settings[i].Parallel = true;
    settings[i].SortSwitch = 2;
    settings[i].SwitchPermute = 2;
    i++;

    settings[i].Parallel = true;
    settings[i].SortSwitch = 100;
    settings[i].SwitchPermute = 100;
    i++;
  }

  settings.resize(i);

  for( size_t i = 0; i < settings.size(); i++ ) {
    BucketSorterSettings s = settings[i];

    test_bucket_sort_simple_impl<4,256>(num, s);
    if( num < 10*1024 ) {
      test_bucket_sort_simple_impl<4,16>(num, s);
      test_bucket_sort_simple_impl<4,1000>(num, s);
      test_bucket_sort_simple_impl<4,2000>(num, s);
      test_bucket_sort_simple_impl<5,256>(num, s);
    }
    // Try some random input.
    test_bucket_sort_random_impl<4,256>(num, s);
    if( num < 10*1024 ) {
      test_bucket_sort_random_impl<4,16>(num, s);
      test_bucket_sort_random_impl<4,1000>(num, s);
      test_bucket_sort_random_impl<4,2000>(num, s);
      test_bucket_sort_random_impl<4,256>(num, s);
      test_bucket_sort_random_impl<5,256>(num, s);
    }
    // Try some permutations.
    test_permute<4>(num, s);
    if( num < 10*1024 ) {
      test_permute<5>(num, s);
    }
  }
}

void minitests(void)
{
  typedef Record<4> MyRecord;
  typedef RecordIterator<MyRecord> MyIterator;
  MyRecord arr[10];
  MyIterator begin((unsigned char*) &arr[0]);
  MyIterator end((unsigned char*) &arr[10]);
  MyIterator i;
  MyRecord tmp;

  int num = 0;
  for( i = begin; i < end; i++ ) {
    tmp.key = num;
    tmp.v = i - begin;
    *i = tmp;
    num++;
  }

  tmp = arr[1];
  assert(tmp.key == 1);
  assert(tmp.v == 1);

  tmp = begin[2];
  assert(tmp.key == 2);
  assert(tmp.v == 2);

  i = begin + 3;
  tmp.key = 300;
  tmp.v = 300;
  *i = tmp;
  tmp = arr[3];
  assert(tmp.key == 300);

  tmp.key = 400;
  tmp.v = 400;
  begin[4] = tmp;
  tmp = arr[4];
  assert(tmp.key == 400);

  begin[1] = begin[2];
  tmp = arr[1];
  assert(tmp.key == 2);
  tmp = arr[2];
  assert(tmp.key == 2);

  tmp = arr[5];
  assert(tmp.key == 5);
  assert(tmp.v == 5);

  num = 10-1;
  for( i = begin; i < end; i++ ) {
    tmp.key = num;
    tmp.v = num;
    *i = tmp;
    num--;
  }

  // Try sorting..
  Criterion<4> ctx;
  RecordSortingCriterion<MyRecord,Criterion<4> > crit(ctx);
  std::sort(begin, end, crit);

  assert(arr[0].key == 0);
  assert(arr[9].key == 9);
}

int main(int argc, char** argv)
{
  srand(1);

  minitests();
  
  //test_bucket_sort_mem(9);
  //test_bucket_sort_mem(1000);
  //test_bucket_sort_mem(20);
  //test_bucket_sort_mem(37);
  //test_bucket_sort_external(false, 4*1024, 16*1024*1024, 0);

  std::cout << "Checking internal memory sorts" << std::endl;

  // test the internal-memory 
  for( int i = 2; i < 50; i++ ) {
    test_bucket_sort_mem(i);
  }
  for( int i = 50; i < 100; i+=7 ) {
    test_bucket_sort_mem(i);
  }
  test_bucket_sort_mem(200);
  // try 1 K
  test_bucket_sort_mem(1024);
  //test_bucket_sort_mem(1024*1024);
  test_bucket_sort_mem(2*1024);
  test_bucket_sort_mem(10*1024);
  test_bucket_sort_mem(512*1024);
  printf("\n"); 
  //test_bucket_sort_mem(200*1024*1024);

  std::cout << "All tests PASSED" << std::endl;
}

