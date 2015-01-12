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

  femto/src/utils_cc/sort_unit_test.cc
*/
#include "sort_tests.hh"
#include "sort.hh"

#include <cstring>
#include <cstdlib>
extern "C" {
#include <unistd.h>
}

#define VERBOSE 0
#define PROGRESS 0
#define EXTRATESTS 0
 
char* TMP_FILE;

void progress(const char* s)
{
  if( PROGRESS ) {
    printf("%s", s);
    fflush(stdout);
  }
}

#define NUM_TESTS 3
// test == 0 -> permute
// test == 1 -> sort easy
// test == 2 -> sort random
template<int ExtraBytes>
void test_external_sort_core(int test, bool parallel, size_t num_bins, size_t num)
{
  typedef Record<ExtraBytes> MyRecord;
  typedef Criterion<ExtraBytes> MyCriterion;
  typedef typename MyCriterion::key_t key_t;
  typedef SampleSplitters<MyRecord, MyCriterion> MySplitters;
  typedef Sorter<MyRecord, MyCriterion, MySplitters> MySorter;
  typedef typename MySorter::translator_t MySorterTranslator;
  typedef DividingSplitters<MyRecord, MyCriterion> MyPermuteSplitters;
  typedef Sorter<MyRecord, MyCriterion, MyPermuteSplitters> MyPermuter;
  typedef typename MyPermuter::translator_t MyPermuterTranslator;
  typedef typename MyPermuter::first_pass MyPermuteFirstPass;
  typedef typename MyPermuter::second_pass MyPermuteSecondPass;
  typedef Sorter<MyRecord, MyCriterion, MySplitters> MySorter;
  typedef typename MySorter::first_pass MyFirstPass;
  typedef typename MySorter::second_pass MySecondPass;
  
  MyCriterion crit;
  RecordSortingCriterion<MyRecord, MyCriterion> criterion(crit);

  //size_t block_size_bytes = block_size_records * sizeof(Record<ExtraBytes>);

  // create a file, store the records.
  std::vector<MyRecord> values;

  if( VERBOSE ) printf("test external Parallel=%i extrabytes=%i test=%i num_bins=%i num=%i\n", parallel, ExtraBytes, test, (int) num_bins, (int) num);
  else progress("e");

  key_t min = 0;
  key_t max = -1;

  values.reserve(num);
  for( size_t i = 0; i < num; i++ ) {
    if( test < 2 ) {
      MyRecord x;
      x.key = i;
      x.v = x.key;
      values.push_back(x);
      max = num - 1;
    } else {
      MyRecord x;
      x.randomize();
      x.v = x.key;
      values.push_back(x);
      max = -1;
    }
  }

  // Try randomly shuffling the data and then check it again.
  std::random_shuffle(values.begin(), values.end());

  // Create a sorted copy of the data for checking.
  std::vector<Record<ExtraBytes> > copy(values);

  // sort the random data with standard sort.
  std::sort(copy.begin(), copy.end(), criterion);

  long use_procs = 0; // automatically choose..
  if( ! parallel ) use_procs = 1;

  memory_pipe pipe(&values[0], num*sizeof(MyRecord),
                   round_up_to_multiple(DEFAULT_TILE_SIZE,
                                        sizeof(MyRecord)) );

  // Now sort externally...
  if( test == 0 ) {
    // External permute.
    typename MyPermuter::sort_status status(crit, MyPermuterTranslator(), TMP_FILE, use_procs, use_procs, true, num_bins, 1024*1024*1024);

    MyPermuteSplitters splitters(crit, num_bins);
    splitters.use_sample_minmax(min, max);

    MyPermuteFirstPass first(&status, &pipe, &splitters);
    first.start();
    first.finish();

    pipe.reset();

    MyPermuteSecondPass second(&status, &pipe, &splitters);
    second.start();
    second.finish();
  } else {
    // External sort.
    typename MySorter::sort_status status(crit, MySorterTranslator(), TMP_FILE, use_procs, use_procs, false, num_bins, 1024*1024*1024);

    MySplitters splitters(crit, num_bins);
    size_t use = 2*num_bins;
    if( use > num ) use = num;

    splitters.sort_then_use_sample(min, max, &values[0], use);

    MyFirstPass first(&status, &pipe, &splitters);
    first.start();
    first.finish();

    pipe.reset();

    MySecondPass second(&status, &pipe, &splitters);
    second.start();
    second.finish();
  }

  // check that we have proper data.
  // check that the data sorted the same.
  for( size_t i = 0; i < values.size(); i++ ) {
    assert(values[i].key == copy[i].key);
    assert(values[i].v == copy[i].v);
  }
}

void test_external_sort(int test, size_t num_bins, size_t num)
{
  test_external_sort_core<4>(test, false, num_bins, num);
  test_external_sort_core<4>(test, true, num_bins, num);
  test_external_sort_core<5>(test, false, num_bins, num);
  test_external_sort_core<5>(test, true, num_bins, num);
  test_external_sort_core<17>(test, false, num_bins, num);
  test_external_sort_core<17>(test, true, num_bins, num);
  test_external_sort_core<44>(test, false, num_bins, num);
  test_external_sort_core<44>(test, true, num_bins, num);
}

int main(int argc, char** argv)
{
  int nbins[] = {1, 2, 10, 100};
  int n_nbins = sizeof(nbins)/sizeof(int);

  int n[] = {1, 2, 4, 15, 25, 45, 100, 1000, 10*1024, 20*1024, 30*1024, 1024*1024};
  int n_n = sizeof(n)/sizeof(int);

  char* fname = strdup("/tmp/sort_test_XXXXXX");

  int fd = mkstemp(fname);
  TMP_FILE =  fname;

  for( int i = 0; i < n_n; i++ ) {
    std::cout << "Test of size " << n[i] << std::endl;
    for( int t = 0; t < NUM_TESTS; t++ ) {
      for( int b = 0; b < n_nbins; b++ ) {
        test_external_sort(t, nbins[b], n[i]);
      }
    }
  }

  std::cout << "PASS" << std::endl;

  close(fd);
  unlink(fname);
  free(fname);
}

