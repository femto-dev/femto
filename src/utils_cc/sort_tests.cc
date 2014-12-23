/*
  (*) 2008-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/utils_cc/sort_tests.cc
*/
#include <vector>
extern "C" {
#include "timing.h"
#include "config.h"
}
#include <cstdio>
#include <cstring>

#include <algorithm>

#ifdef HAVE_PARALLEL_ALGORITHM
#include <parallel/algorithm> // sort
#endif

#include "sort_tests.hh"

#include "file_pipe_context.hh"
#include "pipelining.hh"

//#include "form_runs_node.hh"
#include "merge_node.hh"
#include "bucket_sort.hh"
#include "distributor_node.hh"
#include "sort.hh"

#define EXTRA_BYTES 4
// bits for each of the 2 numbers in a packed record.
#define PACKED_RECORD_BITS 64

typedef Record<EXTRA_BYTES> MyRecord;
typedef RecordPtr<EXTRA_BYTES> MyRecordPtr;
typedef Criterion<EXTRA_BYTES> MyCriterion;
typedef ComparisonCriterion<EXTRA_BYTES> MyComparisonCriterion;
typedef MyCriterion::key_t MyKey;

typedef PackedRecord<PACKED_RECORD_BITS> MyPackedRecord;
typedef PackedRecordCriterion<PACKED_RECORD_BITS> MyPackedRecordCriterion;
typedef MyPackedRecordCriterion::key_t MyPackedRecordKey;
typedef RecordTraits<MyPackedRecord> MyPackedRecordTraits;
typedef MyPackedRecordTraits::iterator_t MyPackedRecordIterator;
#define packed_record_size (RecordTraits<MyPackedRecord>::record_size)

void check_sorted(std::vector<MyRecord> & values, uint64_t num_zeros)
{
  printf("Checking that %li records are sorted and have %li zeros\n",
         (long int) values.size(), (long int) num_zeros);

  uint64_t counted_zeros = 0;
  for( size_t i = 0; i < values.size(); i++ ) {
    if( values[i].key == 0 ) counted_zeros++;
    if( i > 0 ) assert(values[i].key >= values[i-1].key);
  }
  assert(counted_zeros == num_zeros);
}

void check_sorted(FILE* f, uint64_t n, uint64_t num_zeros)
{
  printf("Checking that %li records are sorted and have %li zeros\n",
         (long int) n, (long int) num_zeros);

  uint64_t counted_zeros = 0;
  ssize_t ret_size;
  size_t chunk_size=1024*1024;
  std::vector<MyRecord> values;
  values.resize(chunk_size);
  MyRecord last;

  // get the file length.
  assert( file_len(f) == n*sizeof(MyRecord) );

  rewind(f);

  // Read chunks of records.
  for( size_t i = 0; i < n; ) {
    if( i + values.size() > n ) values.resize(n - i);

    // Read in a chunk.
    ret_size = fread(&values[0], sizeof(MyRecord), values.size(), f);
    assert( ret_size == (ssize_t) values.size() );

    // Go through the chunk.
    for( size_t j = 0; j < values.size(); j++ ) {
      if( values[j].key == 0 ) counted_zeros++;
      if( i > 0 && j > 0 ) {
        if(last.key <= values[j].key) {
          // OK
        } else {
          printf("At position %lli; last.key=%llx cur.key=%llx\n",
                 (long long int) (i+j),
                 (long long int) last.key,
                 (long long int) values[j].key);
          assert(last.key <= values[j].key);
        }
      }
      last = values[j];
    }

    i += values.size();
  }
  assert(counted_zeros == num_zeros);
}
int main(int argc, char** argv)
{
  size_t a_gb = 1024*1024*1024;
  double gb = 1;
  int test = 0;
  Context ctx;
  MyCriterion crit;

  if( argc<3 ) {
    printf("Usage: %s [--file path] [--reuse] <gb> <test-num>\n", argv[0]);
    printf("  where <gb> is the number of gb to sort\n");
    printf("  where <test-num> is:\n"
           "    -1 -- 16-byte direct, serial STL sort\n"
           "    -2 -- 16-byte direct, parallel STL sort\n"
           "    -3 -- 16-byte pointer-indirect parallel STL sort\n"
           "    -4 -- 16-byte adaptive radix sort\n"
           "    -6 -- 16-byte simple_sort\n"
           "    -7 -- 2 9-byte+9-byte records, radix sort\n"
           "    -8 -- 16-byte openmp parallel in-place adaptive radix sort\n"
           "    -9 -- 16-byte general comparison sort (serial STL)\n"
           "\n"
           "  -100 -- 16-byte in-place permutation\n"
           "  -101 -- 16-byte copying permutation\n"
           "  -102 -- 16-byte in-place permutation with chunks\n"
           "  -103 -- 16-byte in-place sort/permutation\n"
           "  -130 -- distributing permutation (last arg #ways)\n"
           "  -200 -- copying\n"
           "  -300 -- distributing (last arg #ways)\n"
           "\n"
           "    6 -- 16-byte adaptive radix sort easy mmap\n"
           "   11 -- 16-byte distribute/adaptive radix sort\n"
           "  101 -- 16-byte distribute/adaptive radix sort (mem-disk-mem)\n"
          );
    return -1;
  }

  bool reuse = false;
  const char* path = "bucket_sort_test_file";
  int i = 1;
  if( 0 == strcmp(argv[i], "--file") ) {
    i++;
    path = argv[i];
    i++;
  }
  if( 0 == strcmp(argv[i], "--reuse") ) {
    reuse = true;
    i++;
  }
  gb = atof(argv[i++]);
  test = atoi(argv[i++]);

  double num_mb = gb * 1024;
  size_t num = (size_t) ((gb*a_gb)/sizeof(MyRecord));

  assert(gb > 0);

  std::vector<MyRecord > values;
  uint64_t num_zeros = 0;

  if( test < 0 ) {
    if( (test < 100 && test > -100) || test <= -200) {
      printf("Forming %f GB input. Sizeof Record is %zi bytes\n", gb, sizeof(MyRecord));
      values.reserve(num);
      for( size_t i = 0; i < num; i++ ) {
        MyRecord x;
        x.randomize();
        if( x.key == 0 ) num_zeros++;
        values.push_back(x);
      }
    } else {
      printf("Forming %f GB permutation input. Sizeof Record is %zi bytes\n", gb, sizeof(MyRecord));
      values.reserve(num);
      for( size_t i = 0; i < num; i++ ) {
        MyRecord x;
        x.key = i;
        if( x.key == 0 ) num_zeros++;
        values.push_back(x);
      }
      std::random_shuffle(values.begin(), values.end());
    }

    if( test == -1 ) {
      printf("Starting direct,serial sort\n");
      start_clock();
      // Now sort!
      RecordSortingCriterion<MyRecord,MyCriterion > criterion(crit);
      sort(values.begin(), values.end(), criterion);
      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -2 ) {
      printf("Starting direct,parallel sort\n");
      start_clock();
      // Now sort!
      RecordSortingCriterion<MyRecord,MyCriterion > criterion(crit);
#ifdef HAVE_PARALLEL_ALGORITHM
      __gnu_parallel::sort(values.begin(), values.end(), criterion);
#else
      sort(values.begin(), values.end(), criterion);
#endif
      stop_clock();
      print_timings("megabytes", num_mb);
   
    } else if( test == -3 ) {
      printf("Starting parallel pointer sort\n");
      start_clock();
      // Now sort the vectors by sorting pointers and re-sorting.
      std::vector<MyRecordPtr> value_ptrs;
      value_ptrs.reserve(num);
      for( size_t i = 0; i < num; i++ ) {
        value_ptrs.push_back(&values[i]);
      }
      start_clock();
      RecordSortingCriterion<MyRecordPtr,MyCriterion > criterion(crit);
#ifdef HAVE_PARALLEL_ALGORITHM
      __gnu_parallel::sort(value_ptrs.begin(), value_ptrs.end(), criterion);
#else
      sort(value_ptrs.begin(), value_ptrs.end(), criterion);
#endif
      stop_clock();
      print_timings("internal sort megabytes", num_mb);

      std::vector<MyRecord > values_out;
      values_out.reserve(num);
      // Now re-arrange with the pointers.
      for( size_t i = 0; i < num; i++ ) {
        values_out.push_back(*(value_ptrs[i]).x);
      }

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == 2 ) {
      // Used to test form runs/merge.
    } else if( test == -4 ) {
      printf("Starting adaptive radix, direct sort\n");
      start_clock();
      MyKey min = 0;
      MyKey max = -1L;

      typedef BucketSorter<MyRecord, MyCriterion> MySorter;
      MySorter::sort(crit, min, max, &values[0], 0, values.size(), NULL);
      stop_clock();
      print_timings("megabytes", num_mb);
   
    } else if( test == -5 ) {
      printf("Starting adaptive radix (countsub), direct sort\n");
      printf("Abandoned - no longer implemented\n"); exit(-1);
    } else if( test == -6 ) {
      printf("Starting simple_sort, direct sort\n");
      start_clock();

      typedef BucketSorter<MyRecord, MyCriterion> MySorter;
      MySorter::both_criterion_t use_crit(crit, CompareSameCriterion());
      MySorter::simple_sort(use_crit, &values[0], values.size());
      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -7 ) {
      printf("Starting packed record sort, direct radix sort\n");
      MyPackedRecordCriterion packed_crit;
      MyPackedRecordKey min = 0;
      MyPackedRecordKey max = -1L;
      size_t n_records = values.size();
      std::vector<char> arr;

      arr.resize(num*packed_record_size);

      for( size_t i = 0; i < n_records; i++ ) {
        MyPackedRecord r;
        r.key = values[i].key;
        r.v = values[i].v;
        MyPackedRecordTraits::encode(r, &arr[0] + i*packed_record_size);
      }

      typedef BucketSorter<MyPackedRecord, MyPackedRecordCriterion> MySorter;
      MyPackedRecordIterator it;

      it = MyPackedRecordTraits::getiter(&arr[0]);

      start_clock();
      MySorter::sort(packed_crit, min, max, it, 0, n_records, NULL);
      stop_clock();

      print_timings("megabytes", num_mb);

      // check it!
      RecordSortingCriterion<MyRecord,MyCriterion > criterion(crit);
      sort(values.begin(), values.end(), criterion);

      for( size_t i = 0; i < n_records; i++ ) {
        MyPackedRecord pr;
        MyPackedRecordTraits::decode(pr, &arr[0] + i*packed_record_size);
        MyRecord r = values[i];
        assert( pr.key == r.key );
      }

    } else if( test == -8 ) {
      printf("Starting openmp parallel adaptive radix in-place direct sort\n");
      start_clock();
      MyKey min = 0;
      MyKey max = -1L;

      BucketSorterSettings settings;
      settings.Parallel = true;

      typedef BucketSorter<MyRecord, MyCriterion> MyOmpSorter;

      MyOmpSorter::sort(crit, min, max, &values[0], 0, num, &settings);

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -9 ) {
      printf("Starting general comparison sort\n");
      start_clock();
      MyRecord min, max;
      min.key = 0;
      max.key = -1L;

      MyComparisonCriterion compare_crit;

      typedef GeneralSorter<MyRecord, MyComparisonCriterion> MyGenSorter;

      MyGenSorter::general_sort(compare_crit, min, max, values.begin(), num, NULL);

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -100 ) {

      printf("Doing in-place permutation\n");
      start_clock();
      MyRecord r, tmp;
      for( uint64_t i = 0; i < num; i++ ) {
        r = values[i];
        while( r.key != i ) {
          // swap r with its final home, values[r.key].
          tmp = values[r.key];
          values[r.key] = r;
          r = tmp;
        }
        // Move it to its final home.
        values[r.key] = r;
      }
      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -101 ) {
      printf("Doing copying permutation\n");
      std::vector<MyRecord > dst;
      dst.resize(values.size());

      start_clock();
      for( uint64_t i = 0; i < num; i++ ) {
        uint64_t dst_idx = values[i].key;
        uint64_t next_idx = (i+1<num)?(values[i+1].key):(dst_idx);
        __builtin_prefetch(&dst[next_idx], 1);
        dst[dst_idx] = values[i];
      }
      stop_clock();
      print_timings("megabytes", num_mb);

      for( uint64_t i = 0; i < num; i++ ) {
        values[i] = dst[i];
      }
    } else if( test == -102 ) {
      printf("Doing in-place permutation with chunks\n");
      start_clock();
      static const int max_buf=8;
      //typedef std::list<MyRecord> RecordList;
      //typedef RecordList::iterator RecordListIter;
      //RecordList buf;
      MyRecord tmp;
      MyRecord buf[max_buf];
      int used_buf; // j < used_buf is valid in buf.
      uint64_t i;

#define SWAP(a,b) {tmp = a; a = b; b = tmp;}

      i = 0;
      used_buf = 0;
      // Fill up our list with some records
      // we need to put in the right place.
      while( used_buf < max_buf && i < num ) {
        buf[used_buf] = values[i];
        used_buf++;
        i++;
      }

      while( used_buf > 0 ) {
        // Now go through the records in our buf,
        // looking for the right home.
        for( int j = 0; j < used_buf; j++ ) {
          // prefetch next.
          uint64_t dst_idx = buf[j].key;
          //uint64_t next_idx = (j+1<used_buf)?(buf[j+1].key):(dst_idx);
          //__builtin_prefetch(&values[next_idx], 1);

          // swap r with its final home, values[r.key]
          SWAP(buf[j],values[dst_idx]);
          if( dst_idx < i ) {
            // We just put it where it belongs and finished our cycle.
            // Get the next element from the input.
            
            // Advance i to next out-of-place one.
            while( i < num && i == values[i].key ) i++;
            // Now, put an empty at the end, or put the value in buf.
            if( i < num ) {
              buf[j] = values[i];
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

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -103 ) {
      printf("Doing in-place sort/permutation\n");
      start_clock();
      MyKey min = 0;
      MyKey max = num-1;


      BucketSorterSettings settings;
      settings.Permute = true;

      typedef BucketSorter<MyRecord, MyCriterion> MySorter;
      MySorter::sort(crit, min, max, &values[0], 0, values.size(), &settings);

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == -200 ) {
      printf("Doing copy\n");
      std::vector<MyRecord > dst;
      dst.resize(values.size());

      start_clock();
      for( uint64_t i = 0; i < num; i++ ) {
        dst[i] = values[i];
      }
      stop_clock();
      print_timings("megabytes", num_mb);

      return 0;
    } else if( test == -130 ) {
      printf("Starting permute distribute\n");

      MyRecord min, max;
      min.key = 0;
      max.key = num-1;

      size_t n_bins = 1;
      assert( i < argc );
      n_bins = atoi(argv[i++]);
      printf("Distribute using %li bins, %li records\n", n_bins, num);

      memory_pipe input(&values[0], num*sizeof(MyRecord), 64*1024);

      // First, get sample.
      typedef DividingSplitters<MyRecord, MyCriterion> Splitters;
      Splitters splitters(crit, n_bins);

      size_t use_records = 512*n_bins;
      if( use_records > num ) use_records = num;

      // Take the first, say, 512*n_bins records.
      splitters.use_sample_minmax(min.key, max.key);

      start_clock();
      // Now we should have splitters.

      std::vector<write_pipe*> output;

      buffered_pipe out_p(64*1024, n_bins+1);

      for( size_t i = 0; i < n_bins; i++ ) {
        output.push_back(&out_p);
      }

      // Run a distributor node.
      typedef distributor_node<MyRecord, MyCriterion, Splitters> Distributor;
 
      Distributor d(crit, &input, &output, &splitters,
                    Distributor::translator_t());
      d.start();

      while( 1 ) {
        tile t = out_p.get_full_tile();
        if( t.is_end() ) break;
        out_p.put_empty_tile(t);
      }

      d.finish();

      stop_clock();
      print_timings("distribute MB", num_mb);

      exit(0);
    } else if( test == -300 ) {
      printf("Starting distribute\n");

      MyRecord min, max;
      min.key = 0;
      max.key = -1L;

      //long max_per_bin = 1L*1024L*1024L*1024L;
      //size_t n_bins = ((num*sizeof(MyRecord))+max_per_bin-1L) / max_per_bin;
      //printf("Using %li num is %li max_per_bin is %li bins\n", n_bins, num, max_per_bin);
      size_t n_bins = 1;
      assert( i < argc );
      n_bins = atoi(argv[i++]);
      printf("Distribute using %li bins, %li records\n", n_bins, num);

      memory_pipe input(&values[0], num*sizeof(MyRecord), 64*1024);

      start_clock();
      // First, get sample.
      typedef SampleSplitters<MyRecord, MyCriterion> Splitters;
      Splitters splitters(crit, n_bins);

      size_t use_records = 512*n_bins;
      if( use_records > num ) use_records = num;

      // Take the first, say, 512*n_bins records.
      splitters.sort_then_use_sample(min.key, max.key, &values[0], use_records);
      stop_clock();
      print_timings("form splitters", n_bins);

      start_clock();
      // Now we should have splitters.

      std::vector<write_pipe*> output;

      buffered_pipe out_p(64*1024, n_bins+1);

      for( size_t i = 0; i < n_bins; i++ ) {
        output.push_back(&out_p);
      }

      // Run a distributor node.
      typedef distributor_node<MyRecord, MyCriterion, Splitters> Distributor;
 
      Distributor d(crit, &input, &output, &splitters,
                    Distributor::translator_t());
      d.start();

      while( 1 ) {
        tile t = out_p.get_full_tile();
        if( t.is_end() ) break;
        out_p.put_empty_tile(t);
      }

      d.finish();

      stop_clock();
      print_timings("distribute MB", num_mb);

      exit(0);
    } else {
      printf("Unknown internal-memory sort test %i\n", test);
    }

    check_sorted(values, num_zeros);

  } else if( test < 100 ) { 
    FILE* f = fopen(path, (reuse)?("r+"):("w+"));

    if( reuse ) {
      printf("Reusing %f GB input. Sizeof Record is %zi bytes\n", gb, sizeof(MyRecord));
    } else {
      printf("Writing %f GB input. Sizeof Record is %zi bytes\n", gb, sizeof(MyRecord));
      assert(f);

      ssize_t ret_size;
      size_t chunk_size=1024*1024;
      values.reserve(chunk_size);
      for( size_t i = 0; i < num; i++ ) {
        MyRecord x;
        x.randomize();
        if( x.key == 0 ) num_zeros++;
        values.push_back(x);
        if( values.size() >= chunk_size ) {
          ret_size = fwrite(&values[0], sizeof(MyRecord), values.size(), f);
          assert( ret_size == (ssize_t) values.size());
          values.clear();
        }
      }
      ret_size = fwrite(&values[0], sizeof(MyRecord), values.size(), f);
      assert( ret_size == (ssize_t) values.size());
      values.clear();


      rewind(f);
      fflush(f);
      
      // free memory used by values.
      {
        std::vector<MyRecord > tmp;
        tmp.swap(values);
      }

    }

    if( test == 4 ) {
      printf("Starting adaptive radix, direct, external waiting sort\n");
      printf("No longer implemented\n"); exit(-1);
    } else if( test == 5 ) {
      printf("Starting adaptive radix, direct, external aio sort\n");
      printf("No longer implemented\n"); exit(-1);
    } else if( test == 6 ) {
      printf("Starting adaptive radix, direct, easy-mmaped sort\n");
      start_clock();
      MyKey min = 0;
      MyKey max = -1L;

      typedef BucketSorter<MyRecord, MyCriterion> MySorter;
      MySorter::sort_file_mmap_easy(crit, min, max, fileno(f), 0, num, NULL);

      stop_clock();
      print_timings("megabytes", num_mb);
    } else if( test == 7 ) {
      printf("Starting adaptive radix, direct, hinting mmaped sort\n");
      printf("No longer implemented\n"); exit(-1);
   } else if( test == 8 ) {
      printf("Starting adaptive radix (countsub), direct, hinting mmaped sort\n");
      printf("No longer implemented\n"); exit(-1);
    } else if( test == 9 ) {
      printf("Starting adaptive radix (countsub), external waiting sort\n");
      printf("No longer implemented\n"); exit(-1);
    } else if( test == 10 ) {
      printf("Starting adaptive radix (countsub), external aio sort\n");
      printf("No longer implemented\n"); exit(-1);
    } else if( test == 11 ) {
      printf("Starting distribute/adaptive radix sort\n");

      start_clock();

      MyRecord min, max;
      min.key = 0;
      max.key = -1L;

      uint64_t use_memory = 1L*1024L*1024L*1024L;
      if( i < argc ) {
        double use_memory_gb = atof(argv[i++]);
        use_memory = (uint64_t) (a_gb * use_memory_gb);
      }

      long use_procs = 1;
      error_t err = get_use_processors(&use_procs);
      if( err ) throw error(err);
      assert( use_procs > 0);


      typedef SampleSplitters<MyRecord, MyCriterion> MySplitters;
      typedef Sorter<MyRecord,MyCriterion,MySplitters> MySorter;
      typedef MySorter::sort_status sort_status_t;
      typedef MySorter::first_pass MyFirstPass;
      typedef MySorter::second_pass MySecondPass;

      size_t n_bins = MySorter::get_n_bins(num, use_memory);

      sort_status_t status(crit, MySorter::translator_t(),
                           path, use_procs, use_procs, false, n_bins,
                           1024*1024*1024);

      printf("Sorting %li records using %li memory (%li bins) and %li processors\n", num, (long) use_memory, status.n_bins, use_procs);

      file_pipe_context input_fctx = fctx_fixed_cached(fileno(f));
      // input pipe must accomadate num_procs threads.
      input_fctx.num_io_groups = 2 * use_procs + 2;

      start_clock();
      // First, get sample.
      MySplitters splitters(crit, status.n_bins);

      // Take the first, say, 512*n_bins records.
      splitters.use_sample_fctx(min.key, max.key, input_fctx, 512*status.n_bins);
      stop_clock();
      print_timings("form splitters", 1);

      start_clock();

      // Now we should have splitters.
      //
      file_read_pipe_t input(input_fctx);

      MyFirstPass first(&status, &input, &splitters);

      start_clock();

      first.start();
      first.finish();

      stop_clock();
      print_timings("distribute MB", num_mb);

      // Check the work of the first pass.
      if( EXTRA_CHECKS ) {
        printf("Checking that distribute operated correctly\n");
        first.check();
      }

      file_pipe_context output_fctx = fctx_fixed_cached(fileno(f));
      output_fctx.create = true;
      output_fctx.write = true;

      {
        int rc = ftruncate(fileno(f), 0);
        if( rc ) throw error(ERR_IO_STR("ftruncate failed"));
      }

      file_write_pipe_t write_pipe(output_fctx);

      MySecondPass second(&status, &write_pipe,//NULL, //&write_pipe,
                          &splitters,
                          MAX_UINT64); //, false /* dont delete */);

      start_clock();

      second.start();
      second.finish();

      stop_clock();
      print_timings("sort subproblems MB", num_mb);

      stop_clock();
      print_timings("megabytes", num_mb);
    } else {
      printf("Unknown external test number %i\n", test);
    }

    check_sorted(f, num, num_zeros);

  } else if ( test == 101 ) {
    printf("Starting distribute/adaptive radix sort\n");

    start_clock();

    MyRecord min, max;
    min.key = 0;
    max.key = -1L;

    uint64_t use_memory = 1L*1024L*1024L*1024L;
    if( i < argc ) {
      double use_memory_gb = atof(argv[i++]);
      use_memory = (uint64_t) (a_gb * use_memory_gb);
    }

    long use_procs = 1;
    error_t err = get_use_processors(&use_procs);
    if( err ) throw error(err);
    assert( use_procs > 0);


    typedef SampleSplitters<MyRecord, MyCriterion> MySplitters;
    typedef Sorter<MyRecord,MyCriterion,MySplitters> MySorter;
    typedef MySorter::sort_status sort_status_t;
    typedef MySorter::first_pass MyFirstPass;
    typedef MySorter::second_pass MySecondPass;

    size_t n_bins = MySorter::get_n_bins(num, use_memory);

    sort_status_t status(crit, MySorter::translator_t(),
                         path, use_procs, use_procs, false, n_bins,
                         1024*1024*1024);

    printf("Sorting %li records using %li memory (%li bins) and %li processors\n", num, (long) use_memory, status.n_bins, use_procs);

    // input pipe must accomadate num_procs threads.
    size_t num_tiles = 2 * use_procs + 2;

    start_clock();
    // First, get sample.
    MySplitters splitters(crit, status.n_bins);

    // Take the first, say, 512*n_bins records.

    srand(0);
    std::vector<MyRecord> sample;
    size_t sample_n = 512*status.n_bins;
    sample.reserve(sample_n);
    for( size_t i = 0; i < sample_n; i++ ) {
      MyRecord x;
      x.randomize();
      sample.push_back(x);
    }
    splitters.sort_then_use_sample(min.key, max.key, &sample[0], sample_n);
    stop_clock();
    print_timings("form splitters", 1);

    start_clock();

    // Now we should have splitters.
    //

    buffered_pipe producer_to_sort(DEFAULT_TILE_SIZE, num_tiles);

    sort_test_producer<MyRecord> producer(&producer_to_sort, num);

    MyFirstPass first(&status, &producer_to_sort, &splitters);

    start_clock();

    srand(0);
    producer.start();
    first.start();
    first.finish();
    producer.finish();

    stop_clock();
    print_timings("distribute MB", num_mb);

    buffered_pipe sort_to_consumer(DEFAULT_TILE_SIZE, num_tiles);
    sort_test_consumer<MyRecord> consumer(&sort_to_consumer, &producer);

    MySecondPass second(&status, &sort_to_consumer,
                        &splitters,
                        MAX_UINT64);

    start_clock();

    consumer.start();
    second.start();
    second.finish();
    consumer.finish();

    stop_clock();
    print_timings("sort subproblems MB", num_mb);

    stop_clock();
    print_timings("megabytes", num_mb);
  }

  return 0;
}
