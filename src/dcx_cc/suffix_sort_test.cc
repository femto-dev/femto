/*
  (*) 2008-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/suffix_sort_test.cc
*/
extern "C" {
  #include "config.h"
}

#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

#include <iostream>
#include "varint.hh"
#include "dcx.hh"

#define EXTRATESTS 0
#define VERBOSE 1

void progress(const char* s)
{
    printf("%s", s);
    fflush(stdout);
}

int iproc = 0;
int nproc = 1;
int bin_check[] = {1, 2, 4, 10};
//int bin_check[] = {2};
#define NUMBINCHECK ((int)(sizeof(bin_check)/sizeof(int)))
//#pragma warning fastcompile_on
//#define FASTCOMPILE

char* tmpdir = NULL;

std::string take_prefix(const char* str, size_t len, size_t max)
{
  std::ostringstream os;

  size_t amt = len;
  if( amt > max ) amt = max;
  for( size_t i = 0; i < amt; i++ ) {
    print_character(os, (unsigned char) str[i]);
  }
  if( amt < len ) os << "...";

  return os.str();
}

// Moving forward! Let's suffix sort a few very simple 
// things and test the results.


int test_num = 0;

template<typename Character, int nbits_character, typename Offset, int Period>
void check_ssort_impl(size_t len, const char* input, const int* expected_output, int bins_per_proc, int inmem, Character max_character=255)
{
  typedef Character character_t;
  typedef Offset offset_t;

  int nbins = nproc * bins_per_proc;

  // check that all characters are <= max_character.
  for( size_t i = 0; i < len; i++ ) {
    unsigned char uch = input[i];
    Character ch = uch;
    assert( ch <= max_character );
  }

  fflush(stdout);
  fflush(stderr);

#ifdef HAVE_MPI_H
  if(nproc > 1) {
    int rc = MPI_Barrier(MPI_COMM_WORLD);
    if( rc ) throw error(ERR_IO_STR("MPI_Barrier failed"));
  }
#endif

  if( iproc == 0 ) {
    if( VERBOSE ) {
      std::cout << "##########################" 
                << " check_ssort_impl"
                << " test_num=" << test_num
                << " period=" << Period
                << " nbins=" << nbins
                << " (" << len << ") \'" << take_prefix(input,len,20) << "\'"
                << std::endl;
    } else progress(".");
  }

  char input_fname[1024];
  char output_fname[1024];
  snprintf(input_fname,1024,"%s/%s_%i", tmpdir, "test_input", test_num);
  snprintf(output_fname,1024,"%s/%s_%i", tmpdir, "test_output", test_num);


  test_num++;

  // Make sure that the tmp dir exists.
  if( iproc == 0 ) { 
    error_t err;
    err = mkdir_if_needed(tmpdir);
    assert(!err);
  }

  // Prepare the input.
  if( iproc == 0 ) {
    FILE* f = fopen(input_fname, "w+");
    assert(f);

    std::vector<character_t> chars;
    chars.resize(len);
    for( size_t i = 0; i < len; i++ ) {
      chars[i] = (unsigned char) input[i]; // avoid sign-extending
    }

    size_t wrote;
    wrote = fwrite(&chars[0], sizeof(character_t), chars.size(), f);
    assert(wrote == chars.size());

    fclose(f);
  }

#ifdef HAVE_MPI_H
  if(nproc > 1) {
    int rc = MPI_Barrier(MPI_COMM_WORLD);
    if( rc ) throw error(ERR_IO_STR("MPI_Barrier failed"));
  }
#endif

  typedef Dcx<nbits_character,
              8*sizeof(Offset) - 1,
              8*sizeof(Offset) - 1,
              Period> dcx_t;
  dcx_t dcx(tmpdir, len, nbins, DEFAULT_COMM, inmem, 0, NULL);

  file_read_pipe_t input_pipe(fctx_fixed_cached(input_fname));
  fileset output_set(nbins, fctx_fixed_cached(output_fname));
  std::vector<std::string> output_filenames = output_set.get_names();
  read_pipe* input_p = &input_pipe;
  if( iproc != 0 ) input_p = NULL;

  Offset len_offset = len;
  dcx.suffix_sort(len_offset, max_character, input_p, &output_filenames);

#ifdef HAVE_MPI_H
  if(nproc > 1) {
    int rc = MPI_Barrier(MPI_COMM_WORLD);
    if( rc ) throw error(ERR_IO_STR("MPI_Barrier failed"));
  }
#endif

  delete dcx_g_handler;
  dcx_g_handler = NULL;

  // Check the output.
  if( iproc == 0 ) {
    // Read the output.
    std::vector<offset_t> sa;
    {
      fileset_read_pipe_t check_pipe(output_set);
      pipe_iterator<Offset> read(&check_pipe);
      pipe_iterator<Offset> end;

      sa.resize(len);

      size_t i = 0;

      while( read != end ) {
        sa[i++] = *read;
        ++read;
      }
      read.finish();
    }

    for( size_t i = 0; i < len; i++ ) {
      long got = sa[i];
      long expected = expected_output[i];
      if( got != expected ) {
        std::cerr << "at " << i << " got " << got << " but expected " << expected << std::endl;
        assert(got == expected);
      }
    }

    if( ! DONT_DELETE ) {
      // DONT_DELETE is in dcx.hh
      unlink_ifneeded(input_fname);
      unlink_ifneeded(output_filenames);
    }
  }
}

void check_ssort_normal(size_t len, const char* input, const int* expected_output, int inmem)
{
  for( int i = 0; i < NUMBINCHECK; i++ ) {
    int nbins = bin_check[i];
    check_ssort_impl<uint8_t, 9, uint32_t, 3>(len, input, expected_output, nbins, inmem);
#ifndef FASTCOMPILE
    check_ssort_impl<uint8_t, 9, uint32_t, 7>(len, input, expected_output, nbins, inmem);
    check_ssort_impl<uint8_t, 9, uint32_t, 13>(len, input, expected_output, nbins, inmem);
#endif
    //check_ssort_impl<uint8_t, uint32_t, 21>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 31>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 39>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 57>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 73>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 91>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 95>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 133>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 1024>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 2048>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 4096>(len, input, expected_output, nbins);
    //check_ssort_impl<uint8_t, uint32_t, 8192>(len, input, expected_output, nbins);
  }
}

void check_ssort_tiny(size_t len, const char* input, const int* expected_output, int inmem)
{
  // Just don't do examples with too-big lengths.
  if( len >= 16000 ) return;

  // Don't do any of the tests which contain a 255.
  for( size_t i = 0; i < len; i++ ) {
    unsigned char ch = input[i];
    if( ch == 255 ) return;
  }

  for( int i = 0; i < NUMBINCHECK; i++ ) {
//#ifndef FASTCOMPILE
    int nbins = bin_check[i];
    check_ssort_impl<uint8_t, 8, uint16_t, 3>(len, input, expected_output, nbins, inmem, 254);
//#endif
  }
}

void check_ssort_wide(size_t len, const char* input, const int* expected_output, int inmem)
{
  for( int i = 0; i < NUMBINCHECK; i++ ) {
#ifndef FASTCOMPILE
    int nbins = bin_check[i];
    check_ssort_impl<uint64_t, 63, uint64_t, 3>(len, input, expected_output, nbins, inmem);
#endif
  }
}

void check_ssort(size_t len, const char* input, const int* expected_output)
{
  int inmem = 0;
  for( inmem = 1; inmem>=0; inmem--) {
    std::cout << "########################## inmem= " << inmem << std::endl;
    if( iproc == 0 ) {
      if( VERBOSE ) {
        std::cout << "##########################" 
                  << " Running tiny suffix sort test with (" << len << ") \'" << take_prefix(input,len,20) << "\'" << std::endl;
      }
    }
    fflush(stdout); fflush(stderr);

    check_ssort_tiny(len, input, expected_output, inmem);

    if( iproc == 0 ) {
      if( VERBOSE ) {
        std::cout << "##########################" 
                  << " Running normal suffix sort test with (" << len << ") \'" << take_prefix(input,len,20) << "\'" << std::endl;
      }
    }
    fflush(stdout); fflush(stderr);

    check_ssort_normal(len, input, expected_output, inmem);

    if( iproc == 0 ) {
      if( VERBOSE ) {
        std::cout << "##########################" 
                  << " Running wide suffix sort test with (" << len << ") \'" << take_prefix(input,len,20) << "\'" << std::endl;
      }
    }
    fflush(stdout); fflush(stderr);

    check_ssort_wide(len, input, expected_output, inmem);
  }
}

// Test sequences consisting of descending characters.
// The pattern
// max, max-1, max-2, ..., 0
// is repeated enough times to create the input.
// If max >256, we'll repeat a character
void test_descending(int max, int repeats, size_t len)
{
  int* output = (int*) malloc(len*sizeof(int));
  char* input = (char*) malloc(len);
  size_t num_cycles;
  size_t cycle_len;

  if( iproc == 0 ) {
    if( VERBOSE ) {
      std::cout << "test_descending " << max << " " << repeats << " " << len << std::endl;
    }
  }

  // Use a length that is a multiple of max*repeats.
  cycle_len = max*repeats;
  len /= cycle_len;
  len *= cycle_len;
  num_cycles = len / (max*repeats);

  assert( len % max == 0 );
  assert( max <= 256 );

  unsigned char c = max-1;
  int count = repeats-1;
  for( size_t i = 0; i < len; i++ ) {
    input[i] = c;
    if( count == 0 ) {
      if( c == 0 ) c = max-1;
      else c--;
      count = repeats-1;
    } else {
      count--;
    }
  }

  size_t i;
  int offset;
  // First, the 0# 00# 000# ...
  for( i = 0; i < (size_t) repeats; i++ ) {
    output[i] = len - i - 1;
  }
  // Next, the other 003's and then the 003's
  for( int rep = repeats-1; rep >= 0; rep-- ) {
    for( ssize_t cycle = num_cycles-2; cycle>=0; cycle-- ) {
      output[i++] = cycle*cycle_len + cycle_len - rep - 1;
    }
  }
  // Finally, the first 1.
  offset = len - repeats - 1;
  for( ; i < len; i++ ) {
    output[i] = offset;
    offset -= max*repeats;
    if( offset < 0 ) {
      offset+=len-1;
    }
  }
  check_ssort(len, input, output);
  free(output);
  free(input);
}

// Test sequences consisting of entirely a repeated character.
void test_repeats(char c, size_t len)
{
  int* output = (int*) malloc(len*sizeof(int));
  char* input = (char*) malloc(len);
  
  if( iproc == 0 ) {
    if( VERBOSE ) {
      std::cout << "  test_repeats " << (int) c << " " << len << std::endl;
    }
  }

  memset(input, c, len);
  for( size_t i = 0; i < len; i++ ) {
    output[i] = len - i - 1;
  }
  check_ssort(len, input, output);
  free(output);
  free(input);
}

int main(int argc, char** argv)
{
  MPI_handler::init_MPI(&argc, &argv);
  nproc = MPI_handler::get_nproc();
  iproc = MPI_handler::get_iproc();

  if( argc == 2 ) {
    tmpdir = argv[1];
    std::cout << "Using tmp dir" << tmpdir << std::endl;
  } else {
    if( iproc == 0 ) {
      tmpdir = tempnam("/tmp/", "dcx_test");
      assert(tmpdir);
    }

#ifdef HAVE_MPI_H
    // Send tmpdir to all other machines.
    long long int len = 0;
    if( iproc == 0 ) len = strlen(tmpdir) + 1;
    // Communicate n to all the nodes.
    MPI_Bcast( &len, 1, MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD );
    if( iproc != 0 ) {
      tmpdir = (char*) malloc(len);
      assert(tmpdir);
    }
    MPI_Bcast( tmpdir, len, MPI_BYTE, 0, MPI_COMM_WORLD );
#endif 
  }
  
  {
    const char* input = "ba";
    int output[] = {1,0};
    std::cout << "Small test" << std::endl;
    check_ssort_wide(2, input, output, 1);
  }

  {
    const char* input = "";
    int output[] = {};
    check_ssort(0, input, output);
  }

  {
    const char* input = "a";
    int output[] = {0};
    check_ssort(1, input, output);
  }
  
  {
    const char* input = "ab";
    int output[] = {0,1};
    check_ssort(2, input, output);
  }

  {
    const char* input = "ba";
    int output[] = {1,0};
    check_ssort(2, input, output);
  }

  {
    const char* input = "aa";
    int output[] = {1,0};
    check_ssort(2, input, output);
  }

  {
    const char* input = "aba";
    int output[] = {2,0,1};
    check_ssort(3, input, output);
  }

  {
    const char* input = "aab";
    int output[] = {0,1,2};
    check_ssort(3, input, output);
  }

  {
    const char* input = "baa";
    int output[] = {2,1,0};
    check_ssort(3, input, output);
  }

  {
    const char* input = "ab\n\n";
    int output[] = {3,2,0,1};
    check_ssort(4, input, output);
  }


  {
    const char* input = "ababab";
    int output[] = {4,2,0,5,3,1};
    check_ssort(6, input, output);
  }

  {
    const char* input = "\x0\xff\x0\xff\x0\xff";
    int output[] = {4,2,0,5,3,1};
    check_ssort(6, input, output);
  }


  {
    const char* input = "bababa";
    int output[] = {5,3,1,4,2,0};
    check_ssort(6, input, output);
  }

  {
    const char* input = "aabaab";
    int output[] = {3,0,4,1,5,2};
    check_ssort(6, input, output);
  }

  {
    const char* input = "bbabba";
    int output[] = {5,2,4,1,3,0};
    check_ssort(6, input, output);
  }

  {
    const char* input = "seeresses";
    int output[] = {1,2,7,4,3,8,0,6,5};
    check_ssort(9, input, output);
  }

  {
    const char* input = "abracadabra";
    int output[] = {10,7,0,3,5,8,1,4,6,9,2};
    check_ssort(11, input, output);
  }

  {
    const char* input = "mississippi";
    int output[] = {10,7,4,1,0,9,8,6,3,5,2};
    check_ssort(11, input, output);
  }

  {
    const char* input = "aaaacaaaacaaaab";
    int output[] = {10,5,0,11,6,1,12,7,2,13,8,3,14,9,4};
    check_ssort(15, input, output);
  }


  // Test repeating cases for different values..
  for( int n = 1; n <= 10; n++ ) {
    test_repeats('a', n);
    test_repeats('\0', n);
    test_repeats('\xff', n);
    test_repeats('\xfe', n);
  }

  test_repeats('a', 14);
  test_repeats('a', 20);
  test_repeats('a', 30);
  test_repeats('a', 40);
  test_repeats('a', 50);
  test_repeats('\0', 50);
  test_repeats('\xff', 50);

  // Test small repeated descending sequences
  test_descending(2, 5, 2*5*4);
  test_descending(3, 3, 3*3*2);
  test_descending(4, 1, 4*1*2);
  test_descending(4, 2, 4*2*2);
  test_descending(4, 2, 4*2*3);
  test_descending(4, 3, 4*3*2);
  test_descending(4, 3, 4*3*3);
  test_descending(4, 8, 4*8*3);
  test_descending(4, 8, 4*8*4);


  // test some more repeats.
  test_repeats('a', 60);
  test_repeats('a', 70);
  test_repeats('a', 80);
  test_repeats('a', 90);
  test_repeats('a', 100);
  test_repeats('\0', 100);
  test_repeats('\xff', 100);

  // Test some medium-size cases.
  test_repeats('a', 1024);
  test_repeats('a', 100*1024);
  test_repeats('\0', 1024);
  test_repeats('\xff', 1024);

  // test some medium sized descending sequences
  test_descending(4, 8, 4*8*1024);
  test_descending(20, 2, 20*2*4);
  test_descending(50, 5, 50*5*10);
  test_descending(200, 1, 200);
  test_descending(255, 1, 255);
  test_descending(255, 2, 255*2);
  test_descending(256, 1, 256);
  test_descending(256, 1, 256*1*2);
  test_descending(256, 2, 256*2*2);
  test_descending(256, 8, 256*8*2);

  if( EXTRATESTS ) {
    // Test large repeats
    test_repeats('a', 1024*1024);
    test_repeats('\0', 1024*1024);
    test_repeats('a', 10*1024*1024);
    test_repeats('a', 50*1024*1024);

    // Try with 100 MB of a.
    test_repeats('a', 100*1024*1024);
    // Try with 100 MB of 0s.
    test_repeats('\0', 100*1024*1024);


    // Test large repeated descending sequences
    test_descending(256, 16, 128*1024*1024);
  }

  MPI_handler::finalize_MPI();

  progress("\n");

  std::cout << "Suffix sort test PASS" << std::endl;
  return 0;
}

