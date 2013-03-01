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

  femto/src/utils_cc/merger_test.cc
*/
#include <cassert>
#include <algorithm>
#include <iostream>
#include "merger.hh"
#include "criterion.hh"
#include "example_record.hh"

#define DEBUG 0

bool read_pipe_is_end(read_pipe* pipe)
{
  tile t;
  do {
    t = pipe->get_full_tile();
  } while( t.len == 0 && t.data != NULL );
  return t.is_end();
}

void test_mergers(std::vector<std::vector<unsigned int> > inputs, std::vector<unsigned int> expected_output)
{
  if( DEBUG ) {
    std::cout << "test_mergers test case: ";
    for( size_t i = 0; i < inputs.size(); i++ ) {
      for( size_t j = 0; j < inputs[i].size(); j++ ) {
        std::cout << inputs[i][j] << ",";
      }
      std::cout << "; ";
    } 
    std::cout << "-> ";
    for( size_t j = 0; j < expected_output.size(); j++ ) {
      std::cout << expected_output[j] << ","; 
    }
    std::cout << std::endl;
  }

  // Check that the input size matches the output size.
  {
    size_t in_size = 0;
    for( size_t i = 0; i < inputs.size(); i++ ) {
      in_size += inputs[i].size();
    }
    if(in_size != expected_output.size()) {
      std::cout << "Test broken; in_size " << in_size
                << " but expected output size " << expected_output.size()
                << std::endl;
      assert(0);
    }
  }

  if( 0 < inputs.size() && inputs.size() <= K_WAY ) {
    if( DEBUG )
      std::cout << "testing direct merge" << std::endl;

    // First, test the mergers themselves (e.g. 2-way merger).
    {
      typedef merger<IntRecord,IntRecordSortingCriterion> m;
      serial_buffered_pipe* out = new serial_buffered_pipe(2*expected_output.size()*sizeof(int), 1);
      std::vector<read_pipe*> ins(inputs.size());
      for( size_t i = 0; i < inputs.size(); i++ ) {
        ins[i] = create_pipe_for_data(sizeof(int)*inputs[i].size(), (const unsigned char*) &inputs[i][0]);
      }
      
      IntRecordSortingCriterion crit;
      m::do_merge(crit, &ins, out);

      // check the result.
      tile t = out->get_full_tile();
      assert(!t.is_end());
      unsigned int* data = (unsigned int*) t.data; 
      for( size_t j = 0; j < expected_output.size(); j++ ) {
        assert(data[j] == expected_output[j]);
      }
      assert(t.len == sizeof(int)*expected_output.size());

      out->put_empty_tile(t);
      // Check that there are no more tiles.
      assert(read_pipe_is_end(out));
      
      // clean up
      for( size_t i = 0; i < ins.size(); i++ ) {
        delete ins[i];
      }
      delete out;
    }
  }

  if( DEBUG ) 
    std::cout << "testing merger network" << std::endl;

  // Next, test the merger network.
  {
    serial_buffered_pipe* out = new serial_buffered_pipe(2*expected_output.size()*sizeof(int), 1);
    std::vector<read_pipe*> ins(inputs.size());
    for( size_t i = 0; i < inputs.size(); i++ ) {
      ins[i] = create_pipe_for_data(sizeof(int)*inputs[i].size(), (const unsigned char*) &inputs[i][0]);
    }
    
    multiway_merge<IntRecord,IntRecordSortingCriterion>
      (IntRecordSortingCriterion(), &ins, out, 1);

    // check the result.
    if( expected_output.size() == 0 ) {
      assert( read_pipe_is_end(out) );
    } else {
      tile t = out->get_full_tile();
      assert(!t.is_end());
      unsigned int* data = (unsigned int*) t.data; 
      for( size_t j = 0; j < expected_output.size() && sizeof(int)*j < t.len; j++ ) {
        if( DEBUG > 1 ) {
          std::cout << "got " << data[j] << " expect " << expected_output[j] << std::endl;
        }
        assert(data[j] == expected_output[j]);
      }
      assert(t.len == sizeof(int)*expected_output.size());

      out->put_empty_tile(t);

      // check that there are no more tiles.
      assert(read_pipe_is_end(out));
    }

    // clean up
    for( size_t i = 0; i < ins.size(); i++ ) {
      delete ins[i];
    }
    delete out;
  }


  // Next, test the merger network.
  {
    long num_procs = 1;
    error_t err = get_num_processors(&num_procs);
    if( err ) throw error(err);
    if( num_procs <= 0 ) num_procs = 1;

    if( DEBUG ) 
      std::cout << "testing parallel merger network with " << num_procs << " threads" << std::endl;

    serial_buffered_pipe* out = new serial_buffered_pipe(2*expected_output.size()*sizeof(int), 1);
    std::vector<read_pipe*> ins(inputs.size());
    for( size_t i = 0; i < inputs.size(); i++ ) {
      ins[i] = create_pipe_for_data(sizeof(int)*inputs[i].size(), (const unsigned char*) &inputs[i][0]);
    }
    
    multiway_merge<IntRecord,IntRecordSortingCriterion>
      (IntRecordSortingCriterion(), &ins, out, num_procs);

    // check the result.
    if( expected_output.size() == 0 ) {
      assert( read_pipe_is_end(out) );
    } else {
      tile t = out->get_full_tile();
      assert(!t.is_end());
      unsigned int* data = (unsigned int*) t.data; 
      for( size_t j = 0; j < expected_output.size(); j++ ) {
        assert(data[j] == expected_output[j]);
      }
      assert(t.len == sizeof(int)*expected_output.size());

      out->put_empty_tile(t);

      // check that there are no more tiles.
      assert( read_pipe_is_end(out) );
    }

    // clean up
    for( size_t i = 0; i < ins.size(); i++ ) {
      delete ins[i];
    }
    delete out;
  }
}

void test_mergers(std::vector<std::string> inputs, std::string expected_output)
{
  std::vector<std::vector<unsigned int> > ins;
  std::vector<unsigned int> out;
  for( size_t i=0; i < inputs.size(); i++ ) {
    std::vector<unsigned int> vec;
    for( size_t j=0; j < inputs[i].size(); j++ ) {
      vec.push_back(inputs[i][j]);
    }
    ins.push_back(vec);
  }

  for( size_t j = 0; j < expected_output.size(); j++ ) {
    out.push_back(expected_output[j]);
  }

  test_mergers(ins, out);
}
void test_mergers(int x)
{
  std::vector<std::vector<unsigned int> > inputs;
  std::vector<unsigned int> expect_vec;

  inputs.clear();
  // First, test 0, ..., x-1
  for( int i = 0; i < x; i++ ) {
    std::vector<unsigned int> v;
    v.push_back(i);
    expect_vec.push_back(i);
    inputs.push_back(v);
  }
  sort(expect_vec.begin(), expect_vec.end());
  if( DEBUG ) std::cout << "Testing string of length " << x << std::endl;
  test_mergers(inputs, expect_vec);
  reverse(inputs.begin(), inputs.end());
  if( DEBUG ) std::cout << "Testing reverse string of length " << x << std::endl;
  test_mergers(inputs, expect_vec);

  expect_vec.clear();
  inputs.clear();
  // Next, test 'a'('a'+x), ..., ('a'+x-1)('a'+x-1+x)
  for( int i = 0; i < x; i++ ) {
    int i2 = i + x;
    std::vector<unsigned int> v;
    v.push_back(i);
    v.push_back(i2);
    expect_vec.push_back(i);
    expect_vec.push_back(i2);
    inputs.push_back(v);
  }
  sort(expect_vec.begin(), expect_vec.end());
  if( DEBUG ) std::cout << "Testing double string of length " << x << std::endl;
  test_mergers(inputs, expect_vec);
  if( DEBUG ) std::cout << "Testing reverse double string of length " << x << std::endl;
  reverse(inputs.begin(), inputs.end());
  test_mergers(inputs, expect_vec);

  expect_vec.clear();
  inputs.clear();
  // Try it also with random inputs.
  for( int i = 0; i < x; i++ ) {
    int len = rand() % 10;
    std::vector<unsigned int> v;
    for( int j = 0; j < len; j++ ) {
      int r = rand() % (2*x*len);
      v.push_back(r);
      expect_vec.push_back(r);
    }
    sort(v.begin(), v.end());
    inputs.push_back(v);
  }
  sort(expect_vec.begin(), expect_vec.end());
  if( DEBUG ) std::cout << "Testing random string of length " << x << std::endl;
  test_mergers(inputs, expect_vec);

}
void test_mergers(void)
{
  std::vector<std::string> inputs;
  
  test_mergers(1);
  test_mergers(2);
  // test 0 inputs _ -> _
  inputs.clear();
  test_mergers(inputs, "");
  // test 2 inputs "a" "a" -> "aa".
  inputs.clear();
  inputs.push_back("a");
  inputs.push_back("a");
  test_mergers(inputs, "aa");
  // test 2 inputs "abc" "def" -> "abcdef"
  inputs.clear();
  inputs.push_back("abc");
  inputs.push_back("def");
  test_mergers(inputs, "abcdef");
  // test 2 inputs "acegi" "bdfhj" -> "abcdefghij"
  inputs.clear();
  inputs.push_back("acegi");
  inputs.push_back("bdfhj");
  test_mergers(inputs, "abcdefghij");
  inputs.clear();
  inputs.push_back("bdfhj");
  inputs.push_back("acegi");
  test_mergers(inputs, "abcdefghij");

  test_mergers(3);
  test_mergers(4);
  test_mergers(5);
  test_mergers(6);
  test_mergers(7);
  test_mergers(8);
  test_mergers(9);
  test_mergers(10);

  for( int i = 10; i < 1200; i++ ) {
    test_mergers(i);
    std::cout << "." << std::flush;
  }
  std::cout << std::endl;
}

int main(int argc, char** argv)
{
  if( argc == 1 ) {
    test_mergers();
    std::cout << "All merger tests PASS" << std::endl;
  } else if( argc == 2 ) {
    // First, generate 1024 1 MB runs.
    size_t k = 1024;
    std::vector<read_pipe*> ins(k);
    size_t tile_size = 128*1024;//128*1024;//16*1024;
    size_t run_len_bytes = 1*1024*1024;
    size_t run_len = run_len_bytes/sizeof(int);
    printf("Preparing input: %zi runs each of %zi bytes\n", k, run_len_bytes);
    for( size_t i = 0; i < k; i++ ) {
      serial_buffered_pipe* pipe =
        new serial_buffered_pipe(tile_size, 1+run_len/tile_size);
      ins[i] = pipe;
      pipe_back_inserter<IntRecord> writer(pipe);

      std::vector<int> data;
      for( size_t j = 0; j < run_len; j++ ) {
        int x = rand(); 
        data.push_back(x);
      }
      // Sort the data.
      sort(data.begin(), data.end());
      // Now put it into our buffers...
      for( size_t j = 0; j < run_len; j++ ) {
        IntRecord r;
        r.b = data[j];
        writer.push_back(r);
      }
      writer.finish();
    }
    serial_buffered_pipe* out = new serial_buffered_pipe(tile_size, 1+run_len*k/tile_size);
    // Now that we've prepared our input, do the merging.
    long num_procs = 1;
    error_t err = get_num_processors(&num_procs);
    if( err ) throw error(err);
    if( num_procs <= 0 ) num_procs = 1;

    printf("Starting merging with %li threads\n", num_procs);
    start_clock();
    multiway_merge<IntRecord,IntRecordSortingCriterion>
      (IntRecordSortingCriterion(), &ins, out, num_procs);

    stop_clock();
    print_timings("merge", k*run_len);

    // clean up
    for( size_t i = 0; i < ins.size(); i++ ) {
      delete ins[i];
    }
    delete out;
  }

  return 0;
}

