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

  femto/src/utils_cc/sort_node_test.cc
*/
extern "C"{
#include <sys/mman.h>
#include <unistd.h>
#include <sys/types.h>
#include "timing.h"
}

#include "cassert"
#include "sort_node.hh"
#include "example_record.hh"

#include "pipelining.hh"
#include "aio_pipe.hh"

#include "print_records_node.hh"

#define DEBUG 0

bool sort_node_unit_test (buffered_pipe &input,ExampleRecordContext* ctx, size_t num_records)
{
  tile in_tile;
  ExampleRecord last;
  unsigned char buf[1024];
  bool isfirst=true;
  size_t got_records=0;
  //start reading the pipe and comparing the nodes
  while( 1 ) 
  {
    // read an input tile
    in_tile = input.get_full_tile();
    if( ! in_tile.has_tile() ) break;
    
    tile& in_t = in_tile;
        
    size_t len;
    size_t in_pos;
    unsigned char* in_data;
    
    // Save the number of bytes in the input tile
    len = in_t.len;
    in_data = in_t.data;
            
    //making record pointers which pointer to each record
    
    //cout<<"length of the tile "<<len<<endl;
    for(in_pos=0;in_pos<len;)
    {
      if (isfirst)
      {
        isfirst=false;
        ExampleRecord current;
        current.decode(ctx, in_data + in_pos);
        in_pos=in_pos+current.get_record_length(ctx);
        current.encode(ctx, buf);
        last.decode(ctx, buf);
        got_records++;
      }
      else
      {
        ExampleRecord current;
        current.decode(ctx, in_data + in_pos);
        if( DEBUG ) {
          std::cout<<"Comparing records:\n"<<
            "1 "<<last.to_string(ctx)<<"\n"<<
            "2 "<<current.to_string(ctx)<<std::endl;
        }
          
        if(compare_record(ctx,last,current)>0) 
        {
          std::cerr<<"Comparing records " << got_records
                << " returns bad order:\n"<<
            "1 "<<last.to_string(ctx)<<"\n"<<
            "2 "<<current.to_string(ctx)<<std::endl;
          input.close_empty();
          return false;
        }
              
        in_pos=in_pos+current.get_record_length(ctx);
        current.encode(ctx, buf);
        last.decode(ctx, buf);
        got_records++;
      }
    }
    input.put_empty_tile(in_tile);
     
  }
  input.close_empty();

  if( got_records != num_records ) {
    std::cerr << "Dropped records" << std::endl;
    return false;
  }
  return true;
}


void test_sort(size_t tile_size, size_t tiles_per_group, size_t total_data)
{
  unsigned char key[1024];
  FILE* input = tmpfile();
  //FILE* input = fopen("/tmp/test_input", "w+");
  size_t num_records = 0;
  //FILE* output = tmpfile();
  //int tile_size = 256*1024;
  //size_t tiles_per_group=128;
  //size_t total_data = 128*1024*1024L; // 128 MB
 
  typedef ExampleRecordContext Ctx; 
  ExampleRecordContext ctx;

  ctx.default_tile_size = tile_size;
  ctx.default_tiles_per_io_group = tiles_per_group;

  assert(input);

  std::cout<<"Forming input..."<<std::endl;
  {
    Ctx::file_write_pipe_t write_pipe(
                         file_pipe_context(NULL, 
                                           fileno(input),
                                           0, PIPE_UNTIL_END,
                                           tile_size,
                                           tiles_per_group,
                                           ctx.default_num_io_groups));
    pipe_back_inserter<ExampleRecordContext,ExampleRecord> writer(&ctx, &write_pipe);
    
    // Make up some records and store them in a file.
    // Make records with random keys and values counting up.
    for( size_t i = 0, size=0; size < total_data; i++ ) {
      int key_length = rand() % 10; // in [0,10)
      for( int j = 0; j < key_length; j++ ) {
        int r = rand();
        if( r % 3 == 0 ) {
          key[j] = 'a' + rand() % (1+'z'-'a');
        } else if( r % 3 == 1 ) {
          key[j] = 'A' + rand() % (1+'Z'-'A');
        } else {
          key[j] = '0' + rand() % (1+'9'-'0');
        }
      }
      ExampleRecord r(key_length, key, i);
      // Write(encode) the record to the file.
      writer.push_back(r); // or *writer = r; ++writer;
      if( DEBUG ) {
        std::cout<<"Adding "<< r.to_string(&ctx) << std::endl;
      }
      size += r.get_record_length(&ctx);
      num_records++;
    }
    writer.finish();
  }

  rewind(input);

  start_clock();

  std::cout<<"Starting sorter..."<<std::endl;
  // create the pipes
  Ctx::file_read_pipe_t input_pipe(
                     file_pipe_context(NULL, 
                                       fileno(input),
                                       0, PIPE_UNTIL_END,
                                       tile_size,
                                       tiles_per_group,
                                       ctx.default_num_io_groups));
  // Then we'll copy that file to another file, printing out the records.
  buffered_pipe output_pipe(tile_size, 4);
  
  
  //passing input and output pipe for sorting
  sort_node<ExampleRecordContext,ExampleRecord,ExampleRecordCriterion> test(&ctx,&input_pipe,&output_pipe,tiles_per_group, NULL, "test_sort_node", true);
  
  // start the threads.
  test.start();
 
  //checking the output file
  bool check=sort_node_unit_test(output_pipe,&ctx, num_records);
  
  // wait for the threads to finish.
  test.finish();
  
  stop_clock();

  if (check) {
    std::cout<<"finished sorter: sorting is correct"<<std::endl;
  } else {
    std::cout<<"finished sorter: sorting is not correct"<<std::endl;
    assert(0);
  }

  print_timings("sorted records", num_records);
  print_timings("sorted bytes", total_data);
  
}



int main(int argc, char** argv)
{
  std::cout<<"tiny sort test running..."<<std::endl;
  test_sort(64, 2, 256);
  std::cout<<"small sort test running..."<<std::endl;
  test_sort(64, 2, 512);
  std::cout<<"medium sort test running..."<<std::endl;
  test_sort(128, 1024, 1024*1024);
  std::cout<<"medium2 sort test running..."<<std::endl;
  test_sort(64, 8, 4*1024*1024);
  std::cout<<"medium3 sort test running..."<<std::endl;
  test_sort(128, 1024, 4*1024*1024);
  std::cout<<"medium4 sort test running..."<<std::endl;
  test_sort(4*1024, 1024, 16*1024*1024);
  std::cout<<"medium5 sort test running..."<<std::endl;
  test_sort(4*1024, 1024, 64*1024*1024);
  std::cout<<"large sort test running..."<<std::endl;
  test_sort(4*1024, 1024, 128*1024*1024);
  std::cout<<"huge merge sort test running..."<<std::endl;
  test_sort(4*1024*1024, 2, 128*1024*1024);

  std::cout<<"sort_node unit tests PASS"<<std::endl;

  return 0;
}
