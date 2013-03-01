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

  femto/src/utils_cc/form_runs_node_test.cc
*/
extern "C"{
#include<sys/mman.h>
#include<unistd.h>
#include<sys/types.h>
}

#include "cassert"
#include "pipelining.hh"
#include "sort_context.hh"
#include "compare_record.hh"
#include "example_record.hh"
#include "form_runs_node.hh"
#include "aio_pipe.hh"
#include <algorithm>
#include <vector>

#define DEBUG 1

typedef file_write_pipe<aio_write_pipe> file_write_pipe_t;

bool sort_node_unit_test (read_pipe &input,size_t num_records,sort_context<file_write_pipe_t>* sort_ctx)
{
  tile in_tile;
  size_t got_records=0;

  //start reading the pipe and comparing the nodes
  for( size_t run_idx = 0; run_idx < sort_ctx->tiles_per_run.size(); run_idx++ ) {
    size_t tiles_in_run = sort_ctx->tiles_per_run[run_idx];
    // We should have a run consisting of tiles_in_run tiles.
    ExampleRecord last;
    unsigned char buf[1024];
    bool isfirst=true;
    
    for( size_t tile_idx = 0; tile_idx < tiles_in_run; tile_idx++ ) {
      // read an input tile
      in_tile = input.get_full_tile();
      assert(in_tile.has_tile());
      
      tile& in_t = in_tile;
          
      int len;
      int in_pos;
      unsigned char* in_data;
      
      // Save the number of bytes in the input tile
      len = in_t.len;
      in_data = in_t.data;
              
      ExampleRecord last;
      //cout<<"length of the tile "<<len<<endl;
      for(in_pos=0;in_pos<len;)
      {
        if (isfirst)
        {
          ExampleRecord current;
          current.decode(in_data + in_pos);
          in_pos=in_pos+current.get_record_length();
          current.encode(buf);
          last.decode(buf);
          got_records++;
        }
        else
        {
          ExampleRecord current;
          current.decode(in_data + in_pos);
          if( DEBUG ) {
            std::cout<<"Comparing records a:"<<last.to_string()
              <<" b:"<<current.to_string()<<std::endl;
          }
          if(compare_record(last,current)>0)
          {
            input.close_empty();
            return false;
          }

          in_pos=in_pos+current.get_record_length();
          current.encode(buf);
          last.decode(buf);
          got_records++;
        }
      }
      input.put_empty_tile(in_tile);
    }
  }
  // Check that we're at the end of the input.
  {
    in_tile = input.get_full_tile();
    assert(!in_tile.has_tile());
  }

  input.close_empty();

  if( got_records != num_records ) {
    std::cout << "Dropped records -- got only " << got_records << " of " << num_records << " records" << std::endl;
    return false;
  }
  return true;
}

void test(int tile_size, size_t tiles_per_group, int num_records)
{
  unsigned char key[1024];
  FILE* input = tmpfile();//fopen("/tmp/test_input", "w+");
  FILE* output = tmpfile();//fopen("/tmp/test_output","w+");
  
  ExampleRecordCriterion ctx;

  assert(input);
  assert(output);

  {
    std::cout<<"Preparing input"<<std::endl;
    Ctx::file_write_pipe_t write_pipe(
                         file_pipe_context(NULL, 
                                           fileno(input),
                                           0, PIPE_UNTIL_END,
                                           tile_size,
                                           tiles_per_group));
    pipe_back_inserter<ExampleRecord> writer(&ctx, &write_pipe);
    
    // Make up some records and store them in a file.
    // Make records with random keys and values counting up.
    for( int i = 0; i < num_records; i++ ) {
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
      //cout<<"Writing record:"<<r.to_string(&ctx)<<endl;
      // Write(encode) the record to the file.
      writer.push_back(r); // or *writer = r; ++writer;
    }
    writer.finish();
  }

  rewind(input);
  std::cout<<"input file made"<<std::endl;
  // create the pipes
  Ctx::file_read_pipe_t input_pipe(
                     file_pipe_context(NULL, 
                                       fileno(input),
                                       0, PIPE_UNTIL_END,
                                       tile_size,
                                       tiles_per_group,
                                       ctx.default_num_io_groups));

  Ctx::file_write_pipe_t output_pipe(
                     file_pipe_context(NULL, 
                                       fileno(output),
                                       0, PIPE_UNTIL_END,
                                       tile_size,
                                       tiles_per_group,
                                       ctx.default_num_io_groups));

  sort_context<file_write_pipe_t> sort_ctx;
  form_runs_node<ExampleRecord,ExampleRecordCriterion> test(&ctx,&sort_ctx,&input_pipe,&output_pipe,tiles_per_group);
  
  std::cout<<"running form_runs"<<std::endl;
  test.start();
  test.finish();
   
  std::cout<<"running unit test"<<std::endl; 
  Ctx::file_read_pipe_t check_pipe(
                     file_pipe_context(NULL, 
                                       fileno(output),
                                       0, PIPE_UNTIL_END,
                                       tile_size,
                                       tiles_per_group,
                                       ctx.default_num_io_groups));

  bool check=sort_node_unit_test(check_pipe,&ctx,num_records,&sort_ctx);
  
   
  if (check)
  {
    std::cout<<"sorting is correct"<<std::endl;
  }
  else
  {
    std::cout<<"sorting is not correct"<<std::endl;
    assert(0);
  }
  
}

int main(int argc, char** argv)
{
  
  test(64, 2, 39);
  test(4*1024, 10, 1000);
  test(4*1024, 100, 10000);
  test(4*1024, 1000, 1000000);

  std::cout<<"All tests PASS"<<std::endl;
  return 0;
}
