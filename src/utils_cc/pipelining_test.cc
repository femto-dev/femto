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

  femto/src/utils_cc/pipelining_test.cc
*/
#include <cassert>
#include "pipelining.hh"
#include "io_pipe.hh"
#include "aio_pipe.hh"
#ifdef HAS_LINUX_LIBAIO
#include "kaio_pipe.hh"
#endif
#include "file_pipe.hh"

#include <cstdio>
#include <cstdlib>

extern "C" {
#include "timing.h"
#include <inttypes.h>
#include <fcntl.h>
}

// Test record..
#include "compare_record.hh"
#include "example_record.hh"

class test_adder_node : public pipeline_node {
  private:
    read_pipe& input;
    write_pipe& output;
    virtual void run() {
      tile in_tile;
      tile out_tile;


      // Check that tile size is a multiple of 8.
      assert((input.get_tile_size() & 7) == 0);
      assert((output.get_tile_size() & 7) == 0);

      try {
        //printf("Test adder main\n");
        while( 1 ) {
          //printf("Test adder reading\n");

          // read an input tile
          in_tile = input.get_full_tile();
          if( ! in_tile.has_tile() ) break;

          // get an output tile
          out_tile = output.get_empty_tile();
          if( ! out_tile.has_tile() ) break;

          tile& in_t = in_tile;
          tile& out_t = out_tile;

          assert(out_t.max >= in_t.max);

          // fill the empty tile!
          {
            int i,j;
            int len;
            uint64_t * in_data;
            uint64_t * out_data;

            len = in_t.len;
            in_data = (uint64_t*) in_t.data;
            out_data = (uint64_t*) out_t.data;
            out_t.len = len;

            //printf("Test processing tile len = %i\n", len);

            for( j=0, i = 0; i < len; i+=sizeof(uint64_t),j++ ) {
              out_data[j] = in_data[j] + 1;
            }
          }

          //printf("Test putting full tile %p\n", out_tile.data);
          // put the full output tile back
          output.put_full_tile(out_t);

          //printf("Test putting empty tile %p\n", in_tile.data);
          // put the empty input tile back
          input.put_empty_tile(in_t);
        }
      } catch ( ... ) {
        input.close_empty();
        output.close_full();
        throw;
      }

      input.close_empty();
      output.close_full();
    }
  public:
    test_adder_node(read_pipe& input, write_pipe& output)
      : input(input), output(output)
    {
    }
};

void check_files(FILE* input, FILE* output, size_t nbytes)
{
  size_t max = nbytes / sizeof(uint64_t);
  assert((nbytes & 7) == 0);
  size_t out_len = file_len(output);

  // Only check the beginning of large files.
  // This way, we do at most 128MB of checking.
  if( max > (16*1024*1024) ) max = 16*1024*1024;

  assert(out_len == nbytes);

  rewind(input);
  rewind(output);
  fflush(input);
  fflush(output);

  // fcntl to unset O_DIRECT if it's on.
  int flags;
  flags = fcntl(fileno(input), F_GETFL);
#ifdef O_DIRECT
  flags &= ~O_DIRECT;
#endif
  fcntl(fileno(input), F_SETFL, flags);

  flags = fcntl(fileno(output), F_GETFL);
#ifdef O_DIRECT
  flags &= ~O_DIRECT;
#endif
  fcntl(fileno(output), F_SETFL, flags);

  // check the output=input+1.
  for( size_t i = 0; i < max; i++ ) {
    uint64_t ic, oc, expect;
    size_t rc; 
    ic = oc = 0;
    rc = fread(&ic, sizeof(uint64_t), 1, input);
    assert(rc == 1);
    fread(&oc, sizeof(uint64_t), 1, output);
    assert(rc == 1);
    expect = ic;
    expect++;
    if( oc != expect ) {
      printf("Difference at offset %zi: got %lx expect %lx\n",
             i, (long) oc, (long) expect);
      assert( oc == expect );
    }
  }
}

void run_on_files(FILE* input, FILE* output, size_t nbytes, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups)
{
  assert(input);
  assert(output);

  printf("Starting file_reader_node/file_writer_node test\n");
  {
    tinfo_t timing;
    buffered_pipe pipe0(tile_size*tiles_per_io_group, num_io_groups);
    buffered_pipe pipe1(tile_size*tiles_per_io_group, num_io_groups);
    file_reader_node reader(input, pipe0);
    test_adder_node test(pipe0, pipe1);
    file_writer_node writer(output, pipe1);

    start_clock_r(&timing);

    // start the threads.
    reader.start();
    test.start();
    writer.start();

    // wait for the threads to finish.
    reader.finish();
    test.finish();
    writer.finish();
    
    stop_clock_r(&timing);

    print_timings_r(&timing, "pipeline adds", nbytes, "");

    printf("Destructing Pipes\n");
  }

  printf("Checking\n");

  // Check the input and output.
  check_files(input, output, nbytes);

  printf("Done running on files\n");
}
void run_on_files_io(FILE* input, FILE* output, size_t nbytes, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups, bool direct)
{
  assert(input);
  assert(output);

  printf("Starting aio_read_pipe/aio_write_pipe test for %zi bytes\n", nbytes);

  {
    tinfo_t timing;
    io_read_pipe reader(file_pipe_context(&timing.io_stats, fileno(input), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));
    io_write_pipe writer(file_pipe_context(&timing.io_stats, fileno(output), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));

    test_adder_node test(reader, writer);

    start_clock_r(&timing);

    // start the threads.
    test.start();

    // wait for the threads to finish.
    test.finish();
    
    stop_clock_r(&timing);

    print_timings_r(&timing, "io pipeline adds", nbytes, "");

    printf("Destructing Pipes\n");
  }

  printf("Checking\n");

  // Check the input and output.
  check_files(input, output, nbytes);

  printf("Done running on files\n");
}


void run_on_files_aio(FILE* input, FILE* output, size_t nbytes, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups, bool direct)
{
  assert(input);
  assert(output);

  printf("Starting aio_read_pipe/aio_write_pipe test for %zi bytes\n", nbytes);

  {
    tinfo_t timing;
    aio_read_pipe reader(file_pipe_context(&timing.io_stats, fileno(input), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));
    aio_write_pipe writer(file_pipe_context(&timing.io_stats, fileno(output), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));

    test_adder_node test(reader, writer);

    start_clock_r(&timing);

    // start the threads.
    test.start();

    // wait for the threads to finish.
    test.finish();
    
    stop_clock_r(&timing);

    print_timings_r(&timing, "aio pipeline adds", nbytes, "");

    printf("Destructing Pipes\n");
  }

  printf("Checking\n");

  // Check the input and output.
  check_files(input, output, nbytes);

  printf("Done running on files\n");
}

#ifdef HAS_LINUX_LIBAIO
void run_on_files_kaio(FILE* input, FILE* output, size_t nbytes, size_t tile_size, size_t tiles_per_io_group, size_t num_io_groups, bool direct)
{
  assert(input);
  assert(output);

  printf("Starting kaio_read_pipe/kaio_write_pipe test for %zi bytes\n", nbytes);

  {
    tinfo_t timing;
    kaio_read_pipe reader(file_pipe_context(&timing.io_stats, fileno(input), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));
    kaio_write_pipe writer(file_pipe_context(&timing.io_stats, fileno(output), 0, nbytes, tile_size, tiles_per_io_group, num_io_groups, true, direct));

    test_adder_node test(reader, writer);

    start_clock_r(&timing);

    // start the threads.
    test.start();

    // wait for the threads to finish.
    test.finish();
    
    stop_clock_r(&timing);

    print_timings_r(&timing, "kaio pipeline adds", nbytes, "");

    printf("Destructing Pipes\n");
  }

  printf("Checking\n");

  // Check the input and output.
  check_files(input, output, nbytes);

  printf("Done running on files\n");
}
#endif

// Returns a pointer to te idx'th record for testing.
// Pointer is valid until next call to this function.
ExampleRecord* get_test_record(size_t idx)
{
  static ExampleRecord r;
  static unsigned char key[1024];
  char* cur = (char*) key;
  size_t key_len = (idx + (idx/7)*(idx % 3)) % 20;
  size_t len=0;
  memset(key, 'X', 1024);

  while( len < key_len ) {
    int printed = snprintf(cur, 1024, "%zx", idx);
    cur += printed;
    len += printed;
  }

  // Finally, set the record.
  ExampleRecord m(key_len, (unsigned char*)(key), idx);
  r = m;
  return &r;
}

IntRecord* get_test_fixed_record(size_t idx)
{
  static IntRecord r;
  size_t key_len = (idx + (idx/7)*(idx % 3)) % 20;
  size_t x = idx + key_len;

  // Finally, set the record.
  r.b = x;
  return &r;
}

// Test fixed-length records with the read/write adapters.
template<typename write_pipe_type,
         typename read_pipe_type>
void test_fixed_records(FILE* input, size_t nbytes, size_t io_group_size, size_t tile_size=4*1024)
{
  file_pipe_context my_context(NULL, fileno(input), 0, PIPE_UNTIL_END, tile_size, io_group_size/(tile_size), 2, true);
  // First, write the file.
  {
    //len_tile_fixer fixer;
    write_pipe_type write_pipe(my_context);
    
    pipe_back_inserter<IntRecord> writer(&write_pipe);
    
    size_t idx = 0;
    size_t bytes_written = 0;
    while( 1 ) {
      IntRecord* r = get_test_fixed_record(idx);
      if( bytes_written + r->get_record_length() > nbytes ) break;
      // Write the record
      // Write(encode) the record to the file.
      writer.push_back(*r); // or *writer = r; ++writer;
      idx++;
      bytes_written += r->get_record_length();
    }

    writer.finish();
  }

  rewind(input);


  // Now, read the file.
  {
    read_pipe_type read_pipe(my_context);
    
    pipe_iterator<IntRecord> reader(&read_pipe);
    pipe_iterator<IntRecord> end;
    
    size_t idx = 0;
    size_t bytes_written = 0;
    while( 1 ) {
      IntRecord* expect = get_test_fixed_record(idx);
      IntRecord read;
      if( bytes_written + expect->get_record_length()> nbytes ) break;
      // Read a record and check that it's the same.
      assert(reader != end); // we shouldn't be at the end yet.
      read = *reader;
      // Check that expect == read
      int cmp = compare_record(*expect, read);
      if( cmp != 0 ) {
        int cmp2 = compare_record(*expect, read);
        printf("Records compared different: %i\n", cmp2);
        printf("Expected:\n");
        std::cout << expect->to_string() << std::endl;
        printf("Read:\n");
        std::cout << read.to_string() << std::endl;
        printf("\n");
      } else {
        //expect->print();
      }
      assert(cmp == 0 );
      bytes_written += expect->get_record_length();
      // move on to the next
      idx++;
      ++reader;
    }

    // Check that we're at the end of the iterator.
    assert(reader == end);

    reader.finish();
  }

  rewind(input);
  // Open up another reader to check that if we only read 1 tile,
  // we get no more tiles than that.
  {
    my_context.len = tile_size;
    read_pipe_type read_pipe(my_context);
    
    pipe_iterator<IntRecord> reader(&read_pipe);
    pipe_iterator<IntRecord> end;
    size_t idx = 0;
    size_t bytes_written = 0;
    while(reader != end){
      IntRecord* expect = get_test_fixed_record(idx);
      IntRecord read;
      // Read a record and check that it's the same.
      read = *reader;
      // Check that expect == read
      int cmp = compare_record(*expect, read);
      if( cmp != 0 ) {
        int cmp2 = compare_record(*expect, read);
        printf("Records compared different: %i\n", cmp2);
        printf("Expected:\n");
        std::cout << expect->to_string() << std::endl;
        printf("Read:\n");
        std::cout << read.to_string() << std::endl;
        printf("\n");
      } else {
        //expect->print();
      }
      assert(cmp == 0 );
      bytes_written += expect->get_record_length();
      // move on to the next
      idx++;
      ++reader;
    }

    // Check that we've read no more than a tile.
    assert(bytes_written <= tile_size);

    reader.finish();
  }

  rewind(input);
}

// Example program that adds 1 to every byte.

void usage(int argc, char** argv)
{
  printf("Usage: %s n-megabytes\n", argv[0]);
  printf("Usage: %s [--kaio|--aio|--fcalls] [--direct|--nodirect] input output [tilesize tiles_per_io_group num_io_groups]\n", argv[0]);
  exit(-1);
}
int main(int argc, char** argv)
{
  char* input_fname;
  char* output_fname;
  FILE* input=NULL;
  FILE* output=NULL;
  size_t tile_size=file_pipe_context::default_tile_size;
  size_t tiles_per_io_group=file_pipe_context::default_tiles_per_io_group;
  size_t num_io_groups=file_pipe_context::default_num_io_groups;

  if( argc == 1 || argc == 2 ) {
    // Do a unit test.
    // First, create two temporary files.
    size_t nmegs = 10;
    size_t nbytes;
    if( argc == 2 ) {
      nmegs = 0;
      sscanf(argv[1], "%zi", &nmegs);
      if( nmegs == 0 ) usage(argc, argv);
    }
    input = tmpfile();

    nbytes = nmegs * 1024 * 1024;

    // Test variable-length records.
    /*printf("Testing variable-length records for 50 byte aio\n");
    test_variable_records<aio_write_pipe,
                          aio_read_pipe >
      (input, 50, 4*1024, 128);

    fclose(input);
    input = tmpfile();
    */
    printf("Testing fixed-length records for 50 byte io\n");
    test_fixed_records<io_write_pipe,
                       io_read_pipe >
      (input, 50, 4*1024, 128);

    fclose(input);
    input = tmpfile();

    printf("Testing fixed-length records for 50 byte aio\n");
    test_fixed_records<aio_write_pipe,
                       aio_read_pipe >
      (input, 50, 4*1024, 128);

    fclose(input);
    input = tmpfile();
    /*
    printf("Testing variable-length records for 5K aio\n");
    test_variable_records<aio_write_pipe,
                          aio_read_pipe >
      (input, 5*1024, 16*1024);

    fclose(input);
    input = tmpfile();*/
    printf("Testing fixed-length records for 5K io\n");
    test_fixed_records<io_write_pipe,
                       io_read_pipe >
      (input, 5*1024, 16*1024);


    fclose(input);
    input = tmpfile();

    printf("Testing fixed-length records for 5K aio\n");
    test_fixed_records<aio_write_pipe,
                       aio_read_pipe >
      (input, 5*1024, 16*1024);


    fclose(input);
    input = tmpfile();
    /*
    printf("Testing variable-length records for 16K aio\n");
    test_variable_records<aio_write_pipe,
                          file_read_pipe<aio_read_pipe> >
      (input, 16*1024, 4*1024);

    fclose(input);
    input = tmpfile(); */
    printf("Testing fixed-length records for 16K io\n");
    test_fixed_records<io_write_pipe,
                       io_read_pipe >
      (input, 16*1024, 4*1024);


    fclose(input);
    input = tmpfile();

    printf("Testing fixed-length records for 16K aio\n");
    test_fixed_records<aio_write_pipe,
                       aio_read_pipe >
      (input, 16*1024, 4*1024);

#ifdef HAS_LINUX_LIBAIO
    fclose(input);
    input = tmpfile();
    /*
    printf("Testing variable-length records for 16K kaio\n");
    test_variable_records<kaio_write_pipe,
                          kaio_read_pipe >
      (input, 16*1024, 4*1024);
    fclose(input);
    input = tmpfile();
    */
    printf("Testing fixed-length records for 16K kaio\n");
    test_fixed_records<kaio_write_pipe,
                       kaio_read_pipe >
      (input, 16*1024, 4*1024);
#endif
    /*
    fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for 256K aio\n");
    test_variable_records<aio_write_pipe,
                          aio_read_pipe >
      (input, 100*256*1024, 256*1024);
      */

    fclose(input);
    input = tmpfile();
    printf("Testing fixed-length records for 256K io\n");
    test_fixed_records<io_write_pipe,
                       io_read_pipe >
      (input, 100*256*1024, 256*1024);

    fclose(input);
    input = tmpfile();
    printf("Testing fixed-length records for 256K aio\n");
    test_fixed_records<aio_write_pipe,
                       aio_read_pipe >
      (input, 100*256*1024, 256*1024);
#ifdef HAS_LINUX_LIBAIO
    /*
    fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for 256K kaio\n");
    test_variable_records<kaio_write_pipe,
                          kaio_read_pipe >
      (input, 10*256*1024, 256*1024);
      */
    fclose(input);
    input = tmpfile();
    printf("Testing fixed-length records for 256K kaio\n");
    test_fixed_records<kaio_write_pipe,
                       kaio_read_pipe >
      (input, 10*256*1024, 256*1024);
#endif
    /*fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for %zi megs aio\n", nmegs);
    test_variable_records<aio_write_pipe,
                          aio_read_pipe >
      (input, nbytes, 1024*1024);*/
    fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for %zi megs io\n", nmegs);
    test_fixed_records<io_write_pipe,
                       io_read_pipe >
      (input, nbytes, 1024*1024);

    fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for %zi megs aio\n", nmegs);
    test_fixed_records<aio_write_pipe,
                       aio_read_pipe >
      (input, nbytes, 1024*1024);

#ifdef HAS_LINUX_LIBAIO
    /*fclose(input);
    input = tmpfile();
    printf("Testing variable-length records for %zi megs kaio\n", nmegs);
    test_variable_records<kaio_write_pipe,
                          kaio_read_pipe >
      (input, nbytes, 1024*1024);
      */
    fclose(input);
    input = tmpfile();
    printf("Testing fixed-length records for %zi megs kaio\n", nmegs);
    test_fixed_records<kaio_write_pipe,
                       kaio_read_pipe >
      (input, nbytes, 1024*1024);
#endif

    // Close and re-open the input.
    fclose(input);
    input = tmpfile();

    printf("Creating input for %zi megs\n", nmegs);
    // fill in the input file.
    for( size_t i = 0,j=0; i < nbytes; i+=8,j++ ) {
      uint64_t num = j;
      int rc;
      rc = fwrite(&num, sizeof(uint64_t), 1, input);
      assert(rc==1);
    }

    rewind(input);
    output = tmpfile();
    run_on_files(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups);
    fclose(output);
    output = NULL;

    rewind(input);
    output = tmpfile();
    run_on_files_io(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, false);
    fclose(output);
    output = NULL;

    rewind(input);
    output = tmpfile();
    run_on_files_io(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, true);
    fclose(output);
    output = NULL;


    rewind(input);
    output = tmpfile();
    run_on_files_aio(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, false);
    fclose(output);
    output = NULL;

    rewind(input);
    output = tmpfile();
    run_on_files_aio(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, true);
    fclose(output);
    output = NULL;


#ifdef HAS_LINUX_LIBAIO
    rewind(input);
    output = tmpfile();
    run_on_files_kaio(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, false);
    fclose(output);
    output = NULL;

    rewind(input);
    output = tmpfile();
    run_on_files_kaio(input, output, nbytes, tile_size, tiles_per_io_group, num_io_groups, true);
    fclose(output);
    output = NULL;

#endif

  } else {
    int do_kaio=0;
    int do_aio=0;
    int do_io=0;
    int do_fcalls=0;
    bool use_o_direct = false;
    int i;

#ifdef O_DIRECT
    use_o_direct = (O_DIRECT>0); // if we have O_DIRECT, use it.
#endif

    if( argc < 4 ) {
      usage(argc, argv);
    }

    i = 1;
    if( 0 == strcmp(argv[i], "--kaio") ) {
      do_kaio=1;
      i++;
    } else if( 0 == strcmp(argv[i], "--aio") ) {
      do_aio=1;
      i++;
    } else if( 0 == strcmp(argv[i], "--io") ) {
      do_io=1;
      i++;
    } else if( 0 == strcmp(argv[i], "--fcalls") ) {
      do_fcalls=1;
      i++;
    }
    if( 0 == strcmp(argv[i], "--direct") ) {
      use_o_direct = true;
      i++;
    } else if( 0 == strcmp(argv[i], "--nodirect") ) {
      use_o_direct = false;
      i++;
    }

    input_fname = argv[i++];
    output_fname = argv[i++];
    if( argc >= i+1 ) tile_size = atoi(argv[i++]);
    if( argc >= i+1 ) tiles_per_io_group = atoi(argv[i++]);
    if( argc >= i+1 ) num_io_groups = atoi(argv[i++]);

    printf("Tile size %i bytes tiles_per_io_group %i num_io_groups %i\n", (int) tile_size, (int) tiles_per_io_group, (int) num_io_groups);
    input = fopen(input_fname, "r");
    output = fopen(output_fname, "w+");

    //fcntl(fileno(input), F_SETFL, O_DIRECT|fcntl(fileno(input), F_GETFL));
    //fcntl(fileno(output), F_SETFL, O_DIRECT|fcntl(fileno(output), F_GETFL));

    if( do_io ) {
      rewind(output);
      fflush(output);
      ftruncate(fileno(output), 0);
      run_on_files_io(input, output, file_len(input), tile_size, tiles_per_io_group, num_io_groups, use_o_direct);
    }


    if( do_aio ) {
      rewind(output);
      fflush(output);
      ftruncate(fileno(output), 0);
      run_on_files_aio(input, output, file_len(input), tile_size, tiles_per_io_group, num_io_groups, use_o_direct);
    }

    if( do_kaio ) {
#ifdef HAS_LINUX_LIBAIO
      rewind(output);
      fflush(output);
      ftruncate(fileno(output), 0);
      run_on_files_kaio(input, output, file_len(input), tile_size, tiles_per_io_group, num_io_groups, use_o_direct);
#endif
    }

    if( do_fcalls ) {
      rewind(output);
      fflush(output);
      ftruncate(fileno(output), 0);
      run_on_files(input, output, file_len(input), tile_size, tiles_per_io_group, num_io_groups);
    }

  }

  if( input ) fclose(input);
  if( output ) fclose(output);

  printf("pipeline test PASSED\n");
  return 0;

}
