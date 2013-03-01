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

  femto/src/dcx_cc/suffix_sort_tool.cc
*/
extern "C" {
#include "config.h"
#include "buffer.h"
#include "file_find.h"
#include "timing.h"
}

#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

#include "file_find_node.hh"
#include "dcx.hh"

typedef uint8_t character_t;
typedef uint64_t ss_offset_t;
#define OFFSET_BITS 39
//typedef uint32_t ss_offset_t;


int main( int argc, char** argv ) 
{
  char* output=NULL;
  char* tmpdir=NULL;
  std::vector<std::string> files;

  int iproc = 0;
  int nproc = 1;

  MPI_handler::init_MPI(&argc, &argv);
  nproc = MPI_handler::get_nproc();
  iproc = MPI_handler::get_iproc();

  for( int i = 1; i < argc; i++ ) {
    //if( 0 == strcmp(argv[i], "--verbose") ) {
    //  state.verbose++;
    //} else {
    {
      if( tmpdir == NULL ) tmpdir = argv[i];
      else if( output == NULL ) output = argv[i];
      else files.push_back(argv[i]);
    }
  }

  if( ! tmpdir || ! output || files.size() == 0 ) {
    printf("Usage: %s local-tmp-dir output-file input input ...\n", argv[0]);
    return -1;
  }

  {
    setup_mem_per_bin(0);

    long long int nperbin = dcx_g_mem_per_bin;
    long long int themin = 0;
#ifdef HAVE_MPI_H
    MPI_Allreduce( &nperbin, &themin, 1, MPI_LONG_LONG_INT, MPI_MIN, MPI_COMM_WORLD );
    dcx_g_mem_per_bin = themin;
#endif

    printf("Using %ld MB per bin\n", dcx_g_mem_per_bin / (1024L*1024L) );
  }

  buffered_pipe* to_ssort = NULL;
  file_find_node* reader = NULL;
  int64_t n_int = 0;
  if( iproc == 0 ) {
    size_t input_tile_size = round_tile_size_to_page_multiple(file_pipe_context::default_tile_size, sizeof(character_t));
    to_ssort = new buffered_pipe(input_tile_size, 2);

    reader = new file_find_node(files, to_ssort);

    //printf("Sizing files...\n");
    n_int = reader->count_size();
    assert( num_bits64(n_int) <= OFFSET_BITS );

    //printf("Found %zi bytes\n", (size_t) n_int);

    // Create a suffix sorter.
    // Start the file reader.

    start_clock();
    reader->start();
  }

  long long int nll = n_int;

#ifdef HAVE_MPI_H
  // Communicate n to all the nodes.
  MPI_Bcast( &nll, 1, MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD );
#endif
  n_int = nll;

  {
#ifdef HAVE_MPI_H
    char name[MPI_MAX_PROCESSOR_NAME];
    int len;
    MPI_Get_processor_name(name, &len);
#else
    char name[100];
    strcpy(name, "localhost");
#endif
    printf("%s (rank %i) starting suffix sort of %lli bytes\n",
           name, iproc, nll);
  }


  // Do the suffix sorting.
  int n_bins = DEFAULT_BINS_PER_NODE*nproc;
  std::vector<std::string> sa_files;
  sa_files.resize(n_bins);
  for( int i = 0; i < n_bins; i++ ) {
    char buf[100];
    sprintf(buf, "_%i", i);
    std::string suffix(buf);
    sa_files[i] = output + suffix; 
  }
  character_t max_char = 255;
  ss_offset_t n = n_int;
  suffix_sort<character_t, ss_offset_t, 8, OFFSET_BITS>(n, max_char, to_ssort, &sa_files, tmpdir);

  if( iproc == 0 ) {
    reader->finish();
    stop_clock();
    print_timings("suffix sorting bytes", n);
  }

  if( to_ssort ) delete to_ssort;
  if( reader ) delete reader;

  MPI_handler::finalize_MPI();

  return 0;
}
