/*
  (*) 2010-2015 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/dcx_cc/index_tool.cc
*/

extern "C" {
#include "bwt.h"
#include "index.h"
#include "file_find.h"

#include "buffer_funcs.h"
#include "bwt_writer.h"
#include "bwt_creator.h"
#include "bwt_prepare.h"
#include "timing.h"
#include "index_tool_support.h"
}

#ifdef HAVE_MPI_H
#include <mpi.h>
#endif

#include "pipelining.hh"
#include "dcx.hh"

#define MIN_LIMIT_OPEN_FILES 10000

#define OFFSET_BITS 39
#define DOCUMENT_BITS 39

typedef struct  {
  file_find_state_t ffs;
  prepared_text_t p;
  write_pipe* output;
  pipe_back_inserter<alpha_t>* writer;
} bwt_state_t;

bwt_state_t bwt_globals;
char* cmd_name;

extern "C" {
  error_t prepare_put_char(prepared_text_t* p, alpha_t ch);
  error_t prepare_end_document(prepared_text_t* p);
  void free_prepared_text_extra(prepared_text_t* p);
}

error_t prepare_put_char(prepared_text_t* p, alpha_t ch)
{
  // Append it to the end of the buffered pipe.
  if( EXTRA_CHECKS ) assert(p == &bwt_globals.p);
  bwt_globals.writer->push_back(ch);
  return ERR_NOERR;
}
error_t prepare_end_document(prepared_text_t* p)
{
  return ERR_NOERR;
}
void free_prepared_text_extra(prepared_text_t* p)
{
}

class my_input_reader_node: public pipeline_node
{
 private:
  int64_t total_docs;
 public:
  // all the state is global..
  my_input_reader_node(int64_t total_docs)
    : pipeline_node(NULL,"",false),
      total_docs(total_docs)
  {
  }
 protected:
  virtual void run()
  {
    int rc;
    error_t err;

    for( int64_t doc_num = 0; doc_num < total_docs; doc_num++ ) {
      int64_t doc_len = 0;
      int64_t doc_info_len = 0;
      unsigned char* doc_info = NULL;
      int64_t num_doc_headers = 0;
      int64_t* doc_header_lens = NULL;
      unsigned char** doc_headers = NULL;
      unsigned char* doc_contents = NULL;
      rc = its_get_doc(doc_num,
                       &doc_len,
                       &doc_info_len,
                       &doc_info,
                       &num_doc_headers,
                       &doc_header_lens,
                       &doc_headers,
                       &doc_contents);
      if( rc != 1 ) {
        printf("its_get_doc returned %i\n", rc);
        die_if_err(ERR_IO_STR("its_get_doc failed"));
      } else {
        // We have a document. Add it to the prepared text.
        err = append_file_mem(&bwt_globals.p, doc_len, doc_contents,
                              num_doc_headers, doc_header_lens, doc_headers,
                              doc_info_len, doc_info);
        die_if_err(err);

        rc = its_free_doc(doc_num, doc_len,
                          doc_info_len, doc_info,
                          num_doc_headers, doc_header_lens, doc_headers,
                          doc_contents);
        if( rc < 0 ) {
          printf("its_free_doc returned %i\n", rc);
          die_if_err(ERR_IO_STR("its_free_doc failed"));
        }
      }
    }

    bwt_globals.writer->finish();
  }
};

void index_tool_usage(void)
{
  printf("Usage: %s --tmp <tmp-dir> [--param index_param] [--outdir <index_directory>] [--outfile <index_file>] ", cmd_name);
  its_usage(stdout);
  printf(" <index_directory> -- where to store the index\n");
  printf(" <file_to_index> -- a file or directory containing the document(s) to be indexed; if it is a directory, every file in that directory will be indexed.\n");
  printf(" where index_parameters can be:\n");
  print_block_param_usage("     ");
  printf(" mark_period -- set the marking period. This program will marks every character with offset %% mark_period = 0. Marked characters store full offset information; locating is faster for smaller mark_periods, but the index will be larger. If <mark_period> is 0, no characters are marked. 20 is a reasonable value here, and is the default.\n");
  printf(" chunk_size -- the number of rows in the Burrows-Wheeler transform to group together and save a list of matching documents. Larger chunks mean that queries for very common terms will report faster and the index will be smaller. Smaller chunks allow the chunks to be used for less common terms. 4096 is a reasonable value here, and is the default.\n");
  printf(" block_size -- size of each index data block\n");
  printf(" --sort-memory -- amount of memory to use when sorting, in MB\n");
  printf(" --inmem or --no-inmem -- use (don't use) in memory suffix sorter\n");
  printf("\n");
}

void usage(int argc, char** argv)
{
  index_tool_usage();
  exit(-1);
}

int main( int argc, char** argv ) 
{
  char* tmpdir=NULL;
  char* index_path = NULL;
  char* index_file = NULL;
  index_block_param_t params;
  std::string info_output;
  std::vector<std::string> input_paths;
  error_t err;
  int norlimit = 0;
  int nocore = 0;
  int rc;
  long sort_memory = 0;
  int force_inmem = -1;

  int iproc = 0;
  int nproc = 1;

  cmd_name = argv[0];

  set_default_param(&params);

  MPI_handler::init_MPI(&argc, &argv);
  nproc = MPI_handler::get_nproc();
  iproc = MPI_handler::get_iproc();

  for( int i = 1; i < argc; i++ ) {
    if( 0 == strcmp(argv[i], "--tmp") ) {
      i++;
      tmpdir = argv[i];
      printf("temp dir is %s\n", tmpdir);
    } else if( 0 == strcmp(argv[i], "--outdir") ) {
      i++;
      index_path = argv[i];
      printf("outdir is %s\n", index_path);
    } else if( 0 == strcmp(argv[i], "--outfile") ) {
      i++;
      index_file = argv[i];
      printf("outfile is %s\n", index_file);
    } else if( 0 == strcmp(argv[i], "--outname") ) {
      fprintf(stderr, "Warning, --outname deprecated\n");
      i++;
    } else if( 0 == strcmp(argv[i], "--param") ) {
      i++;
      parse_param(&params, argv[i]);
    } else if( 0 == strcmp(argv[i], "--no-limit-check") ) {
      norlimit = 1;
    } else if( 0 == strcmp(argv[i], "--no-enable-core") ) {
      nocore = 1;
    } else if( 0 == strcmp(argv[i], "--sort-memory") ) {
      i++;
      sort_memory = atol(argv[i]) * 1024*1024;
    } else if( 0 == strcmp(argv[i], "--inmem") ) {
      force_inmem = 1;
    } else if( 0 == strcmp(argv[i], "--no-inmem") ) {
      force_inmem = 0;
    } else if( 0 == strcmp(argv[i], "-h") ||
               0 == strcmp(argv[i], "--help") ) {
      usage(argc, argv);
    } else {
      printf("adding input %s\n", argv[i]);
      input_paths.push_back(argv[i]);
    }
  }

  if( input_paths.size() == 0 ) {
    printf("Missing input\n");
    usage(argc, argv);
  }

  // Set file ulimit to 10,000 or fail horribly.
  if( ! norlimit ) {
    struct rlimit rlim;

    rc = getrlimit(RLIMIT_NOFILE, &rlim);
    if( rc < 0 ) {
      printf("getrlimit returned %i\n", rc);
      die_if_err(ERR_IO_STR("getrlimit failed"));
    }
    
    if( rlim.rlim_cur != RLIM_INFINITY && rlim.rlim_cur < MIN_LIMIT_OPEN_FILES ) {
      // Try setting rlimit to be higher.
      if( rlim.rlim_max != RLIM_INFINITY && rlim.rlim_max < MIN_LIMIT_OPEN_FILES ) {
        rlim.rlim_max = MIN_LIMIT_OPEN_FILES;
      }
      rlim.rlim_cur = MIN_LIMIT_OPEN_FILES;

      rc = setrlimit(RLIMIT_NOFILE, &rlim);
      if( rc < 0 ) {
        printf("setrlimit failed and returned %i\n", rc);
        printf(
"Please increase the hard limit on the number of file descriptors to %i.\n"
"On many systems this can be set in /etc/security/limits.conf,\n"
"by adding a line such as \n"
"*               hard    nofile          %i\n"
"\n"
"This indexing software may use quite a few more than 1024 files for large\n"
"jobs, and so this check is enforced to prevent a long-running job from\n"
"failing partway through. Modern linux supports millions of file descriptors\n"
"and can easily handle a large number of files once the configuration is\n"
"adjusted. By keeping the soft limit at 1024, other programs will still fail\n"
"if they use too many files in an unexpected manner.\n"
"\n"
"This check can be skipped (at your own risk) if you pass --no-limit-check\n"
"\n",
          MIN_LIMIT_OPEN_FILES, MIN_LIMIT_OPEN_FILES);
        die_if_err(ERR_IO_STR("getrlimit failed"));
      }
    }
  }
  if( ! nocore ) {
    struct rlimit rlim;

    rc = getrlimit(RLIMIT_CORE, &rlim);
    if( rc < 0 ) {
      printf("getrlimit returned %i\n", rc);
      die_if_err(ERR_IO_STR("getrlimit failed"));
    }
    
    if( rlim.rlim_cur != RLIM_INFINITY ) {
      // Try setting rlimit to unlimited.
      if( rlim.rlim_max != RLIM_INFINITY ) {
        rlim.rlim_max = RLIM_INFINITY;
      }
      rlim.rlim_cur = RLIM_INFINITY;

      rc = setrlimit(RLIMIT_CORE, &rlim);
      if( rc < 0 ) {
        printf("setrlimit failed and returned %i\n", rc);
        printf(
"Could not set the core file size to unlimited.\n"
"Because indexing runs can take a very long time, it is key\n"
"for debugging to get a core file if there is a crash.\n"
"Please allow core files, or specify --no-enable-core\n"
"to disable this check.\n"
        );
        die_if_err(ERR_IO_STR("getrlimit failed"));
      }
    }
  }


  err = calculate_params(&params);
  die_if_err(err);

  // Now round up bucket_size so that it is a multiple of chunk_size.
  if( params.chunk_size > 0 ) {
    int64_t b_size = params.b_size;
    int64_t c_size = params.chunk_size;
    int64_t blk_size = params.block_size;
    b_size = round_up_to_multiple(b_size, c_size);
    blk_size = round_up_to_multiple(blk_size, b_size);
    params.b_size = b_size;
    params.block_size = blk_size;
  }

  err = calculate_params(&params);
  die_if_err(err);

  printf("mark_period=%i chunk_size=%i bucket_size=%i block_size=%i\n", params.mark_period, params.chunk_size, params.b_size, params.block_size);

  if( ! tmpdir ) {
    printf("Missing --tmp\n");
    usage(argc, argv);
  }

  if( index_file && ! index_path ) {
    std::string tmp_path(tmpdir);
    tmp_path += "/index.femto";
    index_path = strdup(tmp_path.c_str());
    printf("outdir is %s\n", index_path);
  }

  if( ! index_path ) {
    printf("Missing --outdir\n");
    usage(argc, argv);
  }
  if( input_paths.size() == 0 ) {
    printf("Missing input\n");
    usage(argc, argv);
  }

  {
    setup_mem_per_bin(sort_memory);

#ifdef HAVE_MPI_H
    long long int nperbin = dcx_g_mem_per_bin;
    long long int themin = 0;
    MPI_Allreduce( &nperbin, &themin, 1, MPI_LONG_LONG_INT, MPI_MIN, MPI_COMM_WORLD );
    dcx_g_mem_per_bin = themin;
#endif

    printf("Using %ld MB per bin\n", dcx_g_mem_per_bin / (1024L*1024L) );
  }
 
  {
    std::string index_path_s(index_path);
    std::string slash_info("/info");
    info_output = index_path_s + slash_info;
  }

  buffered_pipe* to_ssort = NULL;
  pipe_back_inserter<alpha_t>* writer = NULL;
  my_input_reader_node* reader = NULL;

  int64_t total_n = 0;
  int64_t total_docs = 0;

  err = mkdir_if_needed(tmpdir);
  die_if_err(err);

  if( iproc == 0 ) {
    err = mkdir_if_needed(index_path);
    die_if_err(err);

    // step 1: prepare the text.
    // initialize the structure
    printf("Sizing input\n");
    start_clock();
    err = init_prepared_text(&bwt_globals.p, info_output.c_str());
    die_if_err(err);
    
    {
      const char** args = (const char**) malloc(sizeof(char*)*input_paths.size());
      for( size_t i = 0; i < input_paths.size(); i++ ) {
        args[i] = input_paths[i].c_str();
      }

      rc = its_use_arguments(input_paths.size(), args);
      if( rc < 0 ) {
        printf("its_use_arguments returned %i\n", rc);
        die_if_err(ERR_IO_STR("its_use_arguments failed"));
      }

      free(args);
    }

    for( int64_t doc_num = 0; 1; doc_num++ ) {
      int64_t doc_len = 0;
      int64_t doc_info_len = 0;
      unsigned char* doc_info = NULL;
      int64_t num_doc_headers = 0;
      int64_t* doc_header_lens = NULL;
      rc = its_get_doc_info(doc_num,
                            &doc_len,
                            &doc_info_len,
                            &doc_info,
                            &num_doc_headers,
                            &doc_header_lens);
      if( rc == 0 ) {
        break; // all done!
      } else if ( rc == 1 ) {
        // We have a document.
        err = count_file(&bwt_globals.p, doc_len, num_doc_headers, doc_header_lens, doc_info_len, doc_info);
        die_if_err(err);
        // Now free what we got.
        rc = its_free_doc(doc_num, doc_len,
                          doc_info_len, doc_info,
                          num_doc_headers, doc_header_lens, NULL,
                          NULL);
        if( rc < 0 ) {
          printf("its_free_doc returned %i\n", rc);
          die_if_err(ERR_IO_STR("its_free_doc failed"));
        }
      } else {
        // An error occured.
        printf("its_get_doc_info returned %i\n", rc);
        die_if_err(ERR_IO_STR("its_get_doc_info failed"));
      }
    }

    rc = its_switch_passes();
    if( rc ) {
      printf("its_switch_passes returned %i\n", rc);
      die_if_err(ERR_IO_STR("its_switch_passes failed"));
    }

    err = prepared_finish_count(&bwt_globals.p);
    die_if_err(err);

    // Now we can ask for the total size.
    err = prepared_num_chars(&bwt_globals.p, &total_n);
    die_if_err(err);
    err = prepared_num_docs(&bwt_globals.p, &total_docs);
    die_if_err(err);

    stop_clock();

    print_timings("size_input", total_n);
  }

#ifdef HAVE_MPI_H
  for( int i = 1; i < nproc; i++ ) {
    if( iproc == i ) {
      err = mkdir_if_needed(index_path);
      die_if_err(err);
    }

    // Barrier to:
    //  1) make sure node 0 is done writing the file
    //  2) serialize the file copy in case we copy over
    //      another file (ie when node1 has proc 2,3)
    mpi_barrier( MPI_COMM_WORLD );

    // Copy the info file to all the nodes.
    mpi_copy_file( MPI_COMM_WORLD, info_output, 0,
                                   info_output, i );
  }
#endif

  long long int nll = total_n;
  // Communicate n to all the nodes.
#ifdef HAVE_MPI_H
  MPI_Bcast( &nll, 1, MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD );
#endif
  int64_t n_int = nll;
  // Get other elements of prepared_text set up correctly.
  // Share the number of documents.
  long long int n_docs_ll = total_docs;
#ifdef HAVE_MPI_H
  MPI_Bcast( &n_docs_ll, 1, MPI_LONG_LONG_INT, 0, MPI_COMM_WORLD );
#endif
  int64_t docs_int = n_docs_ll;

#ifdef HAVE_MPI_H
  MPI_Bcast( &params, sizeof(index_block_param_t), MPI_BYTE, 0, MPI_COMM_WORLD );
#endif
 
  // check that n, number of docs match counts in copied file.
  {
    FILE* info_f;
    bwt_document_info_reader_t info;

    info_f = fopen(info_output.c_str(), "r");
    if( ! info_f ) die_if_err(ERR_IO_STR_OBJ("Could not open info", info_output.c_str()));

    err = bwt_document_info_reader_open( &info, info_f );
    die_if_err(err);
    
    total_n = bwt_document_info_reader_num_chars(&info);
    total_docs = bwt_document_info_reader_num_docs(&info);

    err = bwt_document_info_reader_close( &info );
    die_if_err(err);

    fclose(info_f);
  }

  assert(n_int == total_n);
  assert(docs_int == total_docs);
  assert(nll > 0);
  assert( num_bits64(n_int) <= OFFSET_BITS );
  assert( num_bits64(docs_int) <= DOCUMENT_BITS );

  if( iproc == 0 ) {
    // prepare the pipe to the suffix sorter.
    size_t input_tile_size = round_tile_size_to_page_multiple(file_pipe_context::default_tile_size, sizeof(alpha_t));
    to_ssort = new buffered_pipe(input_tile_size, 2);
    writer = new pipe_back_inserter<alpha_t>(to_ssort);

    bwt_globals.output = to_ssort;
    bwt_globals.writer = writer;

    reader = new my_input_reader_node(total_docs);

    start_clock();
    reader->start();
  }


  {
#ifdef HAVE_MPI_H
    char name[MPI_MAX_PROCESSOR_NAME];
    int len;
    MPI_Get_processor_name(name, &len);
#else
    char name[100];
    strcpy(name, "localhost");
#endif
    printf("%s (rank %i) starting suffix sort+bwt of %lli bytes\n",
           name, iproc, nll);
  }
  
  int n_bins = DEFAULT_BINS_PER_NODE*nproc;

  alpha_t min_char = ESCAPE_CODE_EOF;
  alpha_t max_char = ALPHA_SIZE-1;
  alpha_t doc_end_char = ESCAPE_CODE_SEOF;
  std::string tmpdir_s(tmpdir);
  do_bwt<OFFSET_BITS,DOCUMENT_BITS>
        (n_bins,
         n_int, min_char, max_char, doc_end_char,
         to_ssort, info_output, 
         &params,
         tmpdir, index_path, force_inmem);

  if( iproc == 0 ) {
    reader->finish();
    
    its_cleanup();

    stop_clock();

    printf("Read %" PRIi64 " documents \n", total_docs);

    print_timings("indexing bytes", total_n);

    free_prepared_text(&bwt_globals.p);
  }

  if( iproc == 0 && index_file ) {
    printf("Flattening index from %s to %s\n", index_path, index_file);
    err = flatten_index(index_path, index_file);
    die_if_err(err);

    err = delete_storage_dir(index_path);
    die_if_err(err);
    printf("Done flattening.\n");
  }

  // Delete the temporary info_output file, as it's now part of the 
  // index.
  unlink_ifneeded(info_output);

  if( to_ssort ) delete to_ssort;
  if( writer ) delete writer;
  if( reader ) delete reader;

  MPI_handler::finalize_MPI();

  return 0;
}
