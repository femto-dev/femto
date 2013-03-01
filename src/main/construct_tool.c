/*
  (*) 2006-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/construct_tool.c
*/
#include "index.h"


#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <assert.h>

#include "timing.h"
#include "bit_funcs.h"
#include "construct.h"

void usage(char* name)
{
  printf("Usage: %s [--multifile] <index_path> <bwt_transform> <info_file> <map_file> \"[parameters]\"\n", name);
  printf(" where [parameters] is:\n");
  print_block_param_usage("     ");
  exit(-1);
}

int main( int argc, char** argv )
{
  char* index_path;
  char* file_name;
  char* map_file;
  char* info_file;
  char** params;
  int nparams;
  int multifile = 0;
  int i;

  if( argc < 4 ) usage(argv[0]);

  i = 1;
  if( 0 == strcmp(argv[i], "--multifile")) {
    multifile = 1;
    i++;
  }
  index_path = argv[i++];
  file_name = argv[i++];
  info_file = argv[i++];
  map_file = argv[i++];
  if( argc > i ) {
    params = & argv[i];
    nparams = argc - i;
  }
  else {
    params = NULL;
    nparams = 0;
  }

  {
    struct stat st;
    int err;
    err = stat(index_path, &st);
    if( ! err ) {
      // check that it's a directory.
      if( S_ISDIR(st.st_mode) ) {
        // things are groovy
      } else {
        printf("Error - index storage directory %s exists but is not a directory\n", index_path);
        exit(-1);
      }
    } else {
      printf("Could not stat storage directory %s\n", index_path);
      printf("Creating storage directory %s\n", index_path);
      // make the storage directory if needed.
      mkdir(index_path, 0777);
    }
  }

  {
    FILE* bwt_f = NULL;
    FILE* map_f = NULL;
    FILE* info_f = NULL;
    error_t err;
    index_block_param_t param;
    construct_statistics_t stats;
    bwt_reader_t bwt;
    bwt_document_map_reader_t map;
    bwt_document_info_reader_t info;

    if( ! multifile ) {
      bwt_f = fopen(file_name, "r");
      if( ! bwt_f ) {
        printf("Could not open file %s\n", file_name);
        exit(-1);
      }
      err = bwt_reader_open(&bwt, bwt_f);
      if( err ) {
        printf("Could not open bwt file %s\n", file_name);
      }
      die_if_err(err);

      map_f = fopen(map_file, "r");
      if( ! map_f ) {
        printf("Could not open file %s\n", map_file);
        exit(-1);
      }
      err = bwt_document_map_reader_open(&map, map_f);
      if( err ) {
        printf("Could not open map file %s\n", map_file);
      }
      die_if_err(err);
    } else {
      err = bwt_reader_open_multifile(&bwt, file_name);
      if( err ) {
        printf("Could not open bwt multifile file %s\n", file_name);
      }
      die_if_err(err);

      err = bwt_document_map_reader_open_multifile(&map, map_file);
      if( err ) {
        printf("Could not open map multifile file %s\n", map_file);
      }
      die_if_err(err);

    }

    info_f = fopen(info_file, "r");
    if( ! info_f ) {
      printf("Could not open file %s\n", info_file);
      exit(-1);
    }
    err = bwt_document_info_reader_open(&info, info_f);
    if( err ) {
      printf("Could not open info file %s\n", info_file);
    }
    die_if_err(err);



    printf("Mark period is %" PRIi64 "; chunk size is %" PRIi64 "\n", bwt_mark_period(&bwt), bwt_document_map_reader_chunk_size(&map));
    printf("Indexing %" PRIi64 " characters in %" PRIi64 " documents\n",
           bwt_num_chars(&bwt), bwt_num_docs(&bwt));

    set_default_param(&param);
    for( int i = 0; i < nparams; i++ ) {
      parse_param(&param, params[i]);
    }

    start_clock();

    memset(&stats, 0, sizeof(construct_statistics_t));
    err = index_documents(&bwt, &map, &info, &param, index_path, &stats);
    die_if_err(err);

    stop_clock();
    print_timings("indexed characters", bwt_num_chars(&bwt));

    bwt_reader_close(&bwt);

    printf("%li bytes in block_info\n", stats.bytes_in_block_info);
    printf("%li bytes in buckets\n", stats.bytes_in_buckets);
    printf("  %li bytes in buckets occs\n", stats.bytes_in_bucket_occs);
    printf("  %li bytes in buckets map\n", stats.bytes_in_bucket_map);
    printf("  %li bytes in buckets wtree\n", stats.bytes_in_bucket_wtree);
    printf("    %li bytes in buckets wtree offsets\n", stats.wtree_stats.offs);
    printf("    %li bytes in buckets wtree bseqs\n", stats.wtree_stats.bseqs);
    printf("      %li bytes in buckets wtree bseqs segments\n", stats.wtree_stats.bseq_stats.segments);
    printf("      %li bytes in buckets wtree bseqs segment_sums\n", stats.wtree_stats.bseq_stats.segment_sums);
    printf("      %li bytes in buckets wtree bseqs group_zeros\n", stats.wtree_stats.bseq_stats.group_zeros);
    printf("      %li bytes in buckets wtree bseqs group_ones\n", stats.wtree_stats.bseq_stats.group_ones);
    printf("      %li bytes in buckets wtree bseqs group_ptrs\n", stats.wtree_stats.bseq_stats.group_ptrs);
    for( int z = 0; z < TRACK_RUNS_DIST; z++ ) {
      printf("      run_count[%i] = %i (%i bits)\n", z, stats.wtree_stats.bseq_stats.run_count[z], 2*log2i(z) + 1);
    }
    printf("      num_runs = %li\n", stats.wtree_stats.bseq_stats.num_runs);
    printf("      max_run = %i\n", stats.wtree_stats.bseq_stats.max_run);
    printf("      segment_bits = %li\n", stats.wtree_stats.bseq_stats.segment_bits);
    printf("      segment_bits_used = %li\n", stats.wtree_stats.bseq_stats.segment_bits_used);
    printf("  %li bytes in buckets mark_table\n", stats.bytes_in_bucket_mark_table);
    printf("  %li bytes in buckets mark_arrays\n", stats.bytes_in_bucket_mark_arrays);

  }
 
  printf("Created index %s\n", index_path);
  return 0;
}

