/*
  (*) 2007-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/doc_info_dump.c
*/
#include "index.h"
#include "server.h"

#include "timing.h"
#include "results.h"
#include "femto_internal.h"

#include <assert.h>
#include <time.h>
#include <ctype.h>

#include "query_planning.h"

void usage(char* name)
{
  printf("Usage: %s [options] <index_path>\n", name);
  printf(" where options include:\n");
  exit(-1);
}

int main( int argc, char** argv )
{
  char* index_path;
  int verbose = 0;
  if( argc < 2 ) usage(argv[0]);

  {
    int i = 1;
    while( argv[i][0] == '-' ) {
      // process options
      {
        printf("Unknown option %s\n", argv[i]);
        usage(argv[0]);
      }
      i++;
    }
    index_path = argv[i++];
  }

  {
    error_t err;
    index_locator_t loc;
    femto_server_t srv;
    int64_t num_docs;
    int64_t i;


    // open up the index.
    err = femto_start_server_err(&srv, verbose);
    die_if_err( err );

    err = femto_loc_for_path_err(&srv, index_path, &loc);
    die_if_err(err);

    // step 1: get the number of documents
    {
      header_loc_query_t q;
      setup_header_loc_query(&q, loc, 0, NULL, HDR_LOC_REQUEST_NUM_DOCS, 0, 0);

      err = femto_run_query(&srv, (query_entry_t*) &q);
      die_if_err(err);

      if( q.leaf.entry.err_code ) {
        die_if_err( ERR_MAKE( q.leaf.entry.err_code ) );
      }
      num_docs = q.r.doc_len;
      cleanup_header_loc_query(&q);
    }
    printf("Document info for %"PRIi64" documents :\n", num_docs);
    // step 2: get the doc infos for each document
    for( i = 0; i < num_docs; i++ ) {
      header_loc_query_t q;
      setup_header_loc_query(&q, loc, 0, NULL, HDR_LOC_REQUEST_DOC_INFO, 0, i);

      err = femto_run_query(&srv, (query_entry_t*) &q);
      die_if_err(err);

      if( q.leaf.entry.err_code ) {
        die_if_err( ERR_MAKE( q.leaf.entry.err_code ) );
      }
      printf("%"PRIi64":%"PRIi64": ", i, q.r.doc_info_len);
      for( int64_t j = 0; j < q.r.doc_info_len; j++ ) {
        putchar(q.r.doc_info[j]);
      }
      printf("\n");
      cleanup_header_loc_query(&q);
    }


    //printf("Block requests: %" PRIi64 " faults %" PRIi64 "\n",
    //       sst->stats.block_requests, sst->stats.block_faults);
    femto_stop_server(&srv);
  }

  return 0;
}

