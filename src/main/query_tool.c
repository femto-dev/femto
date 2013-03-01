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

  femto/src/main/query_tool.c
*/
#include "index.h"
#include "server.h"
#include "femto_internal.h"

#include "timing.h"

#include <assert.h>
#include <time.h>

void usage(char* name)
{
  printf("Usage: %s <index_path> [-count|-locate|-singlecount|-singlelocate|-chunklocate|-documents] [max_to_locate]\n", name);
  printf("Takes in stdin queries to run in Pizza&Chile format\n");
  exit(-1);
}

int nqueries;
int* qlen;
alpha_t** queries;
int64_t* count;
int64_t* count_first;
int64_t* count_last;
int* noccs;
int64_t** offsets;

// reads queries in the Pizza&Chile format from stdin.
void read_queries(FILE* f)
{
  char hdr[1024];
  int read;
  int number, length;

  // first, read the line beginning with #
  fgets(hdr, 1024, f);

  // pass the "# "
  if(hdr[0]!='#') {
    fprintf(stderr, "Bad query file format; missing #\n");
    assert(0);
  }


  read = sscanf(hdr, "# number=%i length=%i ", &number, &length);
  if( read != 2 ) {
    fprintf(stderr, "Bad query file format in header line\n");
    assert(0);
  }

  // allocate memory.
  nqueries = number;
  queries = malloc(sizeof(alpha_t*)*number);
  qlen = malloc(sizeof(int)*number);
  noccs = malloc(sizeof(int)*number);
  count = malloc(sizeof(int64_t)*number);
  count_first = malloc(sizeof(int64_t)*number);
  count_last = malloc(sizeof(int64_t)*number);
  offsets = malloc(sizeof(int64_t*)*number); 

  assert( queries && noccs && offsets );

  // allocate the queries structure
  for( int i = 0; i < number; i++ ) {
    queries[i] = malloc(1+sizeof(alpha_t)*length);
    assert(queries[i]);
    memset(queries[i], 0, 1+sizeof(alpha_t)*length);
  }

  // read in the file.
  for( int i = 0; i < number; i++ ) {
    for( int j = 0; j < length; j++ ) {
      read = fgetc(f);
      assert( read != EOF );
      queries[i][j] = CHARACTER_OFFSET + read;
    }
    qlen[i] = length;
  }
}

int main( int argc, char** argv )
{
  char* index_path;
  char* todo;
  int max = INT32_MAX;
  int64_t chunk_size = 1000;
  if( argc != 3 && argc != 4) usage(argv[0]);

  index_path = argv[1];
  todo = argv[2];
  if( argc==4 ) chunk_size = max = atoi(argv[3]);

  {
    error_t err;
    index_locator_t loc;
    femto_server_t srv;
    int64_t nresults = 0;

    printf("Reading queries\n");
    read_queries(stdin);

    // start the server.
    err = femto_start_server_err(&srv, 0);
    die_if_err( err );

    // Get the locator for the index we're using.
    err = femto_loc_for_path_err(&srv, index_path, &loc);
    die_if_err( err );

    if( 0 == strcmp(todo, "-singlecount") ) {
      printf("Starting single count\n");
      start_clock();
      for( int i = 0; i < nqueries; i++ ) {
        err = parallel_count(&srv, loc, 1, &qlen[i], &queries[i], &count[i], NULL);
        die_if_err(err);
      }
      stop_clock();
      print_timings("single count queries", nqueries);
    } else if( 0 == strcmp(todo, "-count") ) {
      printf("Starting count\n");
      start_clock();

      err = parallel_count(&srv, loc, nqueries, qlen, queries, count, NULL);
      die_if_err(err);
      stop_clock();
      nresults = 0;
      for( int i = 0; i < nqueries; i++ ) {
        nresults += count[i];
        //printf("pat#%i %i occs\n", i, (int) count[i]);
      }
      print_timings("parallel count queries", nqueries);
      printf("Counted %" PRIi64 " results\n", nresults);
    } else if( 0 == strcmp(todo, "-singlelocate") ) {
      printf("Single locate\n");
      start_clock();
      for( int i = 0; i < nqueries; i++ ) {
        offsets[i] = NULL;
        err = serial_locate(&srv, loc, 1, &qlen[i], &queries[i], max, &noccs[i], &offsets[i]);
        //err = parallel_locate(&srv, loc, 1, &qlen[i], &queries[i], max, &noccs[i], &offsets[i]);
        die_if_err(err);
        free(offsets[i]);
        offsets[i] = NULL;
      }
      stop_clock();
      nresults = 0;
      for( int i = 0; i < nqueries; i++ ) {
        nresults += noccs[i];
      }
      print_timings("single locate queries", nqueries);
      print_timings("single locate results", nresults);
    } else if( 0 == strcmp(todo, "-locate") ) {
      start_clock();
      err = parallel_locate(&srv, loc, nqueries, qlen, queries, max, noccs, offsets);
      die_if_err(err);
      stop_clock();

      nresults = 0;
      for( int i = 0; i < nqueries; i++ ) {
        nresults += noccs[i];
        //printf("pat#%i %i occs\n", i, (int) noccs[i]);
        free(offsets[i]);
      }
      print_timings("parallel locate queries", nqueries);
      print_timings("parallel locate results", nresults);
    } else if( 0 == strcmp(todo, "-chunklocate") ) {
      int64_t first, last, current, end;

      printf("Chunk locate\n");
      start_clock();
      for( int i = 0; i < nqueries; i++ ) {
        //printf("Chunk locate - count\n");
        // first do the count.
        err = parallel_count(&srv, loc, 1, &qlen[i], &queries[i], &count_first[i], &count_last[i]);
        die_if_err(err);

        // now do the locate in chunks.
        first = count_first[i];
        last = count_last[i];
        noccs[i] = 1 + last - first;

        for( current = first; current <= last; current += chunk_size)
        {
          end = current + chunk_size - 1;
          if( end > last ) end = last;
          offsets[i] = malloc(sizeof(int64_t) * (1+end-current));
          //printf("Chunk locate: range %" PRIi64 " - %" PRIi64 "\n", current, end);
          err = parallel_locate_range(&srv, loc, current, end, offsets[i]);
          die_if_err(err);
          free(offsets[i]);
          offsets[i] = NULL;
        }

      }
      stop_clock();
      nresults = 0;
      for( int i = 0; i < nqueries; i++ ) {
        nresults += noccs[i];
      }
      print_timings("chunk locate queries", nqueries);
      print_timings("chunk locate results", nresults);
    } else if ( 0 == strcmp(todo, "-paralleldocuments") ) {
      parallel_query_t ctx;

      printf("Parallel document locate\n");
      start_clock();

      err = setup_parallel_query(&ctx, NULL, loc, sizeof(string_results_query_t), nqueries);
      die_if_err(err);
      for( int i = 0; i < nqueries; i++ ) {
        err = setup_string_results_query((string_results_query_t*) ith_query(&ctx, i),
                  NULL, loc, chunk_size, RESULT_TYPE_DOCUMENTS,
                  qlen[i], queries[i]);
        die_if_err(err);
      }

      err = femto_run_query(&srv, (query_entry_t*) &ctx);
      die_if_err(err);

      nresults = 0;
      for( int i = 0; i < nqueries; i++ ) {
        string_results_query_t* q = (string_results_query_t*) ith_query(&ctx, i);
        nresults += q->results.results.num_documents;
        results_destroy(&q->results.results);
      }

      stop_clock();
      print_timings("parallel document locate queries", nqueries);
      print_timings("parallel document locate results", nresults);
    } else if ( 0 == strcmp(todo, "-documents") ) {
      string_results_query_t q;

      printf("Document locate\n");
      start_clock();

      for( int i = 0; i < nqueries; i++ ) {
        err = setup_string_results_query(&q,
                  NULL, loc, chunk_size, RESULT_TYPE_DOCUMENTS,
                  qlen[i], queries[i]);
        die_if_err(err);

        err = femto_run_query(&srv, (query_entry_t*) &q);
        die_if_err(err);

        nresults += q.results.results.num_documents;
        results_destroy(&q.results.results);
      }

      stop_clock();
      print_timings("document locate queries", nqueries);
      print_timings("document locate results", nresults);

    }

    /*printf("Block requests: %" PRIi64 " faults %" PRIi64 "\n",
           uu->stats.block_requests, sst->stats.block_faults);
     */

    femto_stop_server(&srv);
  }

  if( 0 ) {
    int i,j;
    if( noccs ) {
      for( i = 0; i < nqueries; i++ ) {
        if( noccs[i] ) {
          for( j = 0; j < noccs[i] && j < max; j++ ) {
            printf("%i %i/%i %li\n", i, j, noccs[i], (long int) offsets[i][j]);
          }
        }
      }
    }
  }

  return 0;
}

