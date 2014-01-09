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

  femto/src/main/search_tool.c
*/
#include "index.h"
#include "server.h"
#include "femto_internal.h"

#include "timing.h"
#include "results.h"

#include <assert.h>
#include <time.h>
#include <ctype.h>

#include "query_planning.h"

void usage(char* name)
{
  printf("Usage: %s [options] <index_path> [<index_path>...] <pattern>\n", name);
  printf(" where options include:\n");
  printf(" -v or --verbose  print extra verbose output\n");
  printf(" --max_results <number> set the maximum number of results\n");
  printf(" --offsets request document offsets\n");
  printf(" --count Ask for only the number of results\n");
  printf(" --matches Show strings matching approximate search and/or regular expression\n");
  printf(" --output <filename> output query results to file instead of stdout\n");
  printf(" --null seperate output lines with 0 bytes instead of newlines\n");
  printf(" --icase make the search case-insensitive\n");
  exit(-1);
}

error_t print_matches(FILE* out, 
                      femto_server_t* srv, index_locator_t loc,
                      results_t* results,
                      const char* prefix, char sep)
{
  int64_t document, last_document;
  int64_t offset;
  results_reader_t reader;
  parallel_query_t ctx;
  error_t err;
  int num = 0;
  long* lens = NULL;
  unsigned char **infos = NULL;
  int i,j;

  err = setup_get_doc_info_results(&ctx, NULL, loc, results);
  if( err ) return err;

  err = femto_run_query(srv, (query_entry_t*) &ctx);
  if( err ) return err;

  get_doc_info_results(&ctx, &num, &lens, &infos);
  cleanup_get_doc_info_results(&ctx);

  last_document = 0;
  err = results_reader_create(&reader, results);
  if( err ) return err;
  i = 0;
  j = 0;
  while( results_reader_next(&reader, &document, &offset) ) {
    // is it a new document?
    if( i == 0 || last_document != document ) {
      last_document = document;
      if( i != 0 ) fprintf(out,"%c%s", sep, prefix);
      for( long k = 0; k < lens[j]; k++ ) {
        fprintf(out, "%c", infos[j][k]);
      }
      if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
        fprintf(out, "%c\t", sep);
      }
      j++;
    }
    if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
      fprintf(out, " %" PRIi64, offset);
    }
    //printf("Match document %" PRIi64 " at offset %" PRIi64 "\n", document, offset);
    i++;
  }

  if( i != 0 ) fprintf(out, "%c", sep);

  for( i = 0; i < num; i++ ) {
    free(infos[i]);
  }
  free(infos);
  free(lens);

  return ERR_NOERR;
}

int main( int argc, char** argv )
{
  char** index_paths = NULL;
  int num_indexes = 0;
  char* pattern = NULL;
  char* output_fname = NULL;
  FILE* out = stdout;
  int verbose = 0;
  int64_t chunk_size = 1024*1024; // 1MB chunks...
  int offsets = 0;
  int count = 0;
  int matches = 0;
  char sep = '\n';
  int icase = 0;

  index_paths = malloc(argc*sizeof(char*));

  {
    int i = 1;
    while( i < argc ) {
      // process options
      if( 0 == strcmp(argv[i], "-v") ||
          0 == strcmp(argv[i], "--verbose") ) {
        verbose++;
      } else if( 0 == strcmp(argv[i], "--max_results") ) {
        // set the chunk size
        i++; // pass the -c
        sscanf(argv[i], "%" SCNi64, &chunk_size);
      } else if( 0 == strcmp(argv[i], "--offsets") ) {
        offsets = 1;
      } else if( 0 == strcmp(argv[i], "--count") ) {
        count = 1;
      } else if( 0 == strcmp(argv[i], "--matches") ) {
        matches = 1;
        count = 1;
      } else if( 0 == strcmp(argv[i], "--output") ) {
        i++; // pass --output
        output_fname = argv[i];
      } else if( 0 == strcmp(argv[i], "--null") ) {
        sep = '\0';
      } else if( 0 == strcmp(argv[i], "--icase") ) {
        icase = 1;
      } else if( argv[i][0] == '-' ) {
        printf("Unknown option %s\n", argv[i]);
        usage(argv[0]);
      } else {
        index_paths[num_indexes++] = argv[i];
      }

      i++;
    }
  }

  if( num_indexes < 2 ) usage(argv[0]);

  // pattern takes the last index path.
  num_indexes--;
  pattern = index_paths[num_indexes];

  if( output_fname ) {
    out = fopen(output_fname, "w");
    if( ! out ) {
      perror("Could not fopen");
      fprintf(stderr, "Could not open output filename '%s' for writing\n", output_fname);
      exit(-1);
    }
  }

  {
    error_t err;
    index_locator_t *locs;
    femto_request_t *reqs;
    femto_server_t srv;
    struct ast_node* ast;
    int64_t total_matches;
    result_type_t type = (offsets)?
                           (RESULT_TYPE_DOC_OFFSETS):
                             (RESULT_TYPE_DOCUMENTS);

    locs = malloc(num_indexes*sizeof(index_locator_t));
    reqs = malloc(num_indexes*sizeof(femto_request_t));

    assert( locs && reqs );

    ast = parse_string(strlen(pattern), pattern);
    if( ! ast ) {
      fprintf(stderr, "Could not parse pattern\n");
      exit(-1);
    }
   
    //Remove useless extra tokens from the Regular Expression
    streamline_query(ast);

    if( icase ) {
      icase_ast(&ast);
    }

    if( verbose ) {
      printf("Query pattern is: %s\n", pattern);
      print_ast_node(stdout, ast, 0);
    }

    // start the server.
    err = femto_start_server_err(&srv, verbose);
    die_if_err( err );

    // Get the locators for the indexes we're using.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_loc_for_path_err(&srv, index_paths[i], &locs[i]);
      die_if_err( err );
    }

    // Create the requests.
    for( int i = 0; i < num_indexes; i++ ) {
      if( count || matches ) {
        process_entry_t* q = NULL;
        err = create_generic_ast_count_query(&q, NULL, locs[i], ast);
        die_if_err(err);
        err = femto_setup_request_err(&reqs[i], (query_entry_t*) q, 1);
        die_if_err(err);
      } else {
        results_query_t* q = NULL;
        err = create_generic_ast_query(&q, NULL, locs[i], chunk_size, type, ast);
        die_if_err(err);
        err = femto_setup_request_err(&reqs[i], (query_entry_t*) q, 1);
        die_if_err(err);
      }
    }

    if( verbose ) start_clock();

    // Start all of the queries.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_begin_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }

    // Wait for all of the queries to finish.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_wait_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }


    total_matches = 0;
    for( int i = 0; i < num_indexes; i++ ) {
      query_entry_t* q = reqs[i].query;
      int64_t num_matches = 0;

      if( verbose ) printf("Results from %s\n", index_paths[i]);

      // Print out the results
      if( count || matches ) {
        if( q->type == QUERY_TYPE_REGEXP ) {
          regexp_query_t* rq = (regexp_query_t*) q;
          // Go through the results...
          for( int i = 0; i < rq->results.num_results; i++ ) {
            regexp_result_t result = rq->results.results[i];
            if( result.last >= result.first &&
                result.first >= 0 ) {
              if( matches ) {
                fprintf(out, "% 4" PRIi64 " [%" PRIi64 ",%" PRIi64"] \"", result.last - result.first + 1, result.first, result.last );
                fprint_alpha(out, result.match_len, result.match);
                fprintf(out, "\"%c", sep);
              }
              num_matches += result.last - result.first + 1;
            }

          }
        } else if( q->type == QUERY_TYPE_STRING ) {
          string_query_t* sq = (string_query_t*) q;
          if( sq->last >= sq->first &&
              sq->first >= 0 ) {
            if( matches ) {
              fprintf(out, "% 4" PRIi64 " [%" PRIi64 ",%" PRIi64 "] \"", sq->last - sq->first + 1, sq->first, sq->last);
              fprint_alpha(out, sq->plen, sq->pat);
              fprintf(out, "\"%c", sep);
            }
            num_matches += sq->last - sq->first + 1;
          }
        }

        if( verbose ) printf("% 4" PRIi64 " index matches\n", num_matches);
        total_matches += num_matches;
      } else {
        results_query_t* rq = (results_query_t*) q;

        print_matches(out, &srv, locs[i], &rq->results, "", sep);
      }


      femto_cleanup_request( & reqs[i] );
    }

    if( verbose ) stop_clock();

    if( count || matches ) {
      if( verbose ) printf("\n");
      fprintf(out, "% 4" PRIi64 " total matches%c", total_matches, sep);
    }

    free_ast_node(ast);

    //printf("Block requests: %" PRIi64 " faults %" PRIi64 "\n",
    //       sst->stats.block_requests, sst->stats.block_faults);
    femto_stop_server(&srv);

    free(locs);
    free(reqs);
  }

  if( verbose ) {
    print_timings("indexes queried", num_indexes);
  }

  if(out != stdout) fclose(out);

  free(index_paths);

  return 0;
}

