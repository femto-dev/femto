/*
  (*) 2013-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/common_tool.c
*/
#include "index.h"
#include "server.h"
#include "femto_internal.h"
#include "json.h"

#include "timing.h"
#include "results.h"

#include <assert.h>
#include <time.h>
#include <ctype.h>

#include "query_planning.h"

void usage(char* name)
{
  printf("Usage: %s [options] <index_path>\n", name);
  printf(" finds the most common substrings in an index using breadth-first-search\n");
  printf(" where options include:\n");
  printf(" -v or --verbose  print extra verbose output\n");
  printf(" --output <filename>  output results to file\n");
  printf(" --suffix <string> required a suffix (an efficient pre-filter)\n");
  //printf(" --prefix <string> required a prefix (an inefficient post-filter)\n");
  printf(" --max_length <number> set the maximum found pattern length\n");
  printf("              (default 1024)\n");
  printf(" --max_results <number> set the maximum number of results\n");
  printf("               (default 1 million)\n");
  printf(" --max_consider <number> set the maximum number of strings to consider\n");
  printf("               (default 1 million)\n");
  printf(" --min_occurences <number> set minimum number of occurences for returned pattern\n");
  printf("               (default 0)\n");
  printf(" --min_occurences <percent>%% set minimum number of occurences for returned pattern as a percentage of the number of matches to the suffix\n");
  printf("               (default 0)\n");
  exit(-1);
}

int main( int argc, char** argv )
{
  char** index_paths = NULL;
  int num_indexes = 0;
  char* prefix = "''";
  char* suffix = "''";
  char* output_fname = NULL;
  FILE* out = stdout;
  int verbose = 0;
  int min_length = 1;
  int max_length = 100*1024;
  int64_t max_results = 1000*1000;
  int64_t max_consider = 1000*1000;
  int64_t min_occurences = -1;
  double min_occurences_percent = -1.0;
  int64_t max_occurences = -1;
  double max_occurences_percent = -1.0;
  int64_t min_occs = -1;
  int64_t max_occs = -1;
  int64_t nfound = 0;

  index_paths = malloc(argc*sizeof(char*));
  {
    int i = 1;
    while( i < argc ) {
      // process options
      if( 0 == strcmp(argv[i], "-v") ||
          0 == strcmp(argv[i], "--verbose") ) {
        verbose++;
      } else if( 0 == strcmp(argv[i], "--suffix") ) {
        i++;
        suffix = argv[i];
      } else if( 0 == strcmp(argv[i], "--prefix") ) {
        i++;
        prefix = argv[i];
      } else if( 0 == strcmp(argv[i], "--max_results") ) {
        i++;
        sscanf(argv[i], "%" SCNi64, &max_results);
      } else if( 0 == strcmp(argv[i], "--max_consider") ) {
        i++;
        sscanf(argv[i], "%" SCNi64, &max_consider);
      } else if( 0 == strcmp(argv[i], "--min_length") ) {
        i++;
        sscanf(argv[i], "%i", &min_length);
      } else if( 0 == strcmp(argv[i], "--max_length") ) {
        i++;
        sscanf(argv[i], "%i", &max_length);
      } else if( 0 == strcmp(argv[i], "--min_occurences") ) {
        i++;
        if( strstr(argv[i], "%") ) {
          // do it in percent.
          sscanf(argv[i], "%lf", &min_occurences_percent);
          if( min_occurences_percent < 0 ) min_occurences_percent = 0;
          if( min_occurences_percent > 100 ) min_occurences_percent = 100;
        } else {
          sscanf(argv[i], "%" SCNi64, &min_occurences);
          if( min_occurences < 0 ) min_occurences = 0;
        }
      } else if( 0 == strcmp(argv[i], "--max_occurences") ) {
        i++;
        if( strstr(argv[i], "%") ) {
          // do it in percent.
          sscanf(argv[i], "%lf", &max_occurences_percent);
          if( max_occurences_percent < 0 ) max_occurences_percent = 0;
          if( max_occurences_percent > 100 ) max_occurences_percent = 100;
        } else {
          sscanf(argv[i], "%" SCNi64, &max_occurences);
          if( max_occurences < 0 ) max_occurences = 0;
        }
      } else if( 0 == strcmp(argv[i], "--output") ) {
        i++; // pass --output
        output_fname = argv[i];
      } else if( argv[i][0] == '-' ) {
        printf("Unknown option %s\n", argv[i]);
        usage(argv[0]);
      } else {
        index_paths[num_indexes++] = argv[i];
      }
      i++;
    }
  }

  if( num_indexes != 1 ) usage(argv[0]);

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
    struct ast_node* prefix_ast;
    struct ast_node* suffix_ast;
    string_t prefix_string = {0,NULL};
    struct row_range {
      int64_t first;
      int64_t last;
    };
    struct per_depth {
      int pattern_length; // all patterns at this depth have this length
      int nrows; // number of ranges of rows == num strings
      int nchars; // number of characters in all strings
      alpha_t* strings;
      struct row_range* rows; 
    };
    struct per_depth new_depth = {0, 0, 0, NULL, NULL};
    int n_common_per_depth = 0;
    struct per_depth* common_per_depth = NULL;
    struct row_range suffix_row_range = {0,-1};
    int stopped_adding = 0;

    locs = malloc(num_indexes*sizeof(index_locator_t));
    reqs = malloc(num_indexes*sizeof(femto_request_t));

    assert( locs && reqs );

    prefix_ast = parse_string(strlen(prefix), prefix);
    if( ! prefix_ast ) {
      fprintf(stderr, "Could not parse prefix pattern\n");
      exit(-1);
    }

    suffix_ast = parse_string(strlen(suffix), suffix);
    if( ! suffix_ast ) {
      fprintf(stderr, "Could not parse suffix pattern\n");
      exit(-1);
    }
    //Convert to string queries when there are no regexp alternatives
    err = simplify_query(&prefix_ast);
    die_if_err(err);
    //Convert to string queries when there are no regexp alternatives
    err = simplify_query(&suffix_ast);
    die_if_err(err);

    // Assert that both queries are string type.
    if( prefix_ast->type != AST_NODE_STRING ) {
      fprintf(stderr, "Regular expression and Boolean terms not supported in prefix pattern\n");
      exit(-1);
    }
    if( suffix_ast->type != AST_NODE_STRING ) {
      fprintf(stderr, "Regular expression and Boolean terms not supported in suffix pattern\n");
      exit(-1);
    }

    if( verbose ) {
      printf("Suffix pattern is: %s\n", suffix);
      print_ast_node(stdout, suffix_ast, 0);
      printf("Prefix pattern is: %s\n", prefix);
      print_ast_node(stdout, prefix_ast, 0);
    }

    // Now, extract the pattern for the prefix since we're going
    // to backward search for it ourselves.
    if( prefix_ast->type == AST_NODE_STRING ) {
      struct string_node* s = (struct string_node*) prefix_ast;
      prefix_string = dup_string(s->string);
      if( verbose ) {
        printf("Extracted prefix pattern: ");
        fprint_alpha(out, prefix_string.len, prefix_string.chars);
        printf("\n");
      }
    }

    // start the server.
    err = femto_start_server_err(&srv, verbose);
    die_if_err( err );

    // Get the locators for the indexes we're using.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_loc_for_path_err(&srv, index_paths[i], &locs[i]);
      die_if_err( err );
    }

   /*
    want to do breadth-first searching in order to more gracefully
      handle the limits in max # results.

    while num strings in set < maximum results
      for all strings in set
        if # matches >= threshold
          add searches for all previous characters
          if num strings in set >= maximum results, stop adding
        else
          output string, count
      if we didn't stop adding, do searches

    */

    // Search for the suffix to get our starting rows
    for( int i = 0; i < num_indexes; i++ ) {
      process_entry_t* q = NULL;
      err = create_generic_ast_count_query(&q, NULL, locs[i], suffix_ast);
      die_if_err(err);
      err = femto_setup_request_err(&reqs[i], (query_entry_t*) q, 1);
      die_if_err(err);
    }

    if( verbose ) start_clock();

    // Start all of the initial queries.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_begin_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }

    // Wait for all of the initial queries.
    for( int i = 0; i < num_indexes; i++ ) {
      err = femto_wait_request_err(&srv, &reqs[i]);
      die_if_err(err);
    }

    // Set suffix_row_range.
    for( int i = 0; i < num_indexes; i++ ) {
      query_entry_t* q = reqs[i].query;
      if( q->type == QUERY_TYPE_STRING ) {
        string_query_t* sq = (string_query_t*) q;
        suffix_row_range.first = sq->first;
        suffix_row_range.last = sq->last;
      } else assert(0);
      femto_cleanup_request( & reqs[i] );
    }

    if( verbose > 1 ) {
      fprintf(out, "suffix matches % 4" PRIi64 " rows [%" PRIi64",%" PRIi64"]\n", 1 + suffix_row_range.last - suffix_row_range.first, suffix_row_range.first, suffix_row_range.last);
    }

    if( suffix_row_range.first > suffix_row_range.last ) {
      goto done;
    }

    // Set min_occs to max(min_occs_percent, min_occs)
    if( min_occurences_percent >= 0 ) {
      min_occs = (suffix_row_range.last-suffix_row_range.first+1) *
                           min_occurences_percent / 100.0;
      if( min_occurences >= 0 && min_occurences > min_occs )
        min_occs = min_occurences;
    } else if( min_occurences >= 0 ) {
      min_occs = min_occurences;
    } else {
      min_occs = -1;
    }

    // Set max_occs to min(max_occs_percent, max_occs)
    if( max_occurences_percent >= 0 ) {
      max_occs = (suffix_row_range.last-suffix_row_range.first+1) *
                           max_occurences_percent / 100.0;
      if( max_occurences >= 0 && max_occurences < max_occs )
        max_occs = max_occurences;
    } else if( max_occurences >= 0 ) {
      max_occs = max_occurences;
    } else {
      max_occs = -1;
    }

    if( verbose ) {
      fprintf(out, "minimum number of occurences is % 4" PRIi64 "\n", min_occs);
    }

    // Add the initial depth (depth 0)
    // Create initial string "" at the initial range of rows in the first depth.
    memset(&new_depth, 0, sizeof(new_depth));
    new_depth.pattern_length = 0;
    new_depth.nrows = 1;
    new_depth.nchars = 0;
    new_depth.strings = NULL;  // no data yet anyway!
    new_depth.rows = malloc(sizeof(struct row_range));
    memcpy(new_depth.rows, &suffix_row_range, sizeof(struct row_range));

    err = append_array(&n_common_per_depth, &common_per_depth,
                       sizeof(struct per_depth), &new_depth);
    die_if_err(err);

    for( int patlen = 0; patlen < max_length; patlen++ ) {
      struct per_depth* from_depth;
      struct per_depth* to_depth;
      int plen;

      memset(&new_depth, 0, sizeof(new_depth));
      new_depth.pattern_length = patlen + 1;
      new_depth.nrows = 0;
      new_depth.nchars = 0;
      new_depth.strings = NULL;  // no data yet anyway!
      new_depth.rows = NULL; // no data yet!

      err = append_array(&n_common_per_depth, &common_per_depth,
                         sizeof(struct per_depth), &new_depth);
      die_if_err(err);

      from_depth = &common_per_depth[patlen];
      to_depth = &common_per_depth[patlen+1];

      plen = from_depth->pattern_length;

      //printf("DEBUG: At depth %i from plen %i n %i to plen %i n %i\n", patlen, from_depth->pattern_length, from_depth->nrows, to_depth->pattern_length, to_depth->nrows);

      for( int pat = 0; pat < from_depth->nrows; pat++ ) {
        alpha_t* from_string = from_depth->strings + pat * plen;

        if( verbose > 1 ) { 
          printf("DEBUG: will search for prev chars to: \"");
          fprint_alpha_json(out, plen, from_string);
          printf("\" with count   % 4" PRIi64 "\n",
             1 + from_depth->rows[pat].last - from_depth->rows[pat].first);
        }

        // Set up the queries on all the indexes to get all previous characters.
        for( int i = 0; i < num_indexes; i++ ) {
          // TODO -- we might be able to establish that only one character
          // actually occurs here as the previous character (quite likely)
          // and in that case wouldn't have to do O(256) work. But it seems
          // that such optimization should be supported more directly within
          // the index (since it just corresponds to a really big run in
          // the L column wavelet tree; in other words we should be just reading
          // the # occs in the L column for each character and can probably
          // do that more efficiently, but if not at least could guess
          // that it's all one character and check the number of results is the same).
          parallel_query_t* pq = NULL;
          pq = malloc(sizeof(parallel_query_t));
          err = setup_parallel_query(pq, NULL, locs[i],
                                     sizeof(backward_search_query_t),
                                     ALPHA_SIZE);
          die_if_err(err);

          for( int ch = 0; ch < ALPHA_SIZE; ch++ ) {
            query_entry_t* subq = ith_query(pq, ch);
            err = setup_backward_search_query((backward_search_query_t*)subq,
                                              &pq->proc,
                                              locs[i],
                                              from_depth->rows[pat].first,
                                              from_depth->rows[pat].last, ch);
          }

          err = femto_setup_request_err(&reqs[i], (query_entry_t*) pq, 1);
          die_if_err(err);
        }

        // Now run all of the queries.
        for( int i = 0; i < num_indexes; i++ ) {
          err = femto_begin_request_err(&srv, &reqs[i]);
          die_if_err(err);
        }
        for( int i = 0; i < num_indexes; i++ ) {
          err = femto_wait_request_err(&srv, &reqs[i]);
          die_if_err(err);
        }

        // Now, consume the query results.
        for( int i = 0; i < num_indexes; i++ ) {
          query_entry_t* q = reqs[i].query;
          if( q->type == QUERY_TYPE_PARALLEL_QUERY ) {
            parallel_query_t* pq = (parallel_query_t*) q;
            int64_t parent_first = 0;
            int64_t parent_last = 0;
            int64_t parent_count = 0;
            int any_children_in_bound = 0;
            assert( pq->num_queries == ALPHA_SIZE );
            for( alpha_t ch = 0; ch < ALPHA_SIZE; ch++ ) {
              query_entry_t* subq = ith_query(pq, ch);
              if( subq->type == QUERY_TYPE_BACKWARD_SEARCH ) {
                backward_search_query_t* bq = (backward_search_query_t*) subq;
                int64_t first = bq->first;
                int64_t last = bq->last;
                int64_t count = last - first + 1;
                int report;
                int proceed;
                struct row_range r_range = {first, last};
                assert(bq->first_in == from_depth->rows[pat].first);
                assert(bq->last_in == from_depth->rows[pat].last);
                assert(bq->ch == ch);
                report = 1;
                proceed = 1;
                parent_first = bq->first_in;
                parent_last = bq->last_in;

                // Should we report this one?
                if( count <= 0 ) {
                  proceed = 0;
                  report = 0;
                } else if( min_occs >= 0 && count < min_occs ) {
                  // below minimum, don't proceed, don't report.
                  proceed = 0;
                  report = 0;
                  // ... but if parent was above minimum, report it 
                } else if( max_occs >= 0 && count > max_occs ) {
                  // above maximum, proceed, but don't report
                  proceed = 1;
                  report = 0;
                } else {
                  // in-between minimum and maximum. Don't report,
                  // but continue. We will report this one if
                  // a longer string is not also satisfactory.
                  proceed = 1;
                  report = 0;
                  any_children_in_bound = 1;
                }
 
                if( proceed ) {
                  if( to_depth->nrows >= max_consider ||
                      nfound >= max_results ) {
                    if( verbose ) fprintf(out, "stopping for too many to consider or too many results\n");
                    // Too many. Don't add any more, but do report
                    // on this one and any more from this depth.
                    stopped_adding = 1;
                    //report = 1;
                  } else {
                    // There are too many occurences.
                    // Continue working with this string by appending it
                    // to the next depth.
                    //
                    // append the new character
                    err = append_array(&to_depth->nchars, &to_depth->strings,
                                       sizeof(alpha_t), &ch);
                    die_if_err(err);

                    // ... and the old characters
                    for( int j = 0; j < plen; j++ ) {
                      err = append_array(&to_depth->nchars, &to_depth->strings,
                                         sizeof(alpha_t), &from_string[j]);
                      die_if_err(err);
                    }
                    // ... and the rows
                    err = append_array(&to_depth->nrows, &to_depth->rows,
                                       sizeof(struct row_range), &r_range);
                    die_if_err(err);

                  }
                }
              } else assert(0);
            }
            parent_count = parent_last - parent_first + 1;
            if( any_children_in_bound ) {
              // OK, don't need to report the parent.
            } else if( min_occs >= 0 && parent_count < min_occs ) {
              // parent was below minimum, don't report it.
            } else if( max_occs >= 0 && parent_count > max_occs ) {
              // parent was above maximum, don't report it.
            } else if( plen < min_length ) {
              // parent was below minimum length, don't report it.
            } else {
              // report the parent.
              // Output this string. (future work - check prefix)
              //
              // TODO -- also form mandatory suffix by forward stepping
              // with first, last until they differ.
              if (nfound < max_results) {
                fprintf(out, "\"");
                fprint_alpha_json(out, from_depth->pattern_length,
                                  from_string);
                fprintf(out, "\" %" PRIi64, parent_count);
                if( verbose ) {
                  fprintf(out, " [%" PRIi64",%" PRIi64"] ", parent_first, parent_last);
                }
                fprintf(out, "\n");
              }
              nfound++;
            }
          } else assert(0);
          femto_cleanup_request( & reqs[i] );
        }
      }

      // Stop the search if we have already reported enough.
      if( stopped_adding ) break;
      // Stop the search if we have no strings to continue.
      if( to_depth->nrows == 0 ) break;
    }

done:
    if( verbose ) stop_clock();


    //printf("Block requests: %" PRIi64 " faults %" PRIi64 "\n",
    //       sst->stats.block_requests, sst->stats.block_faults);
    femto_stop_server(&srv);

    // Not freeing everything we could because we're just going
    // to quit...
    free_ast_node(prefix_ast);
    free_ast_node(suffix_ast);
    free(locs);
    free(reqs);
  }

  if( verbose ) {
    fprintf(out, "% 4" PRIi64 " total common strings found\n", nfound);
    print_timings("indexes queried", num_indexes);
  }

  if(out != stdout) fclose(out);

  free(index_paths);

  return 0;
}

