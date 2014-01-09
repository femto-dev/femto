/*
  (*) 2010-2013 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/femto.c
*/
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <errno.h>

#include "config.h"

#include "femto.h"
#include "femto_internal.h"
#include "error.h"
#include "server.h"
#include "query_planning.h"
#include "json.h"

// returns 0 for no error.
int femto_version(const char **version)
{
  *version = PACKAGE_STRING;

  return 0;
}

int return_err(error_t err)
{
  if( err ) {
    warn_if_err(err);
    return -1;
  }
  return 0;
}

error_t femto_start_server_err(femto_server_t* srv, int verbose)
{
  error_t err;
  server_settings_t settings;

  srv->state = NULL;

  err = set_default_server_settings(&settings);
  if( err ) return err;

  settings.verbose = verbose;

  return start_server(&srv->state, &settings);

  return ERR_NOERR;
}

int femto_start_server(femto_server_t* srv)
{
  error_t err = femto_start_server_err(srv, 0);
  return return_err(err);
}

void femto_stop_server(femto_server_t* srv)
{
  stop_server(srv->state);
  srv->state = NULL;
}

error_t femto_begin_requests_err(femto_server_t* srv, femto_request_t* reqs, int nreqs)
{
  error_t err;
  int rc;
  int i;

  for( i = 0; i < nreqs; i++ ) {
    femto_request_t* req = &reqs[i];
    if( ! req->query ) return ERR_INVALID_STR("No query in request");

    rc = pthread_mutex_lock(&req->proc.lock);
    if( rc ) return ERR_PTHREADS("Could not lock mutex", rc);

    assert( req->state & REQ_INITED );

    req->state |= REQ_BEGUN;

    rc = pthread_mutex_unlock(&req->proc.lock);
    if( rc ) {
      warn_if_err(ERR_PTHREADS("Could not unlock mutex", rc));
    }
  }

  err = server_schedule_queries(srv->state, (void*) reqs, nreqs, sizeof(femto_request_t));
  if( err ) return err;

  return ERR_NOERR;
}

int femto_begin_requests(femto_server_t* srv, femto_request_t* reqs, int nreqs)
{
  error_t err = femto_begin_requests_err(srv, reqs, nreqs);

  return return_err(err);
}

error_t femto_begin_request_err(femto_server_t* srv, femto_request_t* req)
{
  return femto_begin_requests_err(srv, req, 1);
}


int femto_begin_request(femto_server_t* srv, femto_request_t* req)
{
  return femto_begin_requests(srv, req, 1);
}

err_code_t femto_wait_request_core(femto_server_t* srv, femto_request_t* req)
{
  int rc;

  if( ! req->query ) return ERR_CODE_INVALID;

  rc = pthread_mutex_lock(&req->proc.lock);
  assert(!rc);

  if( ! (req->state & REQ_SIGNALLED) ) {
    rc = pthread_cond_wait(&req->callback_cond,
                           &req->proc.lock);
    assert(!rc);
  }

  assert( req->state & REQ_SIGNALLED );

  req->state |= REQ_DONEWAITING;

  rc = pthread_mutex_unlock(&req->proc.lock);
  assert(!rc);

  return req->proc.entry.err_code;
}


error_t femto_cancel_request_err(femto_server_t* srv, femto_request_t* req)
{
  error_t err;
  int rc;
  err_code_t ec;

  if( ! req->query ) return ERR_INVALID_STR("No query in request");

  err = server_cancel_query(srv->state, (process_entry_t*) req);
  if( err ) return err;

  rc = pthread_mutex_lock(&req->proc.lock);
  if( rc ) return ERR_PTHREADS("Could not lock mutex", rc);

  assert( req->state & REQ_INITED );
  req->state |= REQ_CANCELED;

  rc = pthread_mutex_unlock(&req->proc.lock);
  if( rc ) {
    warn_if_err(ERR_PTHREADS("Could not unlock mutex", rc));
  }

  ec = femto_wait_request_core(srv, req);

  // wait for the request.
  if( ec && ec != ERR_CODE_CANCELED ) {
    return ERR_MAKE_STR(ec, "Error during query processing");
  }
  return ERR_NOERR;
}

int femto_cancel_request(femto_server_t* srv, femto_request_t* req)
{
  error_t err;

  err = femto_cancel_request_err(srv, req);
  return return_err(err);
}

error_t femto_wait_request_err(femto_server_t* srv, femto_request_t* req)
{
  err_code_t ec;

  if( ! req->query ) return ERR_INVALID_STR("No query in request");

  ec = femto_wait_request_core(srv, req);

  if( ec ) {
    return ERR_MAKE_STR(ec, "Error during query processing");
  }
  return ERR_NOERR;
}

int femto_wait_request(femto_server_t* srv, femto_request_t* req)
{
  error_t err = femto_wait_request_err(srv, req);
  return return_err(err);
}

// Returns 0 if the request completed, 1 if we need to wait more.
error_t femto_timedwait_request_err(femto_server_t* srv, femto_request_t* req, const struct timespec* ts, int* completed)
{
  int rc;
  int done = 0;

  if( ! req->query ) return ERR_INVALID_STR("No query in request");

  rc = pthread_mutex_lock(&req->proc.lock);
  if( rc ) {
    return ERR_PTHREADS("Could not lock mutex", rc);
  }

  if( req->state & REQ_SIGNALLED ) {
    done = 1;
  } else {
    rc = pthread_cond_timedwait(&req->callback_cond,
                               &req->proc.lock,
                               ts);
    if( rc && rc != ETIMEDOUT ) {
      pthread_mutex_unlock(&req->proc.lock);
      return ERR_PTHREADS("Could wait on condition", rc);
    }

    if( rc != ETIMEDOUT ) {
      done = 1;
    }
  }

  rc = pthread_mutex_unlock(&req->proc.lock);
  if( rc ) {
    warn_if_err(ERR_PTHREADS("Could not unlock mutex", rc));
  }

  if( done ) {
    assert( req->state & REQ_INITED );
    assert( req->state & REQ_BEGUN );
    assert( req->state & REQ_SIGNALLED );

    req->state |= REQ_DONEWAITING;
    *completed = 1;
  } else {
    *completed = 0;
  }

  return ERR_NOERR;
}

int femto_timedwait_request(femto_server_t* srv, femto_request_t* req, const struct timespec* ts, int* completed)
{
  error_t err = femto_timedwait_request_err(srv, req, ts, completed);
  return return_err(err);
}

error_t femto_loc_for_path_err(femto_server_t* srv, const char* index_path, index_locator_t* loc)
{
  return path_translator_id_for_path(&srv->state->path_to_id, index_path, loc);
}

// first stores count when last==NULL
error_t parallel_count(femto_server_t* srv, index_locator_t loc, int npats, int* plen, alpha_t** pats, int64_t* first, int64_t* last)
{
  int i;
  error_t err;
  parallel_query_t* ctx = NULL;
  string_query_t* sq;

  if( ! srv ) return ERR_PARAM;

  ctx = malloc(sizeof(parallel_query_t));
  if( ! ctx ) {
    err = ERR_MEM;
    goto error;
  }

  err = setup_parallel_count(ctx, NULL, loc,
                             npats, plen, pats);
  if( err ) goto error;

  //printf("Starting parallel count for %p\n", ctx);

  err = femto_run_query(srv, (query_entry_t*) ctx);
  if( err ) goto error;

  //printf("Finished parallel count for %p\n", ctx);

  if( ctx->proc.entry.state != npats ) {
    err = ERR_INVALID;
    goto error;
  }

  // Otherwise, put the output into first and last arrays.
  for( i = 0; i < npats; i++ ) {
    sq = (string_query_t*) ith_query(ctx, i);
    // did we have any errors?
    if( sq->proc.entry.err_code ) {
      err = ERR_MAKE(sq->proc.entry.err_code);
      goto error;
    } else if( last != NULL ) {
      first[i] = sq->first;
      last[i] = sq->last;
    } else {
      first[i] = sq->last - sq->first + 1;
    }
  }
  err = ERR_NOERR;

error:
  if( ctx ) {
    cleanup_parallel_count(ctx);
  }
  free(ctx);

  return err;
}

error_t parallel_locate(femto_server_t* srv, index_locator_t loc,
                        int npats, int* plen, alpha_t** pats,
                        int max_occs_each,
                        int* noccs, int64_t** offsets)
{
  int i,j;
  error_t err;
  parallel_query_t* ctx = NULL;
  locate_query_t* lq;
  context_query_t* cq;

  if( ! srv ) return ERR_PARAM;
  // OK - the worker thread is running

  ctx = malloc(sizeof(parallel_query_t));
  if( ! ctx ) {
    err = ERR_MEM;
    goto error;
  }
  err = setup_parallel_locate(ctx, NULL, loc, npats, plen, pats, max_occs_each);
  if( err ) goto error;

  err = femto_run_query(srv, (query_entry_t*) ctx);
  if( err ) goto error;

  if( ctx->proc.entry.state != npats ) {
    err = ERR_INVALID;
    goto error;
  }

  // did we have any errors?
  for( i = 0; i < ctx->num_queries; i++ ) {
    lq = (locate_query_t*) ith_query(ctx, i);
    if( ctx->proc.entry.err_code ) {
      err = ERR_MAKE(ctx->proc.entry.err_code);
      goto error;
    } else {

      SAFETY_CHECK;

      // copy over the loc.
      noccs[i] = lq->locate_range.num_queries;
      offsets[i] = NULL;
      if( noccs[i] > 0 ) {
        SAFETY_CHECK;
        offsets[i] = malloc(sizeof(int64_t) * noccs[i]);
        if( ! offsets[i] ) {
          err = ERR_MEM;
          goto error;
        }
        for( j = 0; j < noccs[i]; j++ ) {
          cq = (context_query_t*) ith_query(&lq->locate_range, j);
          //printf("Offset %i %i %li\n", i, j, (long) cq->offset);
          offsets[i][j] = cq->offset;
        }
      }
    }
  }

  err = ERR_NOERR;

error:
  if( ctx ) {
    cleanup_parallel_locate(ctx);
  }
  free(ctx);

  return err;
}

// for testing.
error_t serial_locate(femto_server_t* srv, index_locator_t loc,
                        int npats, int* plen, alpha_t** pats,
                        int max_occs_each,
                        int* noccs, int64_t** offsets)
{
  int i,j;
  error_t err;
  parallel_query_t* ctx = NULL;
  context_query_t* cq;

  if( ! srv ) return ERR_PARAM;
  // OK - the worker thread is running

  ctx = malloc(sizeof(parallel_query_t));
  if( ! ctx ) {
    err = ERR_MEM;
    goto error;
  }

  for( i = 0; i < npats; i++ ) {
    int64_t first, last;
    int64_t cur;
    first = last = -1;
    err = parallel_count(srv, loc, 1, &plen[i], &pats[i], &first, &last);
    if( err ) goto error;

    if( 1+last-first >= max_occs_each ) noccs[i] = max_occs_each;
    else noccs[i] = 1+last-first;
    offsets[i] = NULL;
    if( noccs[i] > 0 ) {
      SAFETY_CHECK;
      offsets[i] = malloc(sizeof(int64_t) * noccs[i]);
      if( ! offsets[i] ) {
        err = ERR_MEM;
        goto error;
      }
    }
    for( j=0,cur = first; j<noccs[i]; cur++,j++ ) {
      err = setup_locate_range(ctx, NULL, loc, 
                               0, //beforectx
                               0, //afterctx
                               1, //dolocate
                               cur, cur);
      if( err ) goto error;

      err = femto_run_query(srv, (query_entry_t*) ctx);
      if( err ) goto error;
      if( ctx->proc.entry.state != (1 + cur - cur) ) {
        err = ERR_INVALID;
        goto error;
      }
  
      // Check for an error.
      cq = (context_query_t*) ith_query(ctx, 0);
      if( ctx->proc.entry.err_code ) {
        err = ERR_MAKE(ctx->proc.entry.err_code);
        goto error;
      } else {
        SAFETY_CHECK;
        // copy over the loc.
        offsets[i][j] = cq->offset;
        //printf("Offset %i %i %li\n", i, j, (long) cq->offset);
      }
    }
  }

  err = ERR_NOERR;

error:
  if( ctx ) {
    cleanup_locate_range(ctx);
  }
  free(ctx);

  return err;
}


/* offsets must have room for last-first+1 */
error_t parallel_locate_range(femto_server_t* srv, index_locator_t loc,
                        int64_t first, int64_t last,
                        int64_t* offsets)
{
  int i;
  error_t err;
  parallel_query_t* ctx = NULL;
  context_query_t* cq;

  if( ! srv ) return ERR_PARAM;
  // OK - the worker thread is running

  ctx = malloc(sizeof(parallel_query_t));
  if( ! ctx ) {
    err = ERR_MEM;
    goto error;
  }
  err = setup_locate_range(ctx, NULL, loc, 
                           0, //beforectx
                           0, //afterctx
                           1, //dolocate
                           first, last);
  if( err ) goto error;

  err = femto_run_query(srv, (query_entry_t*) ctx);
  if( err ) goto error;

  if( ctx->proc.entry.state != (1 + last - first) ) {
    err = ERR_INVALID;
    goto error;
  }

  // did we have any errors?
  for( i = 0; i < ctx->num_queries; i++ ) {
    cq = (context_query_t*) ith_query(ctx, i);
    if( ctx->proc.entry.err_code ) {
      err = ERR_MAKE(ctx->proc.entry.err_code);
      goto error;
    } else {

      SAFETY_CHECK;

      // copy over the loc.
      offsets[i] = cq->offset;
    }
  }

  err = ERR_NOERR;
error:
  if( ctx ) {
    cleanup_locate_range(ctx);
  }
  free(ctx);

  return err;
}


static
int starts_with(const char* msg, int msglen,
                const char* prefix, const char** next)
{
  int plen = strlen(prefix);
  int ret;

  if( msglen < plen ) return 0;

  ret = (0 == memcmp(msg, prefix, plen));
  if( ret && next ) {
    *next = msg + plen;
  }

  return ret;
}

enum {
  GENERIC_REQ_STRING_ROWS = 1,
  GENERIC_REQ_STRING_ROWS_ALL = 2,
  GENERIC_REQ_STRING_ROWS_ADDLEFT = 3,
  GENERIC_REQ_STRING_ROWS_ADDRIGHT = 4,
  GENERIC_REQ_FIND_STRINGS = 5,
  GENERIC_REQ_DOCS_FOR_RANGE = 6,
  GENERIC_REQ_FIND_DOCS = 7,
};

error_t femto_create_generic_request_err(femto_request_t** out,
                                         // IN
                                         femto_server_t* srv,
                                         const char* index_path,
                                         const char* request)
{
  femto_request_t* r;
  int request_len = strlen(request);
  const char* next = NULL;
  const char* end = request + request_len;
  index_locator_t loc;
  int rc;
  error_t err;
  int type = 0;
  const char* start = next;
  alpha_t* pat = NULL;
  int count = 0;
  query_entry_t* q = NULL;
  struct ast_node* ast = NULL;
  int max_chunk = 1024*16;

  err = femto_loc_for_path_err(srv, index_path, &loc);
  if( err ) return err;

  r = calloc(1, sizeof(femto_request_t));
  if( ! r ) return ERR_MEM;
 
  //printf("ALPHA_SIZE is %i\n", ALPHA_SIZE);

  if( starts_with(request, request_len, "string_rows_left", &next ) ) {
    type = GENERIC_REQ_STRING_ROWS_ADDLEFT;
  } else if( starts_with(request, request_len, "string_rows_right", &next ) ) {
    type = GENERIC_REQ_STRING_ROWS_ADDRIGHT;
  } else if (starts_with(request, request_len, "string_rows_all", &next ) ) {
    type = GENERIC_REQ_STRING_ROWS_ALL;
  } else if( starts_with(request, request_len, "string_rows", &next) ) {
    type = GENERIC_REQ_STRING_ROWS;
  } else if( starts_with(request, request_len, "find_strings", &next) ) {
    type = GENERIC_REQ_FIND_STRINGS;
  } else if( starts_with(request, request_len, "docs_for_range", &next) ) {
    type = GENERIC_REQ_DOCS_FOR_RANGE;
  } else if( starts_with(request, request_len, "find_docs", &next) ) {
    type = GENERIC_REQ_FIND_DOCS;
  } else {
    err = ERR_INVALID_STR("Bad request");
    goto error;
  }
  start = next;

  switch (type) {
    case GENERIC_REQ_STRING_ROWS:
    case GENERIC_REQ_STRING_ROWS_ALL:
    case GENERIC_REQ_STRING_ROWS_ADDLEFT:
    case GENERIC_REQ_STRING_ROWS_ADDRIGHT:

      // Read the pattern (all the current queries take integer pattern)

      // First, count the number of characters.
      // Second, extract the integers.
      for( int pass = 0; pass < 2; pass++ ) {
        count = 0;
        next = start;
        while( next != end ) {
          int num = 0;
          int used = 0;
          alpha_t ch;
          while( next[0] == ' ' ) next++; // pass any spaces.
          if( next == end ) break;
          rc = sscanf(next, "%i%n", &num, &used);
          if( rc <= 0 ) {
            err = ERR_INVALID_STR("Could not scan");
            goto error;
          }
          ch = CHARACTER_OFFSET + num;
          if( ch >= ALPHA_SIZE ) {
            err = ERR_INVALID_STR("Bad request character");
            goto error;
          }

          if( pass == 1 ) {
            pat[count] = ch;
          }
          count++;
          next += used;
        }

        if( pass == 0 ) {
          // Allocate memory for our characters.
          pat = calloc(count, sizeof(alpha_t));
        }
      }

      if( GENERIC_REQ_STRING_ROWS_ALL == type ) {
        q = calloc(1, sizeof(parallel_query_t));

        err = setup_string_rows_all_query((parallel_query_t*) q, NULL,
                                          loc,
                                          count, pat);

      } else if( GENERIC_REQ_STRING_ROWS == type ) {
        q = calloc(1, sizeof(string_query_t));

        err = setup_string_query((string_query_t*) q, NULL, loc, count, pat);

      } else if( GENERIC_REQ_STRING_ROWS_ADDLEFT == type ) {
        q = calloc(1, sizeof(parallel_query_t));

        err = setup_string_rows_addleftright_query((parallel_query_t*) q, NULL,
                                              loc,
                                              count, pat, 1);
      } else if( GENERIC_REQ_STRING_ROWS_ADDRIGHT == type ) {
        q = calloc(1, sizeof(parallel_query_t));

        err = setup_string_rows_addleftright_query((parallel_query_t*) q, NULL,
                                                   loc,
                                                   count, pat, 0);

      } else {
        assert(0); // programming error! type should be checked above.
      }


      break;
    case GENERIC_REQ_FIND_STRINGS:
      {
        process_entry_t* pq = NULL;

        // create an AST of the pattern we got.
        ast = parse_string(strlen(next), next);
        if( ! ast ) {
          err = ERR_INVALID_STR("Syntax error");
          goto error;
        }
        // streamline the AST
        streamline_query(ast);

        // turn the AST into a regexp query.
        err = create_generic_ast_count_query(&pq, NULL, loc, ast);

        q = (query_entry_t*) pq;
      }
      break;
    case GENERIC_REQ_DOCS_FOR_RANGE:
      {
        range_to_results_query_t* rq = calloc(1, sizeof(range_to_results_query_t));
        int find_offsets = 0;
        int max_matches = 0;
        int64_t first = -1;
        int64_t last = -2;
        int rc;
        result_type_t type;
        
        rc = sscanf(next, "%i %i %" SCNi64 " %" SCNi64, &max_matches, &find_offsets, &first, &last);
        if( rc != 4 ) {
          err = ERR_INVALID_STR("Syntax error");
        } else {
          type = (find_offsets)?(RESULT_TYPE_DOC_OFFSETS):(RESULT_TYPE_DOCUMENTS);
          if( max_matches > max_chunk ) max_matches = max_chunk;
          err = setup_range_to_results_query(rq, NULL, loc, max_matches, 
                                             type,
                                             first, last);
          
          q = (query_entry_t*) rq;
        }
      }
      break;
    case GENERIC_REQ_FIND_DOCS:
      {
        results_query_t* rq = NULL;

        int num_scanned = 0;
        int find_offsets = 0;
        int max_matches = 0;
        int rc;
        result_type_t type;
        
        rc = sscanf(next, "%i %i%n", &max_matches, &find_offsets, &num_scanned);
        if( ! (rc == 2 || rc == 3) ) {
          err = ERR_INVALID_STR("Syntax error");
        } else {
          next += num_scanned;
          type = (find_offsets)?(RESULT_TYPE_DOC_OFFSETS):(RESULT_TYPE_DOCUMENTS);
          if( max_matches > max_chunk ) max_matches = max_chunk;

          // create an AST of the pattern we got.
          ast = parse_string(strlen(next), next);
          if( ! ast ) {
            err = ERR_INVALID_STR("Syntax error");
            goto error;
          }
          // streamline the AST
          streamline_query(ast);

          // turn the AST into a results query.
          err = create_generic_ast_query(&rq, NULL, loc, max_matches, type, ast);

          q = (query_entry_t*) rq;
        }
      }
      break;
  }

  if( err ) {
    if( q ) {
      cleanup_generic(q);
      free(q);
    }
    goto error;
  }

  err = femto_setup_request_err(r, q, 1 /* free q on cleanup */);
  if( err ) goto error;

  r->user_type = type;
  r->user_ptr = (void*) request;
  r->ast_to_free = ast;
  *out = r;

  return ERR_NOERR;

error:
  femto_destroy_request(r);
  if( ast ) free_ast_node(ast);
  *out = NULL;
  return err;
}

int femto_create_generic_request(femto_request_t** req,
                                 // IN
                                 femto_server_t* srv,
                                 const char* index_path,
                                 const char* request)
{
  error_t err = femto_create_generic_request_err(req, srv, index_path, request);
  return return_err(err);
}

#define MAYBE_REALLOC(needed) \
          if( printed + (needed) >= max_ret ) { \
            char* newret; \
            int new_len = 2*(printed + (needed)); \
            newret = realloc(ret, new_len); \
            if( ! newret ) { \
              free(ret); \
              return ERR_MEM; \
            } \
            ret = newret; \
            max_ret = new_len; \
          }

#define MAX_NUM (100)


/* Prints out things of the form
 *   {"doc_info":INFO_STRING, "offsets":[NUM,NUM,...]},
 * where "offsets" section is only printed out if we have offsets.
 */
error_t get_matches(char** buf_out,
                    int* buf_len,
                    int* printed_ptr,
                    femto_server_t* srv, index_locator_t loc,
                    results_t* results)
{
  char* ret = *buf_out;
  int max_ret = *buf_len;
  int printed = *printed_ptr;

  int64_t document, last_document;
  int64_t offset;
  results_reader_t reader;
  parallel_query_t ctx;
  error_t err;
  int num = 0;
  long* lens = NULL;
  unsigned char **infos = NULL;
  int i,j;
  int num_this_doc = 0;
  char charbuf[2];

  charbuf[0] = charbuf[1] = '\0';

  if( results_num_results(results) <= 0 ) return ERR_NOERR;

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
      MAYBE_REALLOC(100 + MAX_JSON*(lens[j]));
      if( i != 0 ) {
        if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
          printed += sprintf(&ret[printed],"]");
        }
        printed += sprintf(&ret[printed],"},\n");
      }
      printed += sprintf(&ret[printed],"  {\"doc_info\":\"");
      for( int k = 0; k < lens[j]; k++ ) {
        charbuf[0] = infos[j][k];
        //printed += sprintf(&ret[printed], "%c", infos[j][k]);
        printed += encode_ch_json(&ret[printed], infos[j][k]);
      }
      printed += sprintf(&ret[printed],"\"");
      if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
        printed += sprintf(&ret[printed], ", \"offsets\":[");
      }
      j++;
      num_this_doc = 0;
    }
    if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
      MAYBE_REALLOC(100 + MAX_NUM);
      if( num_this_doc != 0 ) {
        printed += sprintf(&ret[printed], ",");
      }
      printed += sprintf(&ret[printed], "%" PRIi64, offset);
      num_this_doc++;
    }
    i++;
  }

  MAYBE_REALLOC(100);
  if( i != 0 ) {
    if( results->type == RESULT_TYPE_DOC_OFFSETS ) {
      printed += sprintf(&ret[printed], "]");
    }
    printed += sprintf(&ret[printed],"}\n");
  }

  for( i = 0; i < num; i++ ) {
    free(infos[i]);
  }
  free(infos);
  free(lens);

  *buf_out = ret;
  *buf_len = max_ret;
  *printed_ptr = printed;
  return ERR_NOERR;
}

error_t femto_response_for_generic_request_err(femto_request_t* req,
                                               femto_server_t* srv,
                                               char** response)
{
  int max_ret = 100 + MAX_NUM*512; // 30 digits coveres 2^64
  char* ret = NULL;
  int printed = 0;
  error_t err;

  if( ! (req->state & REQ_DONEWAITING) ) {
    return ERR_INVALID_STR("Request not completed");
  }

  ret = malloc(max_ret);
  if( ! ret ) return ERR_MEM;

  switch ( req->user_type ) {
    case GENERIC_REQ_STRING_ROWS:
      {
        string_query_t* sq = (string_query_t*) req->query;
        assert( sq->proc.entry.type == QUERY_TYPE_STRING );

        if( sq->proc.entry.err_code ) {
          err = ERR_MAKE_STR(sq->proc.entry.err_code, "Error in query processing");
          goto error;
        }


        MAYBE_REALLOC(10 + 2*MAX_NUM);
        printed += sprintf(&ret[printed], "{\"range\":[%" PRIi64 ",%" PRIi64 "]}\n", sq->first, sq->last);
        assert( printed < max_ret );
        break;
      }
    case GENERIC_REQ_STRING_ROWS_ALL:
    case GENERIC_REQ_STRING_ROWS_ADDLEFT:
    case GENERIC_REQ_STRING_ROWS_ADDRIGHT:
      {
        char* leftright[3] = {"left", "right", NULL};
        int on_right = 0;
        int first = 1;
        parallel_query_t* pq = (parallel_query_t*) req->query;
        assert( pq->proc.entry.type == QUERY_TYPE_PARALLEL_QUERY );

        if( req->user_type == GENERIC_REQ_STRING_ROWS_ADDRIGHT ) on_right = 1;

        MAYBE_REALLOC(100 + 2*MAX_NUM);
        printed += sprintf(&ret[printed], "{\"%s\":[\n", leftright[on_right]);

        for( int i = 0; i < pq->num_queries; i++ ) {
          string_query_t* sq = (string_query_t*) ith_query(pq, i);
          int ch = i;

          MAYBE_REALLOC(100 + 2*MAX_NUM);

          if( i == ALPHA_SIZE ) {
            on_right = 1;
            printed += sprintf(&ret[printed], "\n ],\n");
            printed += sprintf(&ret[printed], " \"%s\":[\n", leftright[on_right]);
            first = 1;
          }

          if( i >= ALPHA_SIZE ) ch -= ALPHA_SIZE;
          ch -= CHARACTER_OFFSET;

          assert( sq->proc.entry.type == QUERY_TYPE_STRING );

          if( sq->proc.entry.err_code ) {
            err = ERR_MAKE_STR(sq->proc.entry.err_code, "Error in query processing");
            goto error;
          }

          if( sq->first <= sq->last ) {
            if( ! first ) {
              printed += sprintf(&ret[printed], ",\n");
            } else {
              first = 0;
            }

            printed += sprintf(&ret[printed], "  {\"ch\":%i, \"range\":[%" PRIi64 ",%" PRIi64 "]}", ch, sq->first, sq->last);
          }

          assert( printed < max_ret );
        }
        MAYBE_REALLOC(10);
        printed += sprintf(&ret[printed], "\n ]\n");
        printed += sprintf(&ret[printed], "}\n");
        break;
      }
    case GENERIC_REQ_FIND_STRINGS:
      if( req->query->type == QUERY_TYPE_REGEXP ) {
        regexp_query_t* rq = (regexp_query_t*) req->query;
        int first = 1;

        MAYBE_REALLOC(100);
        printed += sprintf(&ret[printed], "{\"matches\":[\n");

        // Put it all into a result.
        for( int i = 0; i < rq->results.num_results; i++ ) {
          regexp_result_t result = rq->results.results[i];
          if( result.last >= result.first &&
              result.first >= 0 ) {

            MAYBE_REALLOC(100 + MAX_JSON*result.match_len);

            if( !first ) {
              printed += sprintf(&ret[printed], ",\n");
            } else {
              first = 0;
            }

            printed += sprintf(&ret[printed], "  {\"range\":[%" PRIi64 ",%" PRIi64 "], \"cost\":%i, \"match\":\"", result.first, result.last, result.cost);
            for( int k = 0; k < result.match_len; k++ ) {
              int ch = result.match[k];
              printed += encode_alpha_json(&ret[printed], ch);
            }
          }

          printed += sprintf(&ret[printed], "\"}");
        }
        printed += sprintf(&ret[printed], "\n ]\n}\n");

      } else if( req->query->type == QUERY_TYPE_STRING ) {
        string_query_t* sq = (string_query_t*) req->query;
        if( sq->last >= sq->first &&
            sq->first >= 0 ) {

          MAYBE_REALLOC(100 + 4*sq->plen);

          printed += sprintf(&ret[printed], "{\"matches\":[\n");
          printed += sprintf(&ret[printed], "  {\"range\":[%" PRIi64 ",%" PRIi64 "], \"cost\":0, \"match\":\"", sq->first, sq->last);
          for( int k = 0; k < sq->plen; k++ ) {
            int ch = sq->pat[k];
            printed += encode_alpha_json(&ret[printed], ch);
          }
          printed += sprintf(&ret[printed], "\"}\n ]\n}\n");
        }
      }
      break;
    case GENERIC_REQ_DOCS_FOR_RANGE:
      {
        range_to_results_query_t* rq = (range_to_results_query_t*) req->query;

        MAYBE_REALLOC(100);

        printed += sprintf(&ret[printed], "{\"range\":[%" PRIi64 ",%" PRIi64 "],\n \"results\":[\n", rq->first, rq->i - 1);

        err = get_matches(&ret, &max_ret, &printed, srv, req->query->loc, &rq->results.results);
        if( err ) goto error;

        MAYBE_REALLOC(100);
        printed += sprintf(&ret[printed], " ]\n}\n");
      }
      break;
    case GENERIC_REQ_FIND_DOCS:
      {
        results_query_t* rq = (results_query_t*) req->query;

        MAYBE_REALLOC(100);

        printed += sprintf(&ret[printed], "{\"results\":[\n");

        err = get_matches(&ret, &max_ret, &printed, srv, req->query->loc, &rq->results);
        if( err ) goto error;

        MAYBE_REALLOC(100);
        printed += sprintf(&ret[printed], " ]\n}\n");
      }
      break;
    default:
      err = ERR_INVALID_STR("Unknown request type");
      goto error;
  }

  *response = ret;
  return ERR_NOERR;

error:
  *response = NULL;
  free(ret);
  return err;
}

error_t femto_response_for_generic_request(femto_request_t* req,
                                           femto_server_t* srv,
                                           char** response)
{
  error_t err = femto_response_for_generic_request_err(req, srv, response);
  return return_err(err);
}

