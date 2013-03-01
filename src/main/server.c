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

  femto/src/main/server.c
*/
#include "server.h"
#include "ast.h"
#include "results.h"
#include "bit_array.h"
#include "util.h"
#include "compile_regexp.h"
#include "processors.h"
#include "femto_internal.h"

#include <ctype.h>
#include <pthread.h>
#include <alloca.h>
#include <errno.h>

// Print info about scheduled queries.
#define DEBUG_SERVER 0

#define MAX_REGEXP_ITERATIONS 1000000

// Some prototypes
void femto_cleanup_request(struct femto_request* req);
void do_signal_request(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_string_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_backward_search_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_back_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_forward_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_context_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_parallel_query(server_state_t* ss, query_mode_t mode, query_entry_t* query);
void do_locate_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_range_to_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_and_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_or_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_not_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_ThenWithin_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_string_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
int server_has_block(server_state_t* ss, index_locator_t index_loc, int64_t block_id);
void do_regexp_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_regexp_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_get_doc_info_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_backward_merge_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_index_merge_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
void do_extract_document_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe);
error_t add_mapping (nfa_description_t* nfa, queue_map_t* queue,
                     regexp_result_t* new_result,
                     nfa_errcnt_t* nfa_states);
error_t setup_generic_boolean_query( generic_boolean_query_t* q,
                                     process_entry_t* requestor, 
                                     index_locator_t loc,
                                     int64_t chunk_size,
                                     struct boolean_node* ast_node);

void cleanup_generic_boolean_query(generic_boolean_query_t* q);

// used by do-forward and do-back
#define I_LOC_UNSET -10

// used in the do_.... functions - these macros
// assume that the "current" query is s, which is 
// a kind of "process query".

void deliver_result(server_state_t* ss, query_entry_t* q);

static inline 
void return_result (server_state_t* ss, process_entry_t* ce_p)
{
    assert( is_process_query( ce_p->entry.type ) );

    ce_p->done = 1;

    if( ! ce_p->entry.err_code && ce_p->num_pending_requests != 0 ) {
      // return result is called early; make sure that this does
      // not pass through unnoticed, so we set an error.
      ce_p->entry.err_code = err_code(ERR_INVALID);
      fprintf(stderr, "Warning: early return_result with no error\n");
    }
    /* if we're returning an error code and there's outstanding requests */
    if( ce_p->entry.err_code && ce_p->num_pending_requests > 0 ) {
      /* just wait for the other responses before returning */
    } else {
      /* otherwise, deliver the result. */
      deliver_result(ss, (query_entry_t*) ce_p);
    }
}

// return the result!
#define RETURN_RESULT { \
  return_result(ss, (process_entry_t*) s); \
  return; \
}

// respond with an error code (of type error_t)
#define RETURN_ERROR(xxx) { \
  ((process_entry_t*)s)->entry.err_code = err_code(xxx); \
  RETURN_RESULT; \
}

// check a thing of type error_t for an error code.
#define CHECK_ERROR(xxx) { \
  if( xxx ) { \
    RETURN_ERROR(xxx); \
  } \
}

// check a response - both that it's non-NULL and that it's set.
#define CHECK_RESPONSE(xxx) { \
  process_entry_t* ce_p = (process_entry_t*) s; \
  query_entry_t* ce_ent = (query_entry_t*) (xxx); \
  if( ! ce_ent ) RETURN_ERROR(ERR_INVALID); \
  /* if we have an error code or the response did, call return_result */ \
  if( ce_ent->err_code ) { \
    ce_p->entry.err_code = ce_ent->err_code; \
  } \
  if( ce_p->entry.err_code ) { \
    RETURN_RESULT; \
  } \
}

// given a new state, wait in that state
#define WAIT_STATE(xxx) { \
  process_entry_t* ce_p = (process_entry_t*) s; \
  ce_p->entry.state = xxx; \
  return; \
}

// just wait; don't change state.
#define WAIT { \
  return; \
}


// Create a new query
static inline void setup_query_entry(query_entry_t* q,
    // params for query entry
    query_type_t type, index_locator_t loc,
    int state, process_entry_t* parent)
{
  q->err_code = ERR_CODE_NOERR;
  q->type = type;
  q->state = state;
  q->loc = loc;
  q->parent = parent;
  
  memset(&q->entries, 0, sizeof(LIST_ENTRY(query_entry)));
}

static inline void setup_leaf_entry(leaf_entry_t* q,
    // params for the query entry
    query_type_t type, index_locator_t loc,
    int state, process_entry_t* parent,
    // params for the leaf entry
    int64_t block_id)
{
  memset(q, 0, leaf_entry_size(type));
  setup_query_entry(&q->entry, type, loc, state, parent);

  q->block_id = block_id;
  memset(& q->rb_node, 0, sizeof(rb_node_t));

  assert(index_locator_is_valid(loc));
}

static inline error_t reset_process_entry(process_entry_t* q,
    // params for the query entry
    query_type_t type, index_locator_t loc,
    int state, process_entry_t* parent,
    // params for the process entry
    query_callback callback)
{
  setup_query_entry(&q->entry, type, loc, state, parent);

  q->num_pending_requests = 0;
  q->canceled = 0;
  q->done = 0;
  q->callback = callback;

  if( ! q->setup ) return ERR_INVALID;

  return ERR_NOERR;
}

static inline error_t setup_process_entry(process_entry_t* q,
    // params for the query entry
    query_type_t type, index_locator_t loc,
    int state, process_entry_t* parent,
    // params for the process entry
    query_callback callback)
{
  int rc;

  setup_query_entry(&q->entry, type, loc, state, parent);
  
  q->num_pending_requests = 0;
  q->canceled = 0;
  q->done = 0;
  q->callback = callback;

  //printf("Initing process mutex %p\n", q);
  rc = pthread_mutex_init(&q->lock, NULL);
  if( rc ) {
    return ERR_PTHREADS("Could not init mutex", rc);
  }
  q->setup = 1;

  return ERR_NOERR;
}

void cleanup_process_entry(process_entry_t* q)
{
  int rc;
  if( q && q->setup ) {
    //printf("Destroying process mutex %p\n", q);
    rc = pthread_mutex_destroy(&q->lock);
    if( rc ) {
      warn_if_err(ERR_PTHREADS("Could not destroy mutex", rc));
    }
    q->setup = 0;
  }
}

void deliver_result(server_state_t* ss, query_entry_t* q)
{
  process_entry_t* p;

  SAFETY_CHECK;
  if( DEBUG_SERVER > 8 ) { 
    printf("%i delivering result %p (type %i) to %p (%i pending)\n", ss->thread_number, q, q->type, q->parent, (q->parent)?(q->parent->num_pending_requests):(-1));
  }

  if( is_process_query(q->type) ) {
    // check that it has no pending requests.
    p = (process_entry_t*) q;
    assert( p->num_pending_requests == 0 );
  }

  if( q->parent ) query_queue_push(&ss->ready_with_results, q);
}

void really_deliver_result(server_state_t* ss, query_entry_t* q)
{
  int rc;
  process_entry_t* p;

  if( q->parent ) {
    p = q->parent;

    rc = pthread_mutex_lock(&p->lock);
    if( rc ) {
      fprintf(stderr, "pthread_mutex_lock failed with %i when delivering err_code %i\n", rc, q->err_code);
      assert(!rc);
    }

    // parent must be a process query.
    assert( is_process_query(p->entry.type) );
    p->num_pending_requests--;
    assert( p->num_pending_requests >= 0 );
    if( DEBUG_SERVER > 8 ) printf("%i calling callback for %p with %p\n", ss->thread_number, p, q);
    p->callback(ss, QUERY_MODE_RESULT, q);

    rc = pthread_mutex_unlock(&p->lock);
    assert(!rc);
  }
}

// ASSUMES that ss->lock is held and that q->parent->lock is held
// (if q->parent != NULL).
error_t schedule_query(server_state_t* ss, query_entry_t* q)
{
  SAFETY_CHECK;
  if( DEBUG_SERVER > 8 ) printf("%i scheduling query %p for %p %i\n", ss->thread_number, q, q->parent, q->type);

  assert( q->type ); // no type == invalid query!

  if( q->parent ) {
    q->parent->num_pending_requests++;
  }

  if( is_leaf_query( q->type ) ) {
    if( DEBUG_SERVER > 8 ) {
      leaf_entry_t* bq = (leaf_entry_t*) q;
      printf("Scheduling leaf query %p for block_id %" PRIi64 "\n", bq, bq->block_id);
    }

    query_queue_push(&ss->new_leaf_queries, q);
  } else {
    assert(is_process_query(q->type));
    query_queue_push(&ss->new_process_queries, q);
  }

  return ERR_NOERR;
}

void setup_header_occs_query(
                     header_occs_query_t* q,
                     // query entry params
                     index_locator_t loc,
                     int state,
                     process_entry_t* requestor,
                     // request params
                     header_occs_request_type_t type,
                     int64_t block_num, int ch, int64_t row, int64_t occs)
{
  setup_leaf_entry( &q->leaf,
                    // query entry params
                    QUERY_TYPE_HEADER_OCCS, loc,
                    state, requestor,
                    // leaf entry params
                    block_id_for_type(INDEX_BLOCK_TYPE_HEADER, 0)
                  );

  if( EXTRA_CHECKS ) assert( ch == INVALID_ALPHA ||
                             (0 <= ch && ch <= ALPHA_SIZE ) );
  q->type = type; 
  q->r.block_num = block_num;
  q->r.ch = ch;
  q->r.row = row;
  q->r.occs = occs;
}

void setup_header_loc_query(
                     header_loc_query_t* q,
                     // params for the leaf entry
                     index_locator_t loc,
                     int state,
                     process_entry_t* requestor,
                     // request params
                     header_loc_request_type_t type,
                     int64_t offset, int64_t doc)
{
  setup_leaf_entry( &q->leaf,
                    // query entry params
                    QUERY_TYPE_HEADER_LOC, loc,
                    state, requestor,
                    // leaf entry params
                    block_id_for_type(INDEX_BLOCK_TYPE_HEADER,0) );
 
  q->type = type; 
  q->r.offset = offset;
  q->r.loc.doc = doc;
  q->r.doc_info = NULL;
}
void cleanup_header_loc_query(header_loc_query_t* q)
{
  assert(q->leaf.entry.type == QUERY_TYPE_HEADER_LOC );
  free(q->r.doc_info);
}

void setup_block_query(block_query_t* q,
                       // leaf entry params
                       index_locator_t loc,
                       int state,
                       process_entry_t* requestor,
                       // request params
                       block_request_type_t type,
                       int64_t block_number,
                       int ch, int row_in_block, int occs_in_block)
{

  setup_leaf_entry( & q->leaf,
                    QUERY_TYPE_BLOCK, loc,
                    state, requestor,
                    block_id_for_type(INDEX_BLOCK_TYPE_DATA, block_number));
 
  if( EXTRA_CHECKS ) assert( ch == INVALID_ALPHA ||
                             (0 <= ch && ch < ALPHA_SIZE ) );

  q->type = type;
  q->r.ch = ch;
  q->r.row_in_block = row_in_block;
  q->r.occs_in_block = occs_in_block;
}
void setup_block_chunk_query(block_chunk_query_t* q,
                       // leaf entry params
                       index_locator_t loc,
                       int state,
                       process_entry_t* requestor,
                       // request params
                       block_chunk_request_type_t type,
                       int64_t block_number, int chunk_number,
                       int first_in_block, int last_in_block)
{

  setup_leaf_entry( & q->leaf,
                    QUERY_TYPE_BLOCK_CHUNK, loc,
                    state, requestor,
                    block_id_for_type(INDEX_BLOCK_TYPE_DATA, block_number));
 
  q->type = type;
  q->r.first = first_in_block;
  q->r.last = last_in_block;
  q->r.chunk_number = chunk_number;
}

void cleanup_block_chunk_query(block_chunk_query_t* q)
{
  assert( q->leaf.entry.type == QUERY_TYPE_BLOCK_CHUNK );
  results_destroy(&q->r.results);
}

#if SUPPORT_INDEX_MERGE

void setup_block_update_query(block_update_query_t* q,
                       // leaf entry params
                       index_locator_t loc,
                       int state,
                       process_entry_t* requestor,
                       // request params
                       block_update_type_t type,
                       int64_t dst_block_number,
                       uint32_t dst_row_in_block,
                       int ch, int64_t src_row, 
                       int64_t offset, int64_t doc)
{
  setup_leaf_entry( & q->leaf,
                    QUERY_TYPE_BLOCK_UPDATE, loc,
                    state, requestor,
                    block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE,
                                      dst_block_number));
 
  q->type = type;
  q->r.dst_row_in_block = dst_row_in_block;
  q->r.ch = ch;
  q->r.src_row = src_row;
  q->r.offset = offset;
  q->r.doc = doc;
}

void setup_block_update_manage_query(block_update_manage_query_t* q,
                                     // leaf entry params
                                     index_locator_t loc,
                                     int state,
                                     process_entry_t* requestor,
                                     // request params
                                     block_update_manage_type_t type,
                                     int64_t dst_block_number,
                                     double ratio,
                                     int start_uncompressed,
                                     int max_uncompressed,
                                     int offset_size_bits,
                                     index_locator_t src_loc,
                                     index_locator_t out_loc,
                                     index_block_param_t* index_params)
{
  setup_leaf_entry( &q->leaf, 
                    QUERY_TYPE_BLOCK_UPDATE_MANAGE, loc,
                    state, requestor, 
                    block_id_for_type(INDEX_BLOCK_TYPE_DATA_UPDATE,
                                      dst_block_number));

  q->type = type;
  q->r.ratio = ratio;
  q->r.start_uncompressed = start_uncompressed;
  q->r.max_uncompressed = max_uncompressed;
  q->r.offset_size_bits = offset_size_bits;
  q->r.src_loc = src_loc;
  q->r.out_loc = out_loc;
  memcpy(&q->r.index_params, index_params, sizeof(index_block_param_t));
}
#endif

error_t femto_run_query(femto_server_t* srv, query_entry_t* query)
{
  struct femto_request req;
  error_t err;

  err = femto_setup_request_err(&req, query, 0);
  if( err ) return err;

  err = femto_begin_request_err(srv, &req);
  if( err ) return err;

  err = femto_wait_request_err(srv, &req);
  if( err ) return err;

  femto_cleanup_request(&req);

  return ERR_NOERR;
}


error_t femto_setup_request_err(struct femto_request* req, 
                                query_entry_t* query,
                                int free_query)
{
  error_t err;
  int rc;

  memset(req, 0, sizeof(struct femto_request));

  if( ! query ) return ERR_INVALID;
  if( ! query->type ) return ERR_INVALID;

  err = setup_process_entry(&req->proc, QUERY_TYPE_SIGNAL, query->loc,
                      0, NULL, do_signal_request);
  if( err ) return err;

  req->query = query;
  req->free_query = free_query;

  rc = pthread_cond_init(&req->callback_cond, NULL);
  if( rc ) {
    err = ERR_PTHREADS("Could not init cond", rc);
    goto error;
  }

  query->parent = (process_entry_t*) req;

  req->state = REQ_INITED;

  req->user_type = 0;
  req->user_ptr = NULL;

  req->ast_to_free = NULL;

  return ERR_NOERR;

error:
  femto_cleanup_request(req);
  return err;
}

void femto_cleanup_request(struct femto_request* req)
{
  int rc;

  if( req ) {
    rc = pthread_cond_destroy(&req->callback_cond);
    if( rc ) {
      warn_if_err(ERR_PTHREADS("Could destroy condition variable", rc));
    }

    if( req->free_query && req->query ) {
      cleanup_generic(req->query);
      free(req->query);
    }

    cleanup_process_entry(&req->proc);

    req->state = 0;

    if( req->ast_to_free ) {
      free_ast_node(req->ast_to_free);
      req->ast_to_free = NULL;
    }
  }
}

void femto_destroy_request(struct femto_request* req)
{
  femto_cleanup_request(req);
  free(req);
}

void do_signal_request(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  struct femto_request* s = NULL;
  query_entry_t* sub = NULL;
  error_t err;
  int rc;

  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_SIGNAL );
      s = (struct femto_request*) qe;
      break;
    case QUERY_MODE_RESULT:
      assert( qe->parent->entry.type == QUERY_TYPE_SIGNAL );
      sub = qe;
      s = (struct femto_request*) sub->parent;
      break;
    default:
      assert(0);
  }

  if( DEBUG_SERVER > 8 ) printf("In do_signal_request %p %p\n", s, sub);

#define SIG_RETURN \
  { \
    s->state |= REQ_SIGNALLED; \
    rc = pthread_cond_broadcast(&s->callback_cond); \
    assert(!rc); \
    RETURN_RESULT; \
  }

#define SIG_CHECK_ERROR(xxx) \
  if( xxx ) { \
    s->proc.entry.err_code = err_code(xxx); \
    SIG_RETURN; \
  }

  while( 1 ) {
    switch( s->proc.entry.state ) {
      case 0:
        if( DEBUG_SERVER > 8 ) printf("In do_signal_request %p %p scheduling %p\n", s, sub, s->query);
        err = schedule_query(ss, s->query);
        SIG_CHECK_ERROR( err );
        if( DEBUG_SERVER > 8 ) printf("In do_signal_request %p %p waiting\n", s, sub);
        WAIT_STATE(0x100);
      case 0x100:
        if( DEBUG_SERVER > 8 ) printf("In do_signal_request %p %p checking\n", s, sub);

        if( sub->err_code ) {
          s->proc.entry.err_code = sub->err_code;

        }

        if( DEBUG_SERVER > 8 ) printf("In do_signal_request %p %p signalling and returning\n", s, sub);

        // The lock is already held!
        SIG_RETURN;
      default:
        SIG_CHECK_ERROR(ERR_INVALID);
    }
  }
}

error_t setup_string_query(
        string_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int plen, 
        alpha_t* pat)
{
  error_t err;

  memset(q, 0, sizeof(string_query_t));

  err = setup_process_entry( & q->proc, 
                             // query entry params
                             QUERY_TYPE_STRING, loc,
                             0, requestor,
                             // proc callback
                             do_string_query);
  if( err ) return err;
  q->plen = plen;
  q->pat = malloc(sizeof(alpha_t) * q->plen);
  if( ! q->pat ) {
    return ERR_MEM;
  }
  memcpy(q->pat, pat, sizeof(alpha_t) * q->plen);
  
  q->i = -1;
  return ERR_NOERR;
}

void cleanup_string_query(string_query_t* q ) 
{
  assert( q->proc.entry.type == QUERY_TYPE_STRING );
  free(q->pat);
  q->pat = NULL;
  cleanup_process_entry(&q->proc);
}

void do_string_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  string_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  error_t err;
  int ch;


  switch ( mode ) {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_STRING );
      s = (string_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_STRING );
      s = (string_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

  
#define BLK_REQUEST(nqx,statex,type,blocknum,chr,row)  \
{ \
  setup_block_query(nqx, \
                    s->proc.entry.loc, \
                    statex, \
                    & s->proc, \
                    type, \
                    blocknum, chr, row, 0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}

#define HDR_REQUEST_OCCS(nqx,statex,type,row,chr)  \
{ \
  setup_header_occs_query(nqx, \
                          s->proc.entry.loc, \
                          statex, \
                          &s->proc,\
                          type, 0,chr,row,0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}


  /* The normal algorithm:
     i = plen-1; ch = pattern[i]; first = C[ch]; last = C[ch+1]-1
     While( first <= last && i >= 1 ) {
       ch = pattern[i-1];
       first = C[ch] + Occ(ch, first - 1);
       last = C[ch] + Occ(ch, last) - 1;
       i--;
     }
   */

  while( 1 ) {
   switch (s->proc.entry.state) {
    case 0:
      // set up - make requests for C[ch] and C[ch+i]
      s->i = s->plen - 1;
      ch = s->pat[s->i];
      // request C[ch] and C[ch+1]
      HDR_REQUEST_OCCS(&s->hq1, 0x101, HDR_REQUEST_C, 0, ch );
      HDR_REQUEST_OCCS(&s->hq2, 0x110, HDR_REQUEST_C, 0, ch+1);
      // wait for C[ch] and C[ch+1]
      WAIT_STATE(0x100); // wait for these to come back.
    case 0x100:
    case 0x101:
    case 0x110:
      CHECK_RESPONSE(hq);
      switch (hq->leaf.entry.state ) {
        case 0x101:
          // we got C[ch] in C.
          s->first = hq->r.occs;
          break;
        case 0x110:
          s->last = hq->r.occs - 1;
          break;
        default:
          RETURN_ERROR(ERR_INVALID);
      }
      s->proc.entry.state |= hq->leaf.entry.state;
      if( s->proc.entry.state != 0x111 ) WAIT; // still have to wait
      // Otherwise,
      // we've got s->first and s->last set 
      // as well as s->i.
      // fall through to start of While loop
    case 0x111:
      // "while" loop state.
      if( s->first > s->last || s->i == 0 ) {
        // we're done and don't need to make any more requests.
        // call our parent's done function.
        if( DEBUG > 3 ) {
          printf("do_string_query returning with [%lli,%lli]\n",
                (long long int) s->first,
                (long long int) s->last);
        }
        RETURN_RESULT;
      }
      ch = s->pat[s->i - 1];

      // which block are we asking about?
      // We're going to request Occ(ch, First -1 );
      // request C[ch] and the block's part of occs.
      if( s->first == 0 ) {
        HDR_REQUEST_OCCS(&s->hq1, 0x202,
                         HDR_REQUEST_C,
                         0, ch);
        s->hq1.r.row = 0;
      } else {
        HDR_REQUEST_OCCS(&s->hq1, 0x202,
                         HDR_BSEARCH_BLOCK_ROWS |
                         HDR_REQUEST_C |
                         HDR_REQUEST_BLOCK_OCCS |
                         HDR_BACK,
                         s->first - 1, ch);
      }

      // request the block's part of occs.
      HDR_REQUEST_OCCS(&s->hq2, 0x220,
                       HDR_BSEARCH_BLOCK_ROWS |
                       HDR_REQUEST_C |
                       HDR_REQUEST_BLOCK_OCCS |
                       HDR_BACK,
                       s->last, ch);

      //return to wait for responses
      WAIT_STATE(0x200);
    case 0x200:
    case 0x202:
    case 0x220:
      // wait for both responses for the header requests.
      CHECK_RESPONSE(hq);
      s->proc.entry.state |= qe->state;

      if( s->proc.entry.state != 0x222 ) WAIT; // continue to wait
      // fall through
    case 0x222:
      ch = s->pat[s->i - 1];

      // request Occ(ch, Last)
      BLK_REQUEST(&s->bq2, 0x330,
              BLOCK_REQUEST_OCCS,
              s->hq2.r.block_num, ch, s->last - s->hq2.r.row);

      // request Occ(ch, First - 1)
      if( s->first == 0 ) {
        s->bq1.r.occs_in_block = 0;
        s->proc.entry.state = 0x303;
      } else {
        BLK_REQUEST(&s->bq1, 0x303,
                BLOCK_REQUEST_OCCS,
                s->hq1.r.block_num, ch, s->first - 1 - s->hq1.r.row);
        s->proc.entry.state = 0x300;
      }

      // Return so we wait for the rest of the requests.
      WAIT;
    case 0x300:
    case 0x303:
    case 0x330:
      CHECK_RESPONSE(bq);
      s->proc.entry.state |= qe->state;

      if( s->proc.entry.state != 0x333 ) WAIT; // continue to wait
      // fall through
    case 0x333:
      // add in the value from the header
      s->first = 0;
      s->last = 0;

      s->first += s->hq1.r.occs;
      s->first += s->bq1.r.occs_in_block;

      // we got the C[ch] + Occ(ch, Last). Add it to last and subtract 1.
      s->last += s->hq2.r.occs;
      s->last += s->bq2.r.occs_in_block;
      s->last--;

      // decrement i and continue the loop
      s->i--;
      s->proc.entry.state = 0x111; // the "while loop state"
      break; // will continue in the while loop.
    default:
      RETURN_ERROR(ERR_INVALID);
   }
  }
 
#undef HDR_REQUEST_OCCS
#undef BLK_REQUEST

}

error_t setup_backward_search_query(
        backward_search_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t first, int64_t last, alpha_t ch)
{
  error_t err;

  memset(q, 0, sizeof(backward_search_query_t));

  err = setup_process_entry( & q->proc, 
                             // query entry params
                             QUERY_TYPE_BACKWARD_SEARCH, loc,
                             0, requestor,
                             // proc callback
                             do_backward_search_query);
  if( err ) return err;
  q->first_in = first;
  q->last_in = last;
  q->ch = ch;
  
  q->first = -1;
  q->last = -1;
  return ERR_NOERR;
}

void cleanup_backward_search_query(backward_search_query_t* q ) 
{
  assert( q->proc.entry.type == QUERY_TYPE_BACKWARD_SEARCH );
  cleanup_process_entry(&q->proc);
}

void do_backward_search_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  backward_search_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_BACKWARD_SEARCH );
      s = (backward_search_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_BACKWARD_SEARCH );
      s = (backward_search_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

  
#define BLK_REQUEST(nqx,statex,type,blocknum,chr,row)  \
{ \
  setup_block_query(nqx, \
                    s->proc.entry.loc, \
                    statex, \
                    & s->proc, \
                    type, \
                    blocknum, chr, row, 0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}

#define HDR_REQUEST_OCCS(nqx,statex,type,row,chr)  \
{ \
  setup_header_occs_query(nqx, \
                          s->proc.entry.loc, \
                          statex, \
                          &s->proc,\
                          type, 0,chr,row,0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}


  /* The normal algorithm:

     first = C[ch] + Occ(ch, first - 1)
     last = C[ch] + Occ(ch, last) - 1
   */

  while( 1 ) {
   switch (s->proc.entry.state) {
    case 0:
      // Request C[ch] and which block it's in and block occs.
      // request the block's part of occs.
      if( s->first_in == 0 ) {
        HDR_REQUEST_OCCS(&s->hq1, 0x202,
                         HDR_REQUEST_C,
                         0, s->ch);
      } else {
        HDR_REQUEST_OCCS(&s->hq1, 0x202,
                         HDR_BSEARCH_BLOCK_ROWS |
                         HDR_REQUEST_C |
                         HDR_REQUEST_BLOCK_OCCS |
                         HDR_BACK,
                         s->first_in - 1, s->ch);
      }

      HDR_REQUEST_OCCS(&s->hq2, 0x220,
                       HDR_BSEARCH_BLOCK_ROWS |
                       HDR_REQUEST_C |
                       HDR_REQUEST_BLOCK_OCCS |
                       HDR_BACK,
                       s->last_in, s->ch);
      //return to wait for responses
      WAIT_STATE(0x200);
    case 0x200:
    case 0x202:
    case 0x220:
      // wait for both responses for the header requests.
      CHECK_RESPONSE(hq);
      s->proc.entry.state |= qe->state;

      if( s->proc.entry.state != 0x222 ) WAIT; // continue to wait
      // fall through
    case 0x222:
      // request Occ(ch, Last)
      BLK_REQUEST(&s->bq2, 0x330,
              BLOCK_REQUEST_OCCS,
              s->hq2.r.block_num, s->ch, s->last_in - s->hq2.r.row);

      // request Occ(ch, First - 1)
      if( s->first_in == 0 ) {
        s->bq1.r.occs_in_block = 0;
        s->proc.entry.state = 0x303;
      } else {
        BLK_REQUEST(&s->bq1, 0x303,
                BLOCK_REQUEST_OCCS,
                s->hq1.r.block_num, s->ch, s->first_in - 1 - s->hq1.r.row);
        s->proc.entry.state = 0x300;
      }

      // Return so we wait for the rest of the requests.
      WAIT;
    case 0x300:
    case 0x303:
    case 0x330:
      CHECK_RESPONSE(bq);
      s->proc.entry.state |= qe->state;

      if( s->proc.entry.state != 0x333 ) WAIT; // continue to wait
      // fall through
    case 0x333:
      // add in the value from the header
      s->first = 0;
      s->last = 0;

      s->first += s->hq1.r.occs;
      s->first += s->bq1.r.occs_in_block;

      // we got the C[ch] + Occ(ch, Last). Add it to last and subtract 1.
      s->last += s->hq2.r.occs;
      s->last += s->bq2.r.occs_in_block;
      s->last--;

      // Return the result!
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
   }
  }
 
#undef HDR_REQUEST_OCCS
#undef BLK_REQUEST

}

/* I havn't figure out how to make forward-search work on the FM-index.
 * While it would be nice, and I believe that it's possible, 
 * this problem must be saved for later.
 */



error_t setup_get_doc_info_query(
        get_doc_info_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t doc_id)
{
  error_t err;

  memset(q, 0, sizeof(get_doc_info_query_t));

  err = setup_process_entry( & q->proc, 
                             // query entry params
                             QUERY_TYPE_GET_URL, loc,
                             0, requestor,
                             // proc callback
                             do_get_doc_info_query);
  if( err ) return err;

  q->doc_id = doc_id;
  q->info_len = 0;
  q->info = NULL;

  setup_header_loc_query( &q->hq, 
                            q->proc.entry.loc,
                            0x0,
                            &q->proc,
                            HDR_LOC_REQUEST_DOC_INFO,
                            0, q->doc_id);

  return ERR_NOERR;
}

void cleanup_get_doc_info_query(get_doc_info_query_t* q)
{
  assert( q->proc.entry.type == QUERY_TYPE_GET_URL );

  cleanup_header_loc_query( &q->hq );

  free( q->info );
  cleanup_process_entry(&q->proc);
}

void do_get_doc_info_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  // back up to search...
  get_doc_info_query_t* s = NULL;
  header_loc_query_t* hq = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_GET_URL );
      s = (get_doc_info_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_LOC ) {
        hq = (header_loc_query_t*) qe;
      } else assert(0);
      s = (get_doc_info_query_t*) qe->parent;
      assert( s->proc.entry.type == QUERY_TYPE_GET_URL );
      break;
    default:
      assert(0);
      break;
  }

  /* The normal algorithm:
     ask for the doc info for doc id
  */

  while (1) {
    switch( s->proc.entry.state ) {
      case 0x0:
        // do the query.
        err = schedule_query(ss, (query_entry_t*) &s->hq);
        CHECK_ERROR(err);
        WAIT_STATE(0x1); // wait for the response.
      case 0x1: // got the response for the string query.
        CHECK_RESPONSE(hq);
        // move the result.
        s->info_len = hq->r.doc_info_len;
        s->info = hq->r.doc_info;
        hq->r.doc_info = NULL;
        // return
        RETURN_RESULT;
      default:
        assert(0);
    }
  }

}


void regexp_result_free(regexp_result_t* v)
{
  free(v->match);
  v->match = NULL;
}
error_t regexp_result_copy(regexp_result_t* dst, regexp_result_t* src)
{
  dst->first = src->first;
  dst->last = src->last;
  dst->cost = src->cost;
  dst->match_len = src->match_len;
  dst->match = malloc(src->match_len * sizeof(alpha_t));
  if( ! dst->match ) return ERR_MEM;
  memcpy(dst->match, src->match, src->match_len * sizeof(alpha_t));
  return ERR_NOERR;
}

hash_t regexp_queue_hash(const void* vPtr)
{
  const regexp_result_t *v = (const regexp_result_t*) vPtr;
  const void* start = &v->first;
  const void* end = &v->last;
  size_t len = end - start + sizeof(int64_t);
  if( EXTRA_CHECKS ) assert(len == 16);
  return hash_data(start, len);
}

int regexp_queue_cmp(const void* aPtr, const void* bPtr)
{
  const regexp_result_t *a = (const regexp_result_t*) aPtr;
  const regexp_result_t *b = (const regexp_result_t*) bPtr;

  if( a->first < b->first ) return -1;
  else if ( a->first > b->first ) return 1;
  else {
    if( a->last < b->last ) return -1;
    else if ( a->last > b->last ) return 1;
    else return 0;
  }
}

error_t setup_regexp_query_take_nfa(
        regexp_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        nfa_description_t* nfa)
{
  error_t err;

  memset(q, 0, sizeof(regexp_query_t));

  assert(nfa);

  err = setup_process_entry( & q->proc, 
                       // query entry params
                       QUERY_TYPE_REGEXP, loc,
                       0, requestor,
                       // proc callback
                       do_regexp_query);
  if( err ) return err;

  q->nfa = nfa;

  // Setup the rest of the needed materials.
  q->r_c = allocate_bit_array(CHARACTER_SET_CHUNKS);
  q->nfa_states = malloc(nfa->num_nodes* sizeof(nfa_errcnt_t));
  q->tmp_states = malloc(nfa->num_nodes* sizeof(nfa_errcnt_t));
  if( ! (q->r_c && q->nfa_states && q->tmp_states) ) {
    err = ERR_MEM;
    goto error;
  }

  memset(&q->results, 0, sizeof(regexp_result_list_t));
  memset(q->new_range, 0, ALPHA_SIZE * sizeof(regexp_result_t));
 
  err = queue_map_create(&q->queue,
                         regexp_queue_hash,
                         regexp_queue_cmp);
  if( err ) goto error;

  return ERR_NOERR;

error:
  free( q->r_c );
  q->r_c = NULL;
  free( q->nfa_states );
  q->nfa_states = NULL;
  free( q->tmp_states );
  q->tmp_states = NULL;
  return err;
}

error_t setup_regexp_query(
        regexp_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        struct ast_node* ast_node)
{
  error_t err;
  nfa_description_t* nfa = NULL;

  // Compile the NFA
  nfa = malloc(sizeof(nfa_description_t));
  if( ! nfa ) return ERR_MEM;

  // reverse the AST
  reverse_regexp(ast_node);

  // compile the AST into an NFA
  err = compile_regexp_from_ast(nfa, ast_node);
  if( err ) return err;
  if( DEBUG > 1 ) {
    printf("AST:\n");
    print_ast_node(stdout, ast_node, 0);
    printf("produced NFA:\n");
    print_nfa(stdout, nfa);
  }

  assert(nfa);

  err = setup_regexp_query_take_nfa(q, requestor, loc, nfa);
  if( err ) return err;

  return ERR_NOERR;
}

void cleanup_regexp_query(regexp_query_t* q ) 
{
  hm_entry_t temp;

  assert( q->proc.entry.type == QUERY_TYPE_REGEXP );
  free(q->r_c);
  q->r_c = NULL;
  free(q->nfa_states);
  q->nfa_states = NULL;
  free(q->tmp_states);
  q->tmp_states = NULL;

  while (queue_map_pop( &(q->queue), &temp) ) {
    regexp_result_t* var = temp.key;
    nfa_errcnt_t* got_states = temp.value;
    regexp_result_free(var);
    free(var);
    free(got_states);
  }

  queue_map_destroy(&q->queue);

  for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
    // free anything that was there..
    regexp_result_free(& q->new_range[chr]);
  }


  for(int i = 0; i < q->results.num_results; i++ ) {
    //printf("cleanup freeing %p %p\n", &q->results.results[i], q->results.results[i].match);
    regexp_result_free(& q->results.results[i]);
  }
  free(q->results.results);
  q->results.results = NULL;
  q->results.num_results = 0;
  free_nfa_description( q->nfa );
  free(q->nfa);
  cleanup_process_entry(&q->proc);
}

error_t regexp_result_list_append(regexp_result_list_t* results, regexp_result_t* new_result)
{
    return append_array(&results->num_results, &results->results, sizeof(regexp_result_t), new_result);
}

// sort by first, reverse sort by last.
int regexp_result_cmp(const void* aPtr, const void* bPtr)
{
  regexp_result_t *a = (regexp_result_t*) aPtr;
  regexp_result_t *b = (regexp_result_t*) bPtr;

  if( a->first < b->first ) return -1;
  else if ( a->first > b->first ) return 1;
  else {
    if( a->last < b->last ) return 1;
    else if ( a->last > b->last ) return -1;
    else return 0;
  }
}
void regexp_result_freef(void* aPtr)
{
  regexp_result_t *a = (regexp_result_t*) aPtr;
  //printf("dedup freeing %p %p\n", a, a->match);
  regexp_result_free(a);
}

void regexp_result_list_sort(regexp_result_list_t* list)
{
  list->num_results = sort_dedup_free(list->results, list->num_results,
                                 sizeof(regexp_result_t), regexp_result_cmp, regexp_result_freef);

  // If there are any results that contain other results, we
  // need to get rid of them, so that we can get an accurate count.
  if( list->num_results > 0 ) {
    int64_t first, last;
    int i;
    first = list->results[0].first;
    last = list->results[0].last;
    i = 1;
    for( int k = 1; k < list->num_results; k++ ) {

      // Is it entirely subsumed by first..last?
      if( list->results[k].first >= first &&
          list->results[k].last <= last ) {
        // we get rid of this match since it's inside
        // one of the other matches.
        regexp_result_free(& list->results[k]);
        continue;
      }

      // Otherwise, we copy.
      first = list->results[k].first;
      last = list->results[k].last;
      if( i == k ) {
        i++;
        continue;
      }
      list->results[i] = list->results[k];
      i++;
    }
    list->num_results = i;
  }
}

/** 
  * This function will:
  *  - copy new_result
  *  - copy nfa_states
  */
error_t add_mapping (nfa_description_t* nfa,
                     queue_map_t* queue,
                     regexp_result_t* new_result,
                     nfa_errcnt_t* nfa_states)
{
  error_t err;

  if (new_result->last < new_result->first) {
    return ERR_NOERR;
  }

  // if there are epsilon transitions, you should 
  // follow them but the way we have implemented 
  // the algorithm negates our need to use them
  // (since we transform all NFAs to not have epsilon
  //  transitions)

  if( DEBUG > 4 ) {
    printf("add_mapping of \n");
    nfa_print_states(stdout, " -- ", nfa, nfa_states);
    printf(" ++ to [%lli, %lli] ",
           (long long int) new_result->first,
           (long long int) new_result->last);
    fprint_alpha(stdout, new_result->match_len, new_result->match);
    printf("\n");
  }

  // we used to check here if it was in a final state,
  // but now we do that in do-regexp-search so that
  // we can use the mapping to remove all duplicate
  // final returns.
  {
    // it's not in a final state, so we actually add the mapping!
    // first, check to see if the mapping exists in the queue
    regexp_result_t* key;
    nfa_errcnt_t* value;
    hm_entry_t entry;

    entry.key = new_result;
    entry.value = NULL;
    if( queue_map_retrieve(queue, &entry) ) { 
      // update the stored value in the queue map
      key = entry.key;
      value = entry.value;
      nfa_states_union(nfa->num_nodes, value, value, nfa_states);
      if( new_result->match_len > key->match_len ) {
        free(key->match);
        key->match = malloc(new_result->match_len * sizeof(alpha_t));
        memcpy(key->match, new_result->match, new_result->match_len * sizeof(alpha_t));
        key->match_len = new_result->match_len;
      }
    } else {
      // otherwise, add the entry to the queue map.
      // set the key to a copy of {new_first, new_last}
      // set the value to the nfa_states.
      key = malloc(sizeof(regexp_result_t));
      //printf("Add mapping allocated %p\n", key);
      value = malloc(nfa->num_nodes * sizeof(nfa_errcnt_t));
      if( ! (key && value)  ) return ERR_MEM;
      err = regexp_result_copy(key, new_result);
      if( err ) {
        free( key );
        free( value );
        return err;
      }

      memcpy(value, nfa_states, nfa->num_nodes * sizeof(nfa_errcnt_t));

      entry.key = key;
      entry.value = value;

      // add it to the queue.
      err = queue_map_push(queue, &entry);
      if( err ) return err;
    }
  }

  return ERR_NOERR;
}


void do_regexp_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  regexp_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  header_loc_query_t* loc_q = NULL;
  error_t err = ERR_NOERR;
  
  switch ( mode )
    {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_REGEXP );
      s = (regexp_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_HEADER_LOC ) {
        loc_q = (header_loc_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_REGEXP );
      s = (regexp_query_t*) qe->parent;
      break;
    default:
      assert(0);
    }
  
  
#define BLK_REQUEST(nqx,statex,type,blocknum,chr,row)   \
  {                                                     \
    setup_block_query(nqx,                              \
                      s->proc.entry.loc,                \
                      statex,                           \
                      & s->proc,                        \
                      type,                             \
                      blocknum, chr, row, 0);           \
    err = schedule_query(ss, (query_entry_t*) nqx);     \
    CHECK_ERROR( err )                                  \
  }
  
#define HDR_REQUEST_OCCS(nqx,statex,type,row,chr)       \
  {                                                     \
    setup_header_occs_query(nqx,                        \
                            s->proc.entry.loc,          \
                            statex,                     \
                            &s->proc,                   \
                            type, 0,chr,row,0);         \
    err = schedule_query(ss, (query_entry_t*) nqx);     \
    CHECK_ERROR( err )                                  \
  }

  /* M is a mapping ( [first,last] -> set of NFA states with # errors
   * a set of NFA states stores # errors to get there,
   * so 0 is a perfect match; maxerr is not a match.
   *
   * new_range[256] stores the new range for every letter
   *                (errors are not allowed in the special FEMTO-inserted
   *                 characters, such as EOF).
   *
   * approx_simulate_nfa {
   *   // 0x000,0x001
   *   add the entry ( [0,n-1] -> set of start states )
   *
   *   while M is not empty {
   *     // 0x010
   *     pop an entry ( [first, last] -> nfa_states ) from M
   *
   *     // handle deletions
   *     tmp_states = states reachable from nfa_states after reading 
   *                  all characters and adding one deletion error to states
   *     nfa_states = nfa_states UNION tmp_states
   *
   *     compute min_state = min(nfa_states)
   *
   *     // compute which characters are reachable with zero-cost
   *     // transitions or within our budget of insert/subst
   *     reachable_ch = characters reachable from nfa_states
   *     if min(min_state+subst_cost,min_state+insert_cost) < maxerr
   *       reachable_ch |= 0..255 ie all normal characters
   *
   *     // 0x020-0x500 MAIN FOR LOOP
   *     // compute the 'new_range's
   *     for every character ch in reachable_ch
   *       new_first = C[ch] + Occ(ch, first - 1)
   *       new_last = C[ch] + Occ(ch, last) - 1
   *       new_range[ch] = [new_first, new_last]
   *     }
   *
   *     // 0x510
   *
   *     // handle substitutions
   *     tmp_states = states reachable from nfa_states after reading all
   *                  characters and adding one subst. error to states
   *     for every character ch with a valid new_range[ch] {
   *       add_mapping( new_range[ch] -> tmp_states )
   *     }
   *
   *     // handle regular transitions
   *     for every character ch reachable from nfa_states {
   *       tmp_states = states reachable from nfa_states after reading ch
   *       add_mapping( new_range[ch] -> tmp_states )
   *     }
   *
   *     // handle insertions
   *     tmp_states = nfa_states with one error added to each state
   *     for every character ch with a valid new_range[ch] {
   *       add_mapping( new_range[ch] -> tmp_states )
   *     }
   *   }
   * }
   */
  while (1) {
    switch( s->proc.entry.state )
      {
      case 0x000: // make the request for n-1
        {
          // find n-1.
          setup_header_loc_query(&s->loc_query,
                                 s->proc.entry.loc,
                                 0x001,
                                 &s->proc,
                                 HDR_LOC_REQUEST_L_SIZE, 0, 0);

          err = schedule_query(ss, (query_entry_t*) &s->loc_query);
          CHECK_ERROR(err);
          WAIT_STATE(0x001); // wait for the response to get the L column size.
        }
      case 0x001: // make the entry
        CHECK_RESPONSE(loc_q);

        cleanup_header_loc_query(loc_q);

        {
          int64_t n = loc_q->r.doc_len;
          regexp_result_t new_result;

          // copy the start states into bit_array
          approx_get_start_states(s->nfa, s->nfa_states);

          new_result.first = 0;
          new_result.last = n-1;
          new_result.match_len = 0;
          new_result.match = NULL;

          s->num_iterations = 0;

          // add a mapping with the initial range of the entire index
          // and with the start states
          err = add_mapping( s->nfa, &s->queue, &new_result, s->nfa_states);
          CHECK_ERROR( err );

          // fallthrough
        }
      case 0x010: //begin while loop (while the queue is not empty)
        {
          hm_entry_t temp;

          // Stop if we've done too much work.
          if( s->num_iterations > MAX_REGEXP_ITERATIONS ) {
            RETURN_ERROR(ERR_OVERWORKED);
          }

          // pop the next entry from the queue
          if (queue_map_pop( &(s->queue), &temp) ) {
            regexp_result_t* var = temp.key;
            nfa_errcnt_t* got_states = temp.value;
            nfa_errcnt_t min_err;
            nfa_errcnt_tmp_t cost;

            // Check if it's a final state.
            if( approx_is_final_state(s->nfa, got_states, &cost) ) {
              var->cost = cost;

              if( DEBUG > 1 ) {
                printf("regexp search result: ");
                printf(" cost %i", (int) cost);
                printf(" to [%lli, %lli] ",
                       (long long int) var->first,
                       (long long int) var->last);
                fprint_alpha(stdout, var->match_len, var->match);
                printf("\n");
              }

              err = regexp_result_list_append(&s->results, var);
              CHECK_ERROR( err );

              free(got_states);
              got_states = NULL;
              var->match = NULL; // we put it in the results list!
              free(var);


              s->proc.entry.state = 0x010; // go back to popping elements.
              break; // try popping another entry.
            }

            // handle deletions.

            // Find states reachable from nfa_states after reading
            //  all characters and adding one deletion error to states.
            approx_get_reachable_states_allchars(s->nfa,
                                          s->nfa->settings.delete_cost,
                                          got_states,
                                          s->tmp_states);
            // Union those states in...
            nfa_states_union(s->nfa->num_nodes, s->nfa_states, got_states, s->tmp_states);

            free(got_states);
            got_states = NULL;

            min_err = s->nfa->settings.cost_bound;
            for(int i = 0; i < s->nfa->num_nodes; i++ ) {
              min_err = NFA_MIN(min_err, s->nfa_states[i]);
            }
            min_err = NFA_MIN(min_err + s->nfa->settings.subst_cost,
                              min_err + s->nfa->settings.insert_cost);

            // Now, compute the set of reachable characters.
            approx_get_reachable_characters( s->nfa, s->nfa_states, s->r_c);
            if( min_err < s->nfa->settings.cost_bound &&
                s->num_iterations > 0 ) {
              // Only add all 0..255 characters if we're not at
              // the first iteration (since they could be added by
              // being substrings) and if we're doing approximate
              // search and there's some errors possibly left.
              // We can reach all characters 0..255
              for( int i = CHARACTER_OFFSET; i < ALPHA_SIZE; i++ ) {
                set_bit(CHARACTER_SET_CHUNKS, s->r_c, i);
              }
            }

            // store current first and last
            s->first = var->first;
            s->last = var->last;

            // clean up new_range structure.
            for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
              // free anything that was there..
              regexp_result_free(& s->new_range[chr]);

              s->new_range[chr].first = -1;
              s->new_range[chr].last = -2;
              s->new_range[chr].cost = MAX_NFA_ERRCNT;
              s->new_range[chr].match_len = 0;
              s->new_range[chr].match = NULL;
            }

            // allocate all of the new_range guys.
            for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
              if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
                size_t newlen = var->match_len + 1;
                s->new_range[chr].match_len = newlen;
                s->new_range[chr].match = malloc(newlen*sizeof(alpha_t));

                if( ! s->new_range[chr].match ) err = ERR_MEM;
                else err = ERR_NOERR;

                CHECK_ERROR( err );

                memcpy(&s->new_range[chr].match[1], var->match, var->match_len * sizeof(alpha_t));
                s->new_range[chr].match[0] = chr;
              }
            }

            // free var and var->match
            regexp_result_free(var);
            free(var);
          } else { // if the pop didn't work, the queue was empty
            if( DEBUG > 3 ) {
              printf("do_regexp_query returning with %i results:\n",
                     s->results.num_results);
              for( int i = 0; i < s->results.num_results; i++ ) {
                printf(" [%lli,%lli]\n",
                    (long long int) s->results.results[i].first,
                    (long long int) s->results.results[i].last);
              }
            }

            // sort the results.
            regexp_result_list_sort(&s->results);

            RETURN_RESULT;
          }

          // fallthrough
        }

      case 0x200:// start of MAIN FOR LOOP (for each reachable ch)
        // Request all header requests.
        s->num_hq1_requested = 0;
        s->num_hq1_got = 0;
        s->num_hq2_requested = 0;
        s->num_hq2_got = 0;

        for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
          if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
            HDR_REQUEST_OCCS(&s->parts[chr].hq2, 0x310,
                           HDR_BSEARCH_BLOCK_ROWS |
                           HDR_REQUEST_C |
                           HDR_REQUEST_BLOCK_OCCS |
                           HDR_BACK,
                           s->last, chr);
            s->num_hq2_requested++;

            if( s->first == 0 ) {
              HDR_REQUEST_OCCS(&s->parts[chr].hq1, 0x301,
                               HDR_REQUEST_C,
                               0, chr);
              s->parts[chr].hq1.r.row = 0;
              s->num_hq1_requested++;
            } else {
              HDR_REQUEST_OCCS(&s->parts[chr].hq1, 0x301,
                               HDR_BSEARCH_BLOCK_ROWS |
                               HDR_REQUEST_C |
                               HDR_REQUEST_BLOCK_OCCS |
                               HDR_BACK,
                               s->first - 1, chr);
              s->num_hq1_requested++;
            }
          }
        }

        WAIT_STATE(0x300); //wait for the results
        // ( there is a return in WAIT_STATE)

      case 0x300:
        // we got one of the header responses.
        // handle an error.
        CHECK_RESPONSE(hq);

        if( hq->leaf.entry.state == 0x301 ) s->num_hq1_got++;
        else if( hq->leaf.entry.state == 0x310 ) s->num_hq2_got++;
        else assert(0); // programming error!

        if( s->num_hq1_got < s->num_hq1_requested ||
            s->num_hq2_got < s->num_hq2_requested ) {
          WAIT; // wait for the other result
        }
        // fall through if we've collected them all!
      case 0x311:
        // we have both header queries done.
        // do the block queries.
        s->num_bq1_requested = 0;
        s->num_bq1_got = 0;
        s->num_bq2_requested = 0;
        s->num_bq2_got = 0;

        for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
          if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
            BLK_REQUEST(&s->parts[chr].bq2, 0x410,
                        BLOCK_REQUEST_OCCS,
                        s->parts[chr].hq2.r.block_num,
                        chr, s->last - s->parts[chr].hq2.r.row);
            s->num_bq2_requested++;

            if(s->first == 0 ) {
              // special case to handle when getting
              // Occ(ch, -1), which should always be 0.
              s->parts[chr].bq1.r.occs_in_block = 0;
            } else {
              BLK_REQUEST(&s->parts[chr].bq1, 0x401,
                          BLOCK_REQUEST_OCCS,
                          s->parts[chr].hq1.r.block_num,
                          chr, s->first - 1 - s->parts[chr].hq1.r.row);
              s->num_bq1_requested++;
            }
          }
        }

        WAIT_STATE(0x400); // wait for the results
        // ( there is a return in WAIT_STATE)
      case 0x400:
      case 0x401:
      case 0x410:
        CHECK_RESPONSE(bq);

        if( bq->leaf.entry.state == 0x401 ) s->num_bq1_got++;
        else if( bq->leaf.entry.state == 0x410 ) s->num_bq2_got++;
        else assert(0); // programming error!

        if( s->num_bq1_got < s->num_bq1_requested ||
            s->num_bq2_got < s->num_bq2_requested ) {
          WAIT; // wait for the other result.
        }
        // fall through
      case 0x411:  // compute the new first and last.
        for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
          if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
            // first = C[ch] + Occ(ch, first - 1)
            s->new_range[chr].first = s->parts[chr].hq1.r.occs + s->parts[chr].bq1.r.occs_in_block;
            // last = C[ch] + Occ(ch, last) - 1
            s->new_range[chr].last = s->parts[chr].hq2.r.occs + s->parts[chr].bq2.r.occs_in_block - 1;
          }
        }
        // fall through
      case 0x510: // end of MAIN FOR LOOP (for each reachable ch)
        {
          // handle substitutions.
          // set tmp_states = states reachable after adding a subst error
          approx_get_reachable_states_allchars(s->nfa, s->nfa->settings.subst_cost,
                                        s->nfa_states, s->tmp_states);


          // For every character with a valid new range, add a mapping.
          for( alpha_t chr = CHARACTER_OFFSET; chr < ALPHA_SIZE; chr++ ) {
            if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
              // add_mapping( new_range[chr] -> tmp_states );
              err = add_mapping( s->nfa, &s->queue, & s->new_range[chr], s->tmp_states);
              CHECK_ERROR( err );
            }
          }

          // handle regular (non-error) transitions.
          for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
            if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
              // tmp_states = states reachable from nfa_states after reading ch
              approx_get_reachable_states(s->nfa, chr, s->nfa_states, s->tmp_states);
              err = add_mapping( s->nfa, &s->queue, & s->new_range[chr], s->tmp_states);
              CHECK_ERROR( err );
            }
          }

          // handle insertions
          // set tmp_states = states with one error added to each state
          approx_add_error_allchars(s->nfa, s->nfa->settings.insert_cost,
                             s->nfa_states, s->tmp_states);
          for( alpha_t chr = 0; chr < ALPHA_SIZE; chr++ ) {
            if ( get_bit(CHARACTER_SET_CHUNKS, s->r_c, chr) ) {
              // add_mapping( new_range[chr] -> tmp_states );
              err = add_mapping( s->nfa, &s->queue, & s->new_range[chr], s->tmp_states);
              CHECK_ERROR( err );
            }
          }

          /*
          if( DEBUG > 7 ) {
            printf("do_regexp_query: range [%"PRIi64",%"PRIi64"] for states", 
                   s->new_first, s->new_last);
            print_bit_array(s->nfa->bit_array_len, bit_array, s->nfa->num_nodes);
            printf("\n");
          }*/
          
          // increment our number of iterations.
          s->num_iterations++;
          s->proc.entry.state = 0x010; // go back to popping elements.
          break;
        }

      default:
        assert(0);
    }
  }
#undef BLK_REQUEST
#undef HDR_REQUEST_OCCS

}

error_t setup_back_query(
        back_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t row,
        locate_type_t doLocate)
{
  error_t err;

  memset(q, 0, sizeof(back_query_t));

  err = setup_process_entry( &q->proc,
                       // query_entry params
                       QUERY_TYPE_BACK, loc,
                       0, requestor,
                       // process_entry params
                       do_back_query);
  if( err ) return err;
  
  // copy over the request info.
  q->row = row;
  q->doLocate = doLocate;
  
  // make sure results are nulled out.
  q->offset = INVALID_OFFSET;

  return ERR_NOERR;
}

error_t reset_back_query(
        back_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t row,
        locate_type_t doLocate)
{
  error_t err;

  err = reset_process_entry( &q->proc,
                             // query_entry params
                             QUERY_TYPE_BACK, loc,
                             0, requestor,
                             // process_entry params
                             do_back_query);
  if( err ) return err;
 
  // copy over the request info.
  q->row = row;
  q->doLocate = doLocate;
  
  // make sure results are nulled out.
  q->offset = INVALID_OFFSET;

  return ERR_NOERR;
}

void cleanup_back_query(back_query_t* q ) 
{
  cleanup_process_entry(&q->proc);
}

void do_back_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  // back up to search...
  back_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  error_t err;


  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_BACK );
      s = (back_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_BACK );
      s = (back_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }


#define BLK_REQUEST(nqx,statex,type,blocknum,chr,row)  \
{ \
  setup_block_query(nqx, \
                    s->proc.entry.loc, \
                    statex, \
                    & s->proc, \
                    type, \
                    blocknum, chr, row, 0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ) \
}

#define HDR_REQUEST_OCCS(nqx,statex,type,blocknum,chr,row)  \
{ \
  setup_header_occs_query(nqx, \
                          s->proc.entry.loc, \
                          statex, \
                          &s->proc, \
                          type, blocknum,chr,row,0); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ) \
}

  /* The normal algorithm:
   chr = L[row]
   row = C[chr] + Occ(chr, row) - 1;
  */

  switch ( s->proc.entry.state ) {
    case 0x0:
      // initialize things.
      HDR_REQUEST_OCCS(&s->hq, 0x1, HDR_BSEARCH_BLOCK_ROWS, 0, 0, s->row);
      WAIT_STATE(0x1); // wait for the response
    case 0x1:
      // we've got hq
      CHECK_RESPONSE(hq);

      // get the next character.
      // make an occs request.

      // we're going to request occs for s->r.
      if( DEBUG > 10 ) {
        printf("Back query will request row %i in block %i\n", (int) s->row, (int) hq->r.block_num);
      }

      BLK_REQUEST(&s->bq, 0x11,
                  BLOCK_REQUEST_CHAR |
                  BLOCK_REQUEST_OCCS |
                  BLOCK_REQUEST_LOCATION,
                  s->hq.r.block_num, INVALID_ALPHA,
                  s->row - s->hq.r.row);

      WAIT_STATE(0x10); // wait for the response
    case 0x10:
      // got a response to the block query.
      CHECK_RESPONSE(bq);

      // it's a block response.
      // save any context that was requested.
      s->chr = bq->r.ch;

      s->row = bq->r.occs_in_block - 1;

      if( s->doLocate != LOCATE_NONE ) {
        s->offset = bq->r.offset;
      }

      if( DEBUG > 10 ) {
        printf("Back query will request C[%i]\n", bq->r.ch);
      }

      // make the request for the C[ch] and for the block occs.
      HDR_REQUEST_OCCS(&s->hq, 0x111,
                       HDR_REQUEST_C | HDR_REQUEST_BLOCK_OCCS | HDR_BACK, 
                       s->hq.r.block_num, s->chr, 0 );


      WAIT_STATE(0x100); // wait for response to header request.
    case 0x100:
      CHECK_RESPONSE(hq);

      // it's a header response.
      // response to block_occs.
      // response to get C
      s->row += hq->r.occs;
      if( s->chr <= ESCAPE_CODE_SEOF ) {
        // we can't go back beyond the end of file marker
        // because the order of documents in this region is 
        // indeterminate.
        s->row = -1; // invalid row!
      }

      s->proc.entry.state = 0x111;
      // fall through.
    case 0x111:
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);

  }
#undef HDR_REQUEST_OCCS
#undef BLK_REQUEST

}


error_t setup_forward_query(
        forward_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t row,
        locate_type_t doLocate)
{
  error_t err;

  memset(q, 0, sizeof(forward_query_t));

  err = setup_process_entry( &q->proc,
                             // params for query entry
                             QUERY_TYPE_FORWARD, loc,
                             0, requestor,
                             // params for proc.
                             do_forward_query);
  if( err ) return err;

  // copy over the request info.
  q->row = row;
  q->doLocate = doLocate;
  
  // make sure results are nulled out.
  q->offset = INVALID_OFFSET;

  return ERR_NOERR;
}

error_t reset_forward_query(
        forward_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t row,
        locate_type_t doLocate)
{
  error_t err;

  err = reset_process_entry( &q->proc,
                             // params for query entry
                             QUERY_TYPE_FORWARD, loc,
                             0, requestor,
                             // params for proc.
                             do_forward_query);
       
  // copy over the request info.
  q->row = row;
  q->doLocate = doLocate;
  
  // make sure results are nulled out.
  q->offset = INVALID_OFFSET;

  return ERR_NOERR;
}

void cleanup_forward_query(forward_query_t* q ) 
{
  cleanup_process_entry(&q->proc);
}


void do_forward_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  // back up to search...
  forward_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  error_t err;


  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_FORWARD );
      s = (forward_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_FORWARD );
      s = (forward_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }


#define BLK_REQUEST(nqx,statex,type,blocknum,chr,row,occs)  \
{ \
  setup_block_query(nqx, \
                    s->proc.entry.loc, \
                    statex, \
                    & s->proc, \
                    type, \
                    blocknum, chr, row, occs); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}

#define HDR_REQUEST_OCCS(nqx,statex,type,chr,row,occs)  \
{ \
  setup_header_occs_query(nqx, \
                          s->proc.entry.loc, \
                          statex, \
                          &s->proc, \
                          type, \
                          0,chr,row,occs); \
  err = schedule_query(ss, (query_entry_t*) nqx); \
  CHECK_ERROR( err ); \
}

  /* The normal algorithm:
   chr = the first character of our row, from binary searching C.
   row = the row number of the (row + 1 - C[chr])'th occurence of chr in L.
  */

  switch ( s->proc.entry.state ) {
    case 0x0:
      // initialize things.
      s->proc.entry.state = 0x1;
      if( DEBUG > 5 ) {
        printf("Forward query %p starting\n", s);
      }
      // fall through
    case 0x1: // while loop starts
      // get the next character.

      // Get the first character of our row.
      HDR_REQUEST_OCCS(&s->hq, 0x11,
                       HDR_BSEARCH_C | HDR_BSEARCH_BLOCK_OCCS |
                       HDR_REQUEST_BLOCK_ROWS | HDR_FORWARD,
                       INVALID_ALPHA, 0, s->row );

      WAIT_STATE(0x10); // wait for the response.
    case 0x10:
      // got response to bsearch_c.
      CHECK_RESPONSE(hq);

      s->chr = hq->r.ch;
      if( s->chr <= ESCAPE_CODE_SEOF ) {
        // we can't forward beyond the end of file marker
        // because the order of documents in this region is 
        // indeterminate.
        s->row = -1;
        RETURN_RESULT;
      }


      // now we know which block number to go after and with how many
      // occs.
      s->occ = hq->r.occs;
      //s->block_number = hq->r.o.block_num;
      s->row = hq->r.row;

      if( DEBUG > 5 ) {
        printf("Forward query looking for %" PRIi64 " 'th occurence of %i in block %" PRIi64 "\n", s->occ, s->chr, hq->r.block_num);
      }

      BLK_REQUEST(&s->bq, 0x1111,
                  BLOCK_REQUEST_ROW |
                  BLOCK_REQUEST_LOCATION,
                  s->hq.r.block_num, s->chr, 0, s->occ);

      WAIT_STATE(0x1000); // wait for the response

    case 0x1000:
      CHECK_RESPONSE(bq);

      if( DEBUG > 5 ) {
        printf("Forward query %" PRIi64 "'th occurence of %i in block %" PRIi64 " is at row %i\n", s->occ, s->chr, s->hq.r.block_num, bq->r.row_in_block);
      }

      // we got a row number.
      s->row += bq->r.row_in_block;

      if( DEBUG > 5 ) {
        printf("Forward query now on row %" PRIi64 "\n", s->row);
      }

      if( s->doLocate != LOCATE_NONE ) {
        s->offset = bq->r.offset;
      }

      s->proc.entry.state = 0x1111;

      // fall through.
    case 0x1111:
      if( DEBUG > 5 ) {
        printf("Forward query %p returning\n", s);
      }
      // all done!
      RETURN_RESULT;
      break;
    default:
      RETURN_ERROR(ERR_INVALID);

  }
#undef HDR_REQUEST
#undef BLK_REQUEST

}

error_t setup_context_query(
        context_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t row,
        int beforeCtxLen, // can be 0 even if locate is 1
        int afterCtxLen, // can be 0 even if locate is 1
        locate_type_t doLocate)
{
  error_t err;

  memset(q, 0, sizeof(context_query_t));

  err = setup_process_entry( &q->proc,
                       QUERY_TYPE_CONTEXT, loc,
                       0, requestor,
                       do_context_query);
  if( err ) return err;

  err = setup_forward_query(&q->forward,
      &q->proc, q->proc.entry.loc, 
      row, LOCATE_WEAK);
  if( err ) return err;

  err = setup_back_query(&q->backward, 
      &q->proc, q->proc.entry.loc,
      row, LOCATE_WEAK);
  if( err ) return err;

  q->doLocate = doLocate;
  q->beforeCtxLen = beforeCtxLen;
  q->afterCtxLen = afterCtxLen;
  // set the initial row
  q->forward.row = row;
  q->backward.row = row;
  // allocate space for context...
  if( beforeCtxLen + afterCtxLen > 0 ) {
    q->context = malloc((1 + beforeCtxLen + afterCtxLen) * sizeof(alpha_t));
    if( ! q->context ) return ERR_MEM;
    memset( q->context, 0, (1 + beforeCtxLen + afterCtxLen) * sizeof(alpha_t));
  } else q->context = NULL;

  return ERR_NOERR;
}

void cleanup_context_query(context_query_t* q)
{
  assert( q->proc.entry.type == QUERY_TYPE_CONTEXT );

  // cleanup forward and back queries.
  cleanup_forward_query(&q->forward);
  cleanup_back_query(&q->backward);

  // free space for context
  free( q->context );
  q->context = NULL;
  cleanup_process_entry(&q->proc);
}


void do_context_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  // back up to search...
  context_query_t* s = NULL;
  forward_query_t* forw = NULL;
  back_query_t* back = NULL;
  error_t err;
#define DEBUG_CTX DEBUG
//#define DEBUG_CTX 10

  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_CONTEXT );
      s = (context_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_FORWARD ) {
        forw = (forward_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BACK ) {
        back = (back_query_t*) qe;
      } else assert(0);
      s = (context_query_t*) qe->parent;
      assert( s->proc.entry.type == QUERY_TYPE_CONTEXT );
      break;
    default:
      assert(0);
      break;
  }

  /* The normal algorithm:
     while( 1 ) {
       if( need more forward letters || locating ) {
         check forwards
       }
       if( need more backwards letters || locating ) {
         check backwards
       }
       if( ! need more backwards && ! need more forwards && done locating)
         break;
     }
  */

  while (1) {
    if( DEBUG_CTX > 5 ) {
      printf("Context query state %lx %i %i\n", s->proc.entry.state, s->forward_i, s->backward_i);
    }
    switch( s->proc.entry.state ) {
      case 0x0:
        // initialize things
        s->offset = INVALID_OFFSET;
        s->forward_i = 0;
        s->backward_i = 0;
        s->proc.entry.state = 0x100;
        // fall through
      case 0x100: // while loop starts.
      case 0x101: // &0x01 --> waiting on forward
      case 0x110: // &0x10 -> waiting on backward
      case 0x111:
        // process received results
        if( forw ) {
          if( DEBUG_CTX > 5 ) {
            printf("Context new forward row is %" PRIi64 "\n", forw->row);
          }
          CHECK_RESPONSE(forw);

          // clear the "waiting for it" bits for forward
          s->proc.entry.state &= 0xff0;
          // the forward query is done!
          // check for offset
          if( forw->offset != INVALID_OFFSET ) {
            s->offset = forw->offset - s->forward_i - 1;
          }
          if( s->forward_i < s->afterCtxLen ) {
            // save the result.
            int idx = s->beforeCtxLen+s->forward_i;
            if( DEBUG_CTX > 5 ) {
              printf("Context (forward) saving character %i at offset %i\n", forw->chr, idx);
            }
            s->context[idx] = forw->chr;
          }
          s->forward_i++;
        } else if( back ) {
          if( DEBUG_CTX > 5 ) {
            printf("Context new backward row is %" PRIi64 "\n", back->row);
          }
          CHECK_RESPONSE(back);

          // clear the "waiting for it" bits for backward
          s->proc.entry.state &= 0xf0f;
          // backward query is done.
          // check for offset
          if( back->offset != INVALID_OFFSET ) {
            s->offset = back->offset + s->backward_i;
          }
          if( s->backward_i < s->beforeCtxLen ) {
            // save the character.
            int idx = s->beforeCtxLen - 1 - s->backward_i;
            if( DEBUG_CTX > 5 ) {
              printf("Context (backward) saving character %i at offset %i\n", back->chr, idx);
            }
            s->context[idx] = back->chr;
          }
          s->backward_i++;
        }

        // now decide what to start.
        if( s->forward.row >= 0 &&
            (s->proc.entry.state & 0x00f) == 0x000 && // not waiting on fw
            (s->forward_i < s->afterCtxLen ||
              (s->doLocate == LOCATE_STRONG &&
               s->offset == INVALID_OFFSET) ) ) {
          if( DEBUG_CTX > 5 ) {
            printf("Context asking forward row %" PRIi64 "\n", s->forward.row);
          }
          // start a forward search query
          err = reset_forward_query(&s->forward,
                                    &s->proc,
                                    s->proc.entry.loc,
                                    s->forward.row,
                                    LOCATE_WEAK);
          CHECK_ERROR(err);
          err = schedule_query(ss, (query_entry_t*) &s->forward);
          CHECK_ERROR(err);
          s->proc.entry.state |= 0x001; // waiting on forwards.
        }
        if( s->backward.row >= 0 &&
            (s->proc.entry.state & 0x0f0) == 0x000 && // not waiting on bw
            (s->backward_i < s->beforeCtxLen ||
              (s->doLocate == LOCATE_STRONG &&
               s->offset == INVALID_OFFSET ) ) ) {
          if( DEBUG_CTX > 5 ) {
            printf("Context asking backward row %" PRIi64 "\n", s->backward.row);
          }
          // start a backward query.
          err = reset_back_query(&s->backward,
                                 &s->proc,
                                 s->proc.entry.loc,
                                 s->backward.row,
                                 LOCATE_WEAK);
          CHECK_ERROR(err);
          err = schedule_query(ss, (query_entry_t*) &s->backward);
          CHECK_ERROR(err);
          s->proc.entry.state |= 0x010; // waiting on backwards
        }

        if( s->proc.entry.state == 0x100 ) {
          // didn't need to query at all! we're done.
          s->proc.entry.state = 0x2;
          // while loop will take us to state 2.
          break; // go to state 2.
        } else {
          WAIT; // wait for the results.
        }
        
      case 0x2: // done while loop.
        s->proc.entry.state = 0x3;
        // fall through
      case 0x3: // state for finishing up by getting final locate infos.
        if( DEBUG_CTX > 10 ) {
          printf("Context query completed with offset %i\n",
                   (int) s->offset );
        }
        RETURN_RESULT;
      default:
        RETURN_ERROR(ERR_INVALID);
    }
  }

}

error_t server_get_block(server_state_t* ss,
                         index_locator_t index_loc, int64_t block_id,
                         server_block_t** block_ptr)
{
  block_locator_t bl;
  block_locator_t* id;
  error_t err;

  bl.block_id = block_id;
  bl.index_loc = index_loc;
  id = & bl;
  CACHE_GET(err, & ss->block_cache, id, *block_ptr);
  ss->stats.block_requests++;

  return err;
}


error_t server_get_header_block(server_state_t* ss,
                                index_locator_t index_loc,
                                server_block_t** block_ptr)
{
  return server_get_block(ss, index_loc,
                          block_id_for_type(INDEX_BLOCK_TYPE_HEADER, 0),
                          block_ptr);
}

static inline void service_process(server_state_t* ss, process_entry_t* qe)
{
  int rc;

  assert( is_process_query(qe->entry.type));

  if( DEBUG_SERVER > 8 ) printf("Evaluating process query %p\n", qe);

  rc = pthread_mutex_lock(&qe->lock);
  assert(!rc);

  // service the query that isn't a block request.
  qe->callback(ss, QUERY_MODE_START, &qe->entry);
  if( qe->entry.err_code && qe->num_pending_requests == 0 ) {
    deliver_result(ss, &qe->entry); // deliver an error result.
  }

  rc = pthread_mutex_unlock(&qe->lock);
  assert(!rc);
}

static inline int leaf_probably_cached(server_state_t* ss, leaf_entry_t* qe)
{
  block_locator_t bl;
  bl.block_id = qe->block_id;
  bl.index_loc = qe->entry.loc;
  return is_cached(&ss->block_cache, &bl);
  // TODO -- check better...
}

static inline void readahead_leaf(server_state_t* ss, leaf_entry_t* qe)
{
  // TODO -- write me
}

static inline void evaluate_leaf(server_state_t* ss, leaf_entry_t* qe)
{
  error_t err;
  server_block_t* block;

  if( DEBUG_SERVER > 8 ) printf("Evaluating leaf query %p\n", qe);

  err = ERR_NOERR;

  switch (qe->entry.type) {
    case QUERY_TYPE_HEADER_OCCS:
      {
        header_occs_query_t* q = (header_occs_query_t*) qe;
        // get the appropriate block
        err = server_get_block(ss, q->leaf.entry.loc, q->leaf.block_id, &block);
        if( err ) {
          qe->entry.err_code = err_code(err);
          return;
        }
        err = header_occs_request(&block->u.header_block, q->type, &q->r);
      }
      break;
    case QUERY_TYPE_HEADER_LOC:
      {
        header_loc_query_t* q = (header_loc_query_t*) qe;
        // get the appropriate block
        err = server_get_block(ss, q->leaf.entry.loc, q->leaf.block_id, &block);
        if( err ) {
          qe->entry.err_code = err_code(err);
          return;
        }
        err = header_loc_request(&block->u.header_block, q->type, &q->r);
      }
      break;
    case QUERY_TYPE_BLOCK:
      {
        block_query_t* q = (block_query_t*) qe;
        // get the appropriate block
        err = server_get_block(ss, q->leaf.entry.loc, q->leaf.block_id, &block);
        if( err ) {
          qe->entry.err_code = err_code(err);
          return;
        }
        err = block_request(&block->u.data_block, q->type, &q->r);
      }
      break;
    case QUERY_TYPE_BLOCK_CHUNK:
      {
        block_chunk_query_t* q = (block_chunk_query_t*) qe;
        // get the appropriate block
        err = server_get_block(ss, q->leaf.entry.loc, q->leaf.block_id, &block);
        if( err ) {
          q->leaf.entry.err_code = err_code(err);
          return;
        }
        err = block_chunk_request(&block->u.data_block, q->type, &q->r);
      }
      break;
#if SUPPORT_INDEX_MERGE
    case QUERY_TYPE_BLOCK_UPDATE_MANAGE:
      {
        block_update_manage_query_t* q = (block_update_manage_query_t*) qe;
        err = block_update_manage_request(q->leaf.entry.loc, block_number_from_id(q->leaf.block_id), q->type, &q->r);
      }
      break;
    case QUERY_TYPE_BLOCK_UPDATE:
      {
        block_update_query_t* q = (block_update_query_t*) qe;
        err = server_get_block(ss, q->leaf.entry.loc, q->leaf.block_id, &block);
        if( err ) {
          q->leaf.entry.err_code = err_code(err);
          return;
        }
        err = block_update_request(&block->u.update_block, q->type, &q->r);
      }
      break;
#endif
    default:
      err = ERR_INVALID;
  }

  if( err ) qe->entry.err_code = err_code(err);

}

error_t schedule_query_on(shared_server_state_t* shared, int victim, query_entry_t* qe)
{
  server_state_t* tgt = &shared->threads[victim];
  error_t err;
  int rc;

  // Put the work on tgt.

  // Lock
  rc = pthread_mutex_lock(&tgt->lock);
  assert(!rc);

  if( tgt->running ) {
    err = schedule_query(tgt, qe);
  } else {
    err = ERR_INVALID_STR("server no longer running");
  }

  // Signal somebody waiting for requests (we only add one).
  rc = pthread_cond_signal(&tgt->wait_here_for_requests);
  assert(!rc);

  // Unlock
  rc = pthread_mutex_unlock(&tgt->lock);
  assert(!rc);

  return err;
}


error_t schedule_query_shared(shared_server_state_t* shared, query_entry_t* qe)
{
  size_t x = try_sync_add_size_t(&shared->enqueued_counter, 1);
  int victim = x % shared->num_threads;
  error_t err;
 
  err = schedule_query_on(shared, victim, qe);
  if( err ) return err;

  // Wake up the other threads, to get work stealing going.
  for( int i = 0; i < shared->num_threads; i++ ) {
    server_state_t* tgt = &shared->threads[i];
    int rc;

    rc = pthread_cond_broadcast(&tgt->wait_here_for_requests);
    assert(!rc);
  }

  return ERR_NOERR;
}

// Steals requests from shared->threads[victim]
// Steals up to max_to_steal requests
// Steals requests in up to max_blocks_to_steal blocks.
// Steals requests after the location of last_done in the tree
// Stores stolen requests in dst.
// Returns the number of stolen requests, or -1 if that queue has ended.
int steal_work(shared_server_state_t* shared, int my_id, int victim, int max_to_steal, int max_blocks_to_steal, leaf_entry_t* last_done, query_queue_t* dst)
{
  server_state_t* tgt = &shared->threads[victim];
  int i = 0;
  int last_block = -1;
  int num_blocks = 0;
  int rc;

  // Lock
  rc = pthread_mutex_lock(&tgt->lock);
  assert(!rc);

  if( ! tgt->running ) {
    rc = pthread_mutex_unlock(&tgt->lock);
    assert(!rc);
    return -1;
  }



  if( rb_not_empty(tgt) ) {
    leaf_entry_t* split_node;
    leaf_entry_t* end;
    leaf_entry_t* node;
    leaf_entry_t* next;
    leaf_entry_t* bq;
    int pass;

    last_block = -1;
    num_blocks = 0;
    i = 0;

    split_node = rb_search_leaf_entry(tgt, last_done);

    for( pass = 0;
          pass < 2 &&
          i < max_to_steal &&
          num_blocks <= max_blocks_to_steal;
        pass++ ) {
      if( pass == 0 ) {
        // First pass -- start at last_done and go to end (NULL)
        node = split_node;
        end = NULL;
      } else {
        // Second pass -- start at rb_first and go to last_done.
        node = rb_min_leaf_entry(tgt);
        end = split_node;
      }

      for( ;
           node && node != end && i < max_to_steal;
           node = next ) {

        // Get next first since we're about to remove node.
        next = rb_next_leaf_entry(tgt, node);

        // Get the leaf query structure
        bq = node;

        // Don't get queries from too many blocks.
        if( bq->block_id != last_block ) {
          last_block = bq->block_id;
          num_blocks++;
          if( num_blocks > max_blocks_to_steal ) break;
        }

        // remove bq from the from the tgt tree
        rb_delete_leaf_entry(tgt, bq);
        tgt->num_leaf_queries--;

        // Add it to our local list of work.
        query_queue_push(dst, &bq->entry);

        i++;
      }
    }
  }

  // Unlock
  rc = pthread_mutex_unlock(&tgt->lock);
  assert(!rc);


  return i;
}

unsigned int primes[] = {
      2,     3,     5,     7,    11,    13,    17,    19,    23,    29,
     31,    37,    41,    43,    47,    53,    59,    61,    67,    71,
     73,    79,    83,    89,    97,   101,   103,   107,   109,   113,
    127,   131,   137,   139,   149,   151,   157,   163,   167,   173,
    179,   181,   191,   193,   197,   199,   211,   223,   227,   229,
    233,   239,   241,   251,   257,   263,   269,   271,   277,   281,
    283,   293,   307,   311,   313,   317,   331,   337,   347,   349,
    353,   359,   367,   373,   379,   383,   389,   397,   401,   409,
    419,   421,   431,   433,   439,   443,   449,   457,   461,   463,
    467,   479,   487,   491,   499,   503,   509,   521,   523,   541,
    547,   557,   563,   569,   571,   577,   587,   593,   599,   601,
    607,   613,   617,   619,   631,   641,   643,   647,   653,   659,
    661,   673,   677,   683,   691,   701,   709,   719,   727,   733,
    739,   743,   751,   757,   761,   769,   773,   787,   797,   809,
    811,   821,   823,   827,   829,   839,   853,   857,   859,   863,
    877,   881,   883,   887,   907,   911,   919,   929,   937,   941,
};

int i_have_work(server_state_t* ss)
{
  return ss->new_process_queries.head.lh_first ||
         ss->new_leaf_queries.head.lh_first ||
         ss->ready_with_results.head.lh_first;
}


void worker_work(server_state_t* ss)
{
  int max_per_iter = 256;
  int max_blocks_per_iter = ss->shared_state->settings.block_cache_size - 1;
  int processed = 0;
  int added = 0;
  shared_server_state_t* shared = ss->shared_state;
  int my_thread_number = ss->thread_number;
  int num_threads = shared->num_threads;
  int rc;
  leaf_entry_t* start_query;
  query_queue_t my_work;
  int stolen;
  int victim;
  size_t n_primes = sizeof(primes)/sizeof(unsigned int);
  size_t ith_prime = ss->thread_number % n_primes;
  size_t multiple = 1 + (ss->thread_number / n_primes);
  unsigned int rng_state = ss->thread_number;
  unsigned int rng_stride = multiple * primes[ith_prime];
  struct timespec ts;
  int no_work_counter = 0;
  int no_work_before_wait = 2 * num_threads;

  start_query = alloca(max_leaf_entry_size());
  memset(start_query, 0, max_leaf_entry_size());

  //printf("Thread %i starting rng=%i rng_stride=%i\n", my_thread_number, rng_state, rng_stride);

  query_queue_init(&my_work);

  while( 1 ) {
    // Steal some work.. from ourselves.
    stolen = steal_work(shared, my_thread_number, my_thread_number, max_per_iter, max_blocks_per_iter, start_query, &my_work);

    if( stolen == -1 ) return; // terminate this thread!

    if( stolen == 0 && ! i_have_work(ss) ) {
      // Look for other work.
      
      // Generate a 'random' number.
      rng_state += rng_stride;
      victim = rng_state % num_threads;

      //printf("Thread %i stealing from %i\n", my_thread_number, victim);

      // Steal from new_process_queries or leaf_queries
      stolen = steal_work(shared, my_thread_number, victim, max_per_iter, max_blocks_per_iter, start_query, &my_work);
    }

    //printf("Thread %i have %i requests\n", my_thread_number, stolen);

    if( stolen == 0 && ! i_have_work(ss) ) no_work_counter++;
    else no_work_counter = 0;

    if( stolen == 0 && no_work_counter >= no_work_before_wait ) {
      // Wait on the condition variable to get more work,
      // but only wait for a second in case other guys get real busy.

      // Lock the state.
      rc = pthread_mutex_lock(&ss->lock);
      assert(!rc);

      // Do we have anything to do?
      if( rb_not_empty(ss) ||
          i_have_work(ss) ) {
        // OK -- work has appeared.
      } else {
        rc = clock_gettime(CLOCK_REALTIME, &ts);
        assert(!rc);

        ts.tv_sec += 1;

        rc = pthread_cond_timedwait(&ss->wait_here_for_requests, &ss->lock, &ts);
        if( rc && rc != ETIMEDOUT ) assert(!rc);
      }

      // Unlock the state.
      rc = pthread_mutex_unlock(&ss->lock);
      assert(!rc);
    }

    // Now process whatever we've got in stolen or in our 
    // new request queues.
    // Now work with whatever we harvested for our local storage.
    // TODO -- do this twice, calling readahead_leaf the first time.
    {
      query_entry_t* qe;
      query_entry_t* nextq;
      
      // Copy the first one to start_query.
      if( my_work.head.lh_first ) {
        qe = my_work.head.lh_first;

        assert( is_leaf_query(qe->type) ); // only steal leaf queries.

        // Copy the query to the cached query, for purposes
        // of getting things in the right order when stealing.
        copy_leaf_query(start_query, (leaf_entry_t*) qe);
      }

      // Just go through the list of queries in working_queries
      for( qe = my_work.head.lh_first;
           qe != NULL;
           qe = nextq ) {
        // Get next since we're going to remove qe from the list.
        nextq = qe->entries.le_next;

        if( DEBUG_SERVER > 8 ) printf("worker %i working on %p\n", ss->thread_number, qe);

        // remove qe from the list.
        query_queue_remove(&my_work, qe);

        processed++;

        assert( is_leaf_query(qe->type) ); // only steal leaf queries.

        {
          leaf_entry_t* bq;
          bq = (leaf_entry_t*) qe;

          evaluate_leaf(ss, bq);

          // call the callback function with the result.
          deliver_result(ss, & bq->entry );
        }
      }
    }


    // We might have added to new_process_queries,
    // new_leaf_queries, or ready_with_results.
    // new_process_queries are handled in below
    // new_leaf_queries are handled below.
    // ready_with_results are handled below.
    {
      query_entry_t* qe;
      query_entry_t* nextq;

      // Now actually deliver any results that we have.
      for( qe = ss->ready_with_results.head.lh_first;
           qe != NULL;
           qe = nextq ) {
        // Get next since we're going to remove qe from the list.
        nextq = qe->entries.le_next;

        if( DEBUG_SERVER > 8 ) printf("worker %i ready results has %p\n", ss->thread_number, qe);
      }


      // Now actually deliver any results that we have.
      for( qe = ss->ready_with_results.head.lh_first;
           qe != NULL;
           qe = nextq ) {
        // Get next since we're going to remove qe from the list.
        nextq = qe->entries.le_next;

        if( DEBUG_SERVER > 8 ) printf("worker %i delivering result on %p to %p\n", ss->thread_number, qe, qe->parent);

        // remove qe from the list.
        query_queue_remove(&ss->ready_with_results, qe);

        really_deliver_result(ss, qe);
      }

      // Now actually deliver any results that we have.
      for( qe = ss->ready_with_results.head.lh_first;
           qe != NULL;
           qe = nextq ) {
        // Get next since we're going to remove qe from the list.
        nextq = qe->entries.le_next;

        if( DEBUG_SERVER > 8 ) printf("worker %i ready results has after %p\n", ss->thread_number, qe);
      }
    }
   
    // Handle any new process queries
    {
      query_entry_t* qe;
      query_entry_t* nextq;
      for( qe = ss->new_process_queries.head.lh_first;
           qe != NULL;
           qe = nextq ) {
        // get next since we're about to remove qe.
        nextq = qe->entries.le_next;

        // remove qe from the shared list
        query_queue_remove(&ss->new_process_queries, qe);

        assert( is_process_query(qe->type) ); // only steal leaf queries.

        // process the query.
        service_process(ss, (process_entry_t*) qe);
      }
    }

    if( ss->new_leaf_queries.head.lh_first ) {
      // Lock the state.
      rc = pthread_mutex_lock(&ss->lock);
      assert(!rc);

      // Now put any new leaf queries in the shared tree.
      {
        leaf_entry_t* bq;
        query_entry_t* qe;
        query_entry_t* nextq;
        
        // Any new leaf queries we got... we should put in the shared tree.
        for( qe = ss->new_leaf_queries.head.lh_first;
             qe;
             qe = nextq ) {
          nextq = qe->entries.le_next;

          // remove qe from the list.
          query_queue_remove(&ss->new_leaf_queries, qe);

          assert( is_leaf_query(qe->type) );
          bq = (leaf_entry_t*) qe;
          if( DEBUG_SERVER > 8 ) printf("worker %i adding leaf %p\n", ss->thread_number, bq);

          if( ! (bq->entry.parent && bq->entry.parent->canceled) ) {
            added++;
            rb_insert_leaf_entry(ss, bq);
            ss->num_leaf_queries++;
          } else {
            processed++;
            bq->entry.err_code = ERR_CODE_CANCELED;
            deliver_result(ss, & bq->entry );
          }
        }
      }

      // Unlock the state.
      rc = pthread_mutex_unlock(&ss->lock);
      assert(!rc);
    }
  }
}

void* worker_thread(void* arg)
{
  server_state_t* sst = (server_state_t*) arg;

  worker_work(sst);

  return NULL;
}

/*
void work_until_done(server_state_t* sst)
{
  int num;
  do {
    num = worker_work(sst);
  } while ( sst->pending_queries.head.lh_first );
}*/

void cleanup_server(shared_server_state_t* shared )
{
  if( shared ) {
    for( int i = 0; i < shared->num_threads; i++ ) {
      server_state_t* s = &shared->threads[i];

      pthread_mutex_destroy(&s->lock);
      pthread_cond_destroy(&s->wait_here_for_requests);

      cache_destroy(&s->block_cache);
      query_queue_destroy(&s->new_process_queries);
      query_queue_destroy(&s->new_leaf_queries);
      query_queue_destroy(&s->ready_with_results);
    }

    path_translator_destroy(&shared->path_to_id);

    free(shared->threads);
  }
  free(shared);
}


error_t block_fault(cache_id_t cid, int data_size, void* data, void* context)
{
  // load block numbered id.
  block_locator_t* id = (block_locator_t*) cid;
  server_state_t* s = (server_state_t*) context;
  server_block_t* block = (server_block_t*) data;
  index_block_type_t type;
  int64_t num;

  if( id == NULL ) return ERR_PARAM;

  if( DEBUG_SERVER > 2 ) printf("Block fault: %p index %" PRlocid " block %" PRIi64 " %p\n", id, id->index_loc.id, id->block_id, data);

  s->stats.block_faults++; // save some stats...

  type = block_type_from_id(id->block_id);
  block->type = type;
  switch ( type ) {
    case INDEX_BLOCK_TYPE_HEADER:
      return open_header_block(& block->u.header_block, & s->shared_state->path_to_id, id->index_loc);
    case INDEX_BLOCK_TYPE_DATA:
      num = block_number_from_id(id->block_id);
      return open_data_block(& block->u.data_block, & s->shared_state->path_to_id, id->index_loc, num, s->shared_state->settings.bucket_cache_size);
#if SUPPORT_INDEX_MERGE
    case INDEX_BLOCK_TYPE_DATA_UPDATE:
      num = block_number_from_id(id->block_id);
      return open_update_block(& block->u.update_block, id->index_loc, num,
                               1 /* writeable */);
      break;
#endif
    default:
      assert(0);
  }

  return ERR_INVALID;
}

void block_evict(cache_id_t cid, int data_size, void* data, void* context)
{
  block_locator_t* id = (block_locator_t*) cid;
  server_block_t* block = (server_block_t*) data;
  if( DEBUG_SERVER > 2 ) printf("Block evict: %p index %" PRlocid " %i %p\n", cid, id->index_loc.id, (int) id->block_id, data);
  switch (block->type) {
    case INDEX_BLOCK_TYPE_HEADER:
      close_header_block(&block->u.header_block);
      break;
    case INDEX_BLOCK_TYPE_DATA:
      close_data_block(&block->u.data_block);
      break;
#if SUPPORT_INDEX_MERGE
    case INDEX_BLOCK_TYPE_DATA_UPDATE:
      close_update_block(& block->u.update_block);
      break;
#endif
    default:
      assert(0);
      break;
  }

}

block_locator_t* copy_block_locator(block_locator_t* b)
{
  block_locator_t* ret = malloc(sizeof(block_locator_t));
  ret->index_loc = b->index_loc;
  ret->block_id = b->block_id;
  return ret;
}
void free_block_locator(block_locator_t* b)
{
  free(b);
}

cache_id_t block_cache_copy_id(cache_id_t id)
{
  return copy_block_locator((block_locator_t*)id);
}

void block_cache_free_id(cache_id_t id)
{
  free_block_locator((block_locator_t*)id);
}

hash_t hash_block_locator(const void* a)
{
  block_locator_t* bl = (block_locator_t*) a;
  return hash_index_locator(bl->index_loc) ^ hash_data((unsigned char*) &bl->block_id, 8);
}

int cmp_block_locators(const void* a, const void* b)
{
  block_locator_t* one = (block_locator_t*) a;
  block_locator_t* two = (block_locator_t*) b;
  int ret;

  ret = index_locator_cmp(one->index_loc, two->index_loc);
  if( ret != 0 ) return ret;
 
  return one->block_id - two->block_id;
}

error_t set_default_server_settings(server_settings_t* settings)
{
  long min_blocks = 3; // we must be able to store at least 3 default blocks.
  long threads_per_proc = 8; // try to oversubscribe by this much.
  long oversubscribe_real_memory = 4; // try to oversubscribe by this much.
  long divide_max_files = 8; // only use up to 1/8th the max # of files.
  long divide_max_mappings = 8; // only use up to 1/8th the max # of mappings.
  long bucket_cache_size = 32;

  /* Did a small study of number of threads in disk-bound environment:
    (note these were done with debug builds)

    echo 1 > /proc/sys/vm/drop_caches
    time ./search_tool /home/areca_c/index_1 --offsets Speaker

    --  ( 1 thread ) 2m33s
    x2  (10 threads) 2m43s
    x4  (20 threads) 2m41s
    x6  (31 threads) 2m54s
    x8  (41 threads) 2m25s
    x12 (62 threads) 2m42s
    x16 (83 threads) 2m44s

  */
     
  error_t err;
  long long phys_mem = 0;
  long long block_size = 0;
  long max_blocks = 3;
  long num_proc = 1;
  long num_threads = 1;
  long blocks_per_thread = 1;
  index_block_param_t index_param;

  set_default_param(&index_param);
  block_size = index_param.block_size; // should be 256MB

  assert(block_size > 1);

  // Decide max # blocks we can store in memory.
  err = get_phys_mem(&phys_mem);
  if( err ) return err;

  // This code assumes 512MB index blocks...
  // ... and that we only want to use half of available memory.
  // ... and blocks are up to block_size*2
  max_blocks = oversubscribe_real_memory * phys_mem / block_size / 2;

#ifdef _64_BIT
#else
  {
    long long mb = 1024*1024;
    long long one_gb = 1*1024*mb;
    long max_blocks_virt = one_gb / block_size / 2;
    if( max_blocks_virt < max_blocks ) max_blocks = max_blocks_virt;
  }
#endif

  // Check that we don't use all the available file descriptors
  // (this is also a stand-in for not using all available
  {
    long long max_fds = 0;
    long max_blocks_fds;
    err = get_max_open_files(&max_fds);
    if( err ) return err;
    max_blocks_fds = max_fds / divide_max_files;
    //printf("Max files is %lli max blocks then is %li\n", max_fds, max_blocks_fds);
    if( max_blocks_fds < max_blocks ) max_blocks = max_blocks_fds;
  }

  {
    long long max_mappings = 0;
    long max_blocks_mappings;
    err = get_max_mappings(&max_mappings);
    if( err ) return err;
    max_blocks_mappings = max_mappings / divide_max_mappings;
    //printf("Max mappings is %lli max blocks then is %li\n", max_mappings, max_blocks_mappings);
    if( max_blocks_mappings < max_blocks ) max_blocks = max_blocks_mappings;
  }


  //printf("max_blocks is %i\n", (int) max_blocks);

  if( max_blocks < min_blocks ) return ERR_MEM;

  // OK - we know the max # of blocks we can make,
  // now decide how many processors to use.
  err = get_num_processors(&num_proc);
  if( err ) return err;

  assert( num_proc >= 1 );

  num_threads = threads_per_proc * num_proc;

//#pragma warning unoptimal
//  num_threads = 1;

  // OK -- now choose num_threads in [1,num_threads]
  // so that each thread at least has min_blocks.
  blocks_per_thread = max_blocks / num_threads;
  if( blocks_per_thread < min_blocks ) {
    // We're memory limited!
    // Just use as many threads as we have memory to use..
    num_threads = max_blocks / min_blocks;
    blocks_per_thread = min_blocks;
  }

  assert( blocks_per_thread >= min_blocks );
  assert( blocks_per_thread * num_threads <= max_blocks );

  settings->verbose = 0;
  settings->num_threads = num_threads;
  settings->block_cache_size = blocks_per_thread;
  settings->bucket_cache_size = bucket_cache_size;

  return ERR_NOERR;
}

error_t start_server(shared_server_state_t** sst, server_settings_t* settings)
{
  shared_server_state_t* shared = NULL;
  error_t err = ERR_NOERR;
  int rc;

  if( settings->verbose > 0 ) {
    fprintf(stderr, "FEMTO starting server with %i threads, %i blocks per thread, %i buckets per block\n", settings->num_threads, settings->block_cache_size, settings->bucket_cache_size);
  }

  shared = calloc(1, sizeof(shared_server_state_t));
  if( ! shared ) {
    err = ERR_MEM;
    goto error;
  }

  memcpy(&shared->settings, settings, sizeof(server_settings_t));

  err = path_translator_init(&shared->path_to_id);
  if( err ) goto error;

  shared->enqueued_counter = 0;

  shared->num_threads = 0;
  shared->threads = calloc(settings->num_threads,sizeof(server_state_t));

  // Now create the per-thread structures.
  for( shared->num_threads = 0;
       shared->num_threads < settings->num_threads;
       shared->num_threads++ ) {
    server_state_t* s = &shared->threads[shared->num_threads];

    s->shared_state = shared;
 
    rc = pthread_mutex_init(&s->lock, NULL);
    if( rc ) {
      err = ERR_PTHREADS("Could not create lock", rc);
      goto error;
    }
    rc = pthread_cond_init(&s->wait_here_for_requests, NULL);
    if( rc ) {
      err = ERR_PTHREADS("Could not create condition variable", rc);
      goto error;
    }

    s->thread_number = shared->num_threads;

    s->running = 0;

    err = cache_create(&s->block_cache, settings->block_cache_size,
                       hash_block_locator, cmp_block_locators,
                       sizeof(server_block_t),
                       s, block_fault, block_evict,
                       block_cache_copy_id, block_cache_free_id);
    if( err ) goto error;

    s->num_leaf_queries = 0;
    RB_INIT(&s->leaf_queries);

    query_queue_init(&s->new_process_queries);
    query_queue_init(&s->new_leaf_queries);
    query_queue_init(&s->ready_with_results);

    memset(&s->stats, 0, sizeof(server_statistics_t));
  }

  // Now start the threads.
  for( int i = 0; i < shared->num_threads; i++ ) {
    server_state_t* s = &shared->threads[i];
    int rc;

    s->running = 1;

    rc = pthread_create(&s->thread, NULL, worker_thread, s);
    if( rc ) {
      err = ERR_PTHREADS("Could not create thread", rc);
      for( int j = 0; j < i; j++ ) {
        void* retval = NULL;
        rc = pthread_join(s->thread, &retval);
        if( rc ) {
          warn_if_err(ERR_PTHREADS("Could not join thread", rc));
        }
      }
      goto error;
    }
  }

  *sst = shared;
  return ERR_NOERR;
error:
  cleanup_server(shared);
  return err;
}

void stop_server(shared_server_state_t* shared)
{
  int rc;

  // Tell all the threads to quit.
  for( int i = 0; i < shared->num_threads; i++ ) {
    server_state_t* s = &shared->threads[i];

    rc = pthread_mutex_lock(&s->lock);
    assert(!rc);

    s->running = 0;

    rc = pthread_cond_broadcast(&s->wait_here_for_requests);
    assert(!rc);

    rc = pthread_mutex_unlock(&s->lock);
    assert(!rc);
  }

  // Wait for all threads to finish.
  for( int i = 0; i < shared->num_threads; i++ ) {
    server_state_t* s = &shared->threads[i];
    void* retval = NULL;

    rc = pthread_join(s->thread, &retval);
    if( rc ) {
      warn_if_err(ERR_PTHREADS("Could not join thread", rc));
    }
  }

  cleanup_server(shared);
}

error_t server_schedule_query(shared_server_state_t* shared, process_entry_t* q)
{
  return schedule_query_shared(shared, (query_entry_t*) q);
}


void traverse_pe(query_entry_t* qe, void(*fn)(process_entry_t*))
{
  if( ! qe ) return;

  // Can only traverse process queries.
  if( ! is_process_query(qe->type) ) return;

  // Lock the state.
  {
    process_entry_t* pe = (process_entry_t*) qe;
    int rc;

    // Don't try to traverse queries that have not been set up.
    if( ! pe->setup ) return;

    rc = pthread_mutex_lock(&pe->lock);
    assert(!rc);

    // Call the function.
    fn(pe);

    // Traverse any children.
    switch( pe->entry.type ) {
      case QUERY_TYPE_SIGNAL:
        { struct femto_request* q = (struct femto_request*) pe;
          traverse_pe(q->query, fn); }
        break;
      case QUERY_TYPE_STRING:
        // Do nothing.
        break;
      case QUERY_TYPE_BACKWARD_SEARCH:
        // Do nothing.
        break;
      case QUERY_TYPE_BACK:
        // Do nothing.
        break;
      case QUERY_TYPE_FORWARD:
        // Do nothing.
        break;
      case QUERY_TYPE_CONTEXT:
        { context_query_t* q = (context_query_t*) pe;
          traverse_pe((query_entry_t*) &q->backward, fn);
          traverse_pe((query_entry_t*) &q->forward, fn); }
        break;
      case QUERY_TYPE_REGEXP:
        // Do nothing... only header and block queries in there.
        break;
      case QUERY_TYPE_PARALLEL_QUERY:
        { parallel_query_t* q = (parallel_query_t*) pe;
          for( long i = 0; i < q->num_queries; i++ ) {
            query_entry_t* sub = ith_query(q, i);
            traverse_pe(sub, fn);
          }
        }
        break;
      case QUERY_TYPE_LOCATE:
        { locate_query_t* q = (locate_query_t*) pe;
          traverse_pe((query_entry_t*) &q->count, fn);
          traverse_pe((query_entry_t*) &q->locate_range, fn); }
        break;
      case QUERY_TYPE_GET_URL:
        // Do nothing.
        break;
      case QUERY_TYPE_EXTRACT_DOCUMENT:
        { extract_document_query_t* q = (extract_document_query_t*) pe;
          traverse_pe((query_entry_t*) &q->ctx, fn); }
        break;
      case QUERY_TYPE_RANGE_TO_RESULTS:
        { range_to_results_query_t* q = (range_to_results_query_t*) pe;
          traverse_pe((query_entry_t*) &q->locate_range, fn); }
        break;
      case QUERY_TYPE_STRING_RESULTS:
        { string_results_query_t* q = (string_results_query_t*) pe;
          traverse_pe((query_entry_t*) &q->rtrq, fn);
          traverse_pe((query_entry_t*) &q->string_query, fn); }
        break;
      case QUERY_TYPE_REGEXP_RESULTS:
        { regexp_results_query_t* q = (regexp_results_query_t*) pe;
          traverse_pe((query_entry_t*) &q->rtrq, fn);
          traverse_pe((query_entry_t*) &q->regexp, fn); }
        break;
      case QUERY_TYPE_BOOLEAN:
        { generic_boolean_query_t* q = (generic_boolean_query_t*) pe;
          traverse_pe((query_entry_t*) q->left, fn);
          traverse_pe((query_entry_t*) q->right, fn); }
        break;
      default:
        assert(0);
    }

    // Unlock the state.
    rc = pthread_mutex_unlock(&pe->lock);
    assert(!rc);
  }
}

void cancel_fn(process_entry_t* pe)
{
  pe->canceled = 1;
}

error_t server_cancel_query(shared_server_state_t* shared, process_entry_t* q)
{
  traverse_pe((query_entry_t*) q, cancel_fn);

  return ERR_NOERR;
}

error_t setup_parallel_query(parallel_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int query_size, int num_queries)
{
  error_t err;

  memset(q, 0, sizeof(parallel_query_t));

  err = setup_process_entry( &q->proc,
                       QUERY_TYPE_PARALLEL_QUERY, loc,
                       0, requestor,
                       do_parallel_query);
  if( err ) return err;

  q->query_size = query_size;
  q->num_queries = num_queries;
  q->queries = NULL;

  if( q->num_queries == 0 ) return ERR_PARAM;

  q->queries = malloc(q->num_queries * q->query_size);
  if( DEBUG_SERVER > 10 ) printf("setup malloced %li items in %p\n", q->num_queries, q->queries);
  if( ! q->queries ) return ERR_MEM;
  memset( q->queries, 0, q->num_queries * q->query_size);

  return ERR_NOERR;
}

void cleanup_parallel_query(parallel_query_t* q)
{
  assert(q->proc.entry.type == QUERY_TYPE_PARALLEL_QUERY);
  free(q->queries);
  q->queries = NULL;
  cleanup_process_entry(&q->proc);
}


void do_parallel_query(server_state_t* ss, query_mode_t mode, query_entry_t* query)
{
  parallel_query_t* s;
  long i;


  switch ( mode ) {
    case QUERY_MODE_START:
      assert( query->type == QUERY_TYPE_PARALLEL_QUERY );
      s = (parallel_query_t*) query;
      // start all of the queries.
      for( i = 0; i < s->num_queries; i++ ) {
        if( DEBUG_SERVER > 10 ) printf("parallel_query %p starting %li/%li   %p\n", s, i, s->num_queries, ith_query(s, i));
        CHECK_ERROR(schedule_query(ss, ith_query(s, i)));
      }

      break;
    case QUERY_MODE_RESULT:
      assert( query->parent->entry.type == QUERY_TYPE_PARALLEL_QUERY );
      s = (parallel_query_t*) query->parent;
      CHECK_RESPONSE(query);
      if( DEBUG_SERVER > 10 ) printf("parallel_query %p got response#%li/%li   %p\n", s, s->proc.entry.state, s->num_queries, query);
      s->proc.entry.state++; // count up to it..
      if( s->proc.entry.state >= s->num_queries ) {
        // call our parent's done function!
        if( DEBUG_SERVER > 10 ) printf("parallel_query %p returning result\n", s);
        RETURN_RESULT;
      }
      break;
    default:
      assert(0);
  }
}


error_t setup_parallel_count(parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int npats, int* plen, alpha_t** pats)
{
  long i, j;
  error_t err;

  err = setup_parallel_query(ctx, requestor, loc,
                             sizeof(string_query_t), npats);
  if( err ) return err;

  for( i = 0; i < ctx->num_queries; i++ ) {
    err = setup_string_query((string_query_t*) ith_query(ctx, i),
                             &ctx->proc,
                             loc,
                             plen[i], pats[i]);
    if( err ) {
      for( j = 0; j < i; j++ ) {
        cleanup_string_query((string_query_t*) ith_query(ctx, j));
      }
      cleanup_parallel_query(ctx);
      return err;
    }
  }

  return ERR_NOERR;
}

void cleanup_parallel_count(parallel_query_t* ctx ) 
{
  long i;
  for( i = 0; i < ctx->num_queries; i++ ) {
    cleanup_string_query((string_query_t*) ith_query(ctx, i));
  }
  cleanup_parallel_query(ctx);
}

void print_pat(int len, alpha_t* pat)
{
  fprint_alpha(stdout, len, pat);
}

error_t setup_locate_range(parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int beforeCtxLen, int afterCtxLen, int doLocate,
                             int64_t first, int64_t last)
{
  long i, j;
  error_t err;

  // currently getting the context is not supported.
  assert( beforeCtxLen == 0 && afterCtxLen == 0 );


  err = setup_parallel_query(ctx, requestor, loc,
                             sizeof(context_query_t), 1 + last - first);
  if( err ) return err;

  for( i = 0; i < ctx->num_queries; i++ ) {
    err = setup_context_query((context_query_t*) ith_query(ctx, i),
                           & ctx->proc, loc,
                           first + i, 0, 0, LOCATE_STRONG);
    if( err ) {
      for( j = 0; j < i; j++ ) {
        cleanup_context_query((context_query_t*) ith_query(ctx, j));
      }
      cleanup_parallel_query(ctx);
      return err;
    }
  }

  return ERR_NOERR;
}

void cleanup_locate_range(parallel_query_t* q ) 
{
  long j;
  
  if( q->queries ) {
    for( j = 0; j < q->num_queries; j++ ) {
      query_entry_t* qe = ith_query(q,j);
      cleanup_context_query((context_query_t*) qe);
    }
  }
  cleanup_parallel_query(q);
}

error_t setup_get_doc_info_results( parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             results_t* results)
{
  long i, j;
  error_t err;
  results_reader_t reader;

  err = setup_parallel_query(ctx, requestor, loc,
                             sizeof(get_doc_info_query_t), results->num_documents);
  if( err ) return err;

  err = results_reader_create(&reader, results);
  if( err ) return err;

  for( i = 0; i < ctx->num_queries; i++ ) {
    int64_t document;
    j = results_reader_next(&reader, &document, NULL);

    err = setup_get_doc_info_query((get_doc_info_query_t*) ith_query(ctx, i),
                               & ctx->proc, loc, document);
    if( ! err && ! j ) err = ERR_INVALID; // didn't read one!
    if( err ) {
      for( j = 0; j < i; j++ ) {
        cleanup_get_doc_info_query((get_doc_info_query_t*) ith_query(ctx, j));
      }
      cleanup_parallel_query(ctx);
      return err;
    }
  }

  return ERR_NOERR;
}
void cleanup_get_doc_info_results(parallel_query_t* ctx ) 
{
  if( ctx->queries ) {
    for( long j = 0; j < ctx->num_queries; j++ ) {
      query_entry_t* qe = ith_query(ctx,j);
      cleanup_get_doc_info_query((get_doc_info_query_t*) qe);
    }
  }
  cleanup_parallel_query(ctx);
}
error_t get_doc_info_results( parallel_query_t* ctx, 
                       int* num, long** lensOut, unsigned char*** infosOut )
{
  long* lens = NULL;
  unsigned char** infos = NULL;
  if( ctx->queries ) {
    lens = malloc(sizeof(long)*ctx->num_queries);
    if( ! lens ) return ERR_MEM;
    infos = malloc(sizeof(unsigned char*)*ctx->num_queries);
    if( ! infos ) return ERR_MEM;
    for( long j = 0; j < ctx->num_queries; j++ ) {
      get_doc_info_query_t* q = (get_doc_info_query_t*) ith_query(ctx,j);
      lens[j] = q->info_len;
      infos[j] = q->info;
      q->info = NULL;
    }
  }
  *num = ctx->num_queries;
  *lensOut = lens;
  *infosOut = infos;

  return ERR_NOERR;
}

error_t setup_string_rows_all_query(
          parallel_query_t* ctx,
          process_entry_t* requestor,
          index_locator_t loc,
          int plen,
          alpha_t* pat)
{
  error_t err;
  alpha_t* ext_pat = NULL;
  int num_setup = 0;
 
  ext_pat = calloc(2+plen, sizeof(alpha_t));

  if( ! ext_pat ) return ERR_MEM;

  err = setup_parallel_query( ctx, requestor, loc,
                              sizeof(string_query_t), 2*ALPHA_SIZE );

  if( err ) goto error;

  memcpy(&ext_pat[1], pat, plen*sizeof(alpha_t));

  // Try all prefixes and suffixes.
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    ext_pat[0] = i;
    ext_pat[plen+1] = i;
    // prefix query.
    err = setup_string_query((string_query_t*) ith_query(ctx, i),
                             &ctx->proc,
                             loc,
                             plen + 1, &ext_pat[0]);
    if( err ) goto error;

    // suffix query.
    err = setup_string_query((string_query_t*) ith_query(ctx, ALPHA_SIZE + i),
                             &ctx->proc,
                             loc,
                             plen + 1, &ext_pat[1]);
    if( err ) goto error;

    num_setup = i;
  }

  free(ext_pat);

  return ERR_NOERR;

error:
  for( int i = 0; i < num_setup; i++ ) {
    cleanup_string_query((string_query_t*) ith_query(ctx, i));
    cleanup_string_query((string_query_t*) ith_query(ctx, ALPHA_SIZE + i));
  }
  cleanup_parallel_query(ctx);

  if( ext_pat ) free(ext_pat);

  return err;
}

void cleanup_string_rows_all_query(parallel_query_t* ctx)
{
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    cleanup_string_query((string_query_t*) ith_query(ctx, i));
    cleanup_string_query((string_query_t*) ith_query(ctx, ALPHA_SIZE + i));
  }
  cleanup_parallel_query(ctx);
}

error_t setup_string_rows_addleftright_query(
          parallel_query_t* ctx,
          process_entry_t* requestor,
          index_locator_t loc,
          int plen,
          alpha_t* pat,
          int adding_left)
{
  error_t err;
  alpha_t* ext_pat = NULL;
  int num_setup = 0;
 
  ext_pat = calloc(2+plen, sizeof(alpha_t));

  if( ! ext_pat ) return ERR_MEM;

  err = setup_parallel_query( ctx, requestor, loc,
                              sizeof(string_query_t), ALPHA_SIZE );

  if( err ) goto error;

  memcpy(&ext_pat[1], pat, plen*sizeof(alpha_t));

  // Try all prefixes and suffixes.
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    ext_pat[0] = i;
    ext_pat[plen+1] = i;

    if( adding_left ) {
      // prefix query.
      err = setup_string_query((string_query_t*) ith_query(ctx, i),
                               &ctx->proc,
                               loc,
                               plen + 1, &ext_pat[0]);
      if( err ) goto error;
    } else {
      // suffix query.
      err = setup_string_query((string_query_t*) ith_query(ctx, i),
                               &ctx->proc,
                               loc,
                               plen + 1, &ext_pat[1]);
      if( err ) goto error;
    }

    num_setup = i;
  }

  free(ext_pat);

  return ERR_NOERR;

error:
  for( int i = 0; i < num_setup; i++ ) {
    cleanup_string_query((string_query_t*) ith_query(ctx, i));
  }
  cleanup_parallel_query(ctx);

  if( ext_pat ) free(ext_pat);

  return err;
}

void cleanup_string_rows_addleftright_query(parallel_query_t* ctx)
{
  for( int i = 0; i < ALPHA_SIZE; i++ ) {
    cleanup_string_query((string_query_t*) ith_query(ctx, i));
  }
  cleanup_parallel_query(ctx);
}
      
error_t setup_locate_query(
          locate_query_t* q,
          process_entry_t* parent,
          index_locator_t loc,
          int plen, alpha_t* pat, 
          int max_occs)
{
  error_t err;

  memset(q, 0, sizeof(locate_query_t));

  err = setup_process_entry( &q->proc,
                       QUERY_TYPE_LOCATE, loc,
                       0, parent,
                       do_locate_query );
  if( err ) return err;

  q->max_occs = max_occs;

  err = setup_string_query(&q->count, &q->proc,
                           loc, plen, pat);
  if( err ) return err;

  memset(&q->locate_range, 0, sizeof(parallel_query_t));
  // we'll set up the parallel locate_range query later.
  return ERR_NOERR;
}

void cleanup_locate_query(locate_query_t* q)
{
  assert(q->proc.entry.type == QUERY_TYPE_LOCATE);

  cleanup_string_query(&q->count);
  if( q->proc.entry.state >= 0x2 ) {
    // ending at state 0 or 1 we never made the locate range 
    cleanup_locate_range(&q->locate_range);
  }
  cleanup_process_entry(&q->proc);

}

void do_locate_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  // back up to search...
  locate_query_t* s = NULL;
  error_t err;
  int64_t last;


  switch ( mode ) {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_LOCATE );
      s = (locate_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      assert( qe->parent->entry.type == QUERY_TYPE_LOCATE );
      s = (locate_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

  /* The normal algorithm:
  */

  switch (s->proc.entry.state) {
    case 0x0:
      // start the count query
      if( DEBUG > 10 ) printf("Starting locate query %p with count %p\n", s, &s->count);
      schedule_query(ss, (query_entry_t*) &s->count);
      WAIT_STATE(0x1); // wait for the response
    case 0x1:
      if( DEBUG > 10 ) printf("Continue locate query %p with locate_range %p\n", s, &s->locate_range);
      CHECK_RESPONSE( qe );
      if(qe != (query_entry_t*) &s->count) RETURN_ERROR(ERR_INVALID);
      if( s->count.first > s->count.last ) {
        // no results!
        RETURN_RESULT;
      }
      if( s->count.last - s->count.first > (int64_t) s->max_occs ) {
        last = s->count.first + (int64_t) s->max_occs - 1;
      } else {
        last = s->count.last;
      }

      // start the parallel locate range query
      err = setup_locate_range(&s->locate_range,
                               & s->proc,
                               s->proc.entry.loc,
                               0 /*beforectx*/, 0 /*afterCtx*/, 1 /*dolocate*/, 
                               s->count.first, last);
      CHECK_ERROR(err);
      err = schedule_query(ss, (query_entry_t*) &s->locate_range);
      CHECK_ERROR(err);
      WAIT_STATE(0x2);
    case 0x2:
      // we've got all the results; report them.
      CHECK_RESPONSE( qe );
      if(qe != (query_entry_t*) &s->locate_range) RETURN_ERROR(ERR_INVALID);
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
  }

}


error_t setup_parallel_locate(parallel_query_t* ctx,
                              process_entry_t* requestor,
                              index_locator_t loc,
                              int npats, int* plen, alpha_t** pats, 
                              int max_occs_each)
{

  long i, j;
  error_t err;

  err = setup_parallel_query(ctx, requestor, loc,
                             sizeof(locate_query_t), npats);
  if( err ) return err;

  for( i = 0; i < ctx->num_queries; i++ ) {
    err = setup_locate_query((locate_query_t*) ith_query(ctx, i),
                             & ctx->proc, loc,
                             plen[i], pats[i], max_occs_each);
    if( err ) {
      for( j = 0; j < i; j++ ) {
        cleanup_locate_query((locate_query_t*) ith_query(ctx, j));
      }
      cleanup_parallel_query(ctx);
      return err;
    }
  }

  return ERR_NOERR;
}

void cleanup_parallel_locate(parallel_query_t* ctx)
{
  long j;

  for( j = 0; j < ctx->num_queries; j++ ) {
    cleanup_locate_query((locate_query_t*) ith_query(ctx, j));
  }

  cleanup_parallel_query(ctx);
}

error_t setup_results_query(
    results_query_t* q,
    query_type_t type,
    index_locator_t loc,
    process_entry_t* requestor,
    query_callback callback,
    int64_t chunk_size, result_type_t result_type)
{
  error_t err;

  memset(q, 0, sizeof(results_query_t));

  err = setup_process_entry( &q->proc,
                       type, loc,
                       0, requestor,
                       callback);
  if( err ) return err;
 
  q->chunk_size = chunk_size;
  q->is_last_chunk = 0;
  results_clear_set_type(&q->results, result_type);

  return ERR_NOERR;
}

void cleanup_results_query(results_query_t* q)
{
  assert( is_result_query(q->proc.entry.type) );
  results_destroy(&q->results);
  cleanup_process_entry(&q->proc);
}

error_t setup_range_to_results_query(
                             range_to_results_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int64_t chunk_size, result_type_t result_type,
                             int64_t first, int64_t last)
{
  error_t err;

  memset(q, 0, sizeof(range_to_results_query_t));

  err = setup_results_query( &q->results,
                       QUERY_TYPE_RANGE_TO_RESULTS, loc,
                       requestor, do_range_to_results_query,
                       chunk_size, result_type );
  if( err ) return err;

  q->first = first;
  q->last = last;

  memset(&q->hq, 0, sizeof(header_occs_query_t));
  memset(&q->bq, 0, sizeof(block_query_t));
  memset(&q->bcq, 0, sizeof(block_chunk_query_t));
  memset(&q->locate_range, 0, sizeof(parallel_query_t));

  return ERR_NOERR;
}

void cleanup_range_to_results_query(range_to_results_query_t* q ) 
{
  assert( q->results.proc.entry.type == QUERY_TYPE_RANGE_TO_RESULTS );

  cleanup_results_query(&q->results);
}

void do_range_to_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  range_to_results_query_t* s = NULL;
  block_chunk_query_t* bcq = NULL;
  header_occs_query_t* hq = NULL;
  parallel_query_t* locate_range = NULL;
  error_t err;


  switch ( mode ) {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_RANGE_TO_RESULTS );
      s = (range_to_results_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK_CHUNK ) {
        bcq = (block_chunk_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_PARALLEL_QUERY ) {
        locate_range = (parallel_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_RANGE_TO_RESULTS );
      s = (range_to_results_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

 
  /** The normal algorithm:
      given [first,last] is the range of interest.

      if( first > last ) {
        return as the last chunk an empty chunk.
      }
      i = first

      // get a chunk:
      if( looking for document numbers ) {
        block = the block containing the row i (HDR_BSEARCH_BLOCK_ROWS)
        chunk = the chunk containing row i (QUERY_TYPE_BLOCK_CHUNK)

        if( chunk.start < first || // we're on one of those boundaries
            chunk.last > last ) {
          do a locate to find the results (parallel query)
        } else {
          get the chunk (QUERY_TYPE_BLOCK_CHUNK)
        }
        i = chunk.last + 1;
      } else {
        do a locate to find chunk_size results (parallel query)
        i += chunk_size;
      }

    */

  while ( 1 ) {
  switch (s->results.proc.entry.state) {
    case 0x0: // initial setup
      // check for an invalid range.
      if( s->first > s->last ) {
        s->results.proc.entry.state = 0;
        s->results.is_last_chunk = 1;
        // results were cleared in setup_result_query.
        RETURN_RESULT;
      }
      s->i = s->first;
      s->results.proc.entry.state = 0x10;
      if( DEBUG > 8 ) printf("range_results_t %p starting, chunk=%i\n", s, (int) s->results.chunk_size);
      // fall through
    case 0x10: // get a chunk
      if( results_type(&s->results.results) == RESULT_TYPE_DOCUMENTS ) {
        s->results.proc.entry.state = 0x100;
      } else {
        s->results.proc.entry.state = 0x200;
      }
      break; // goes to the appropriate state (using while around switch)
    case 0x100:
      if( DEBUG > 8 ) printf("range_results_t %p doing header occs query\n", s);
      // do a header query to find the block containing row i.
      setup_header_occs_query(&s->hq, s->results.proc.entry.loc,
                              0x1, // state
                              & s->results.proc,
                              HDR_BSEARCH_BLOCK_ROWS,
                              0, 0, 
                              s->i, // row number
                              0);
      err = schedule_query(ss, (query_entry_t*) &s->hq);
      CHECK_ERROR(err);

      WAIT_STATE(0x110); // wait for the header query to come back.
    case 0x110:
      // we should have the response for the header query.
      if( DEBUG > 8 ) printf("range_results_t %p got header occs query\n", s);
      CHECK_RESPONSE(hq);

      // do the chunk query to find the chunk containing row i
      setup_block_chunk_query(&s->bcq, s->results.proc.entry.loc,
                              0x2, // state
                              & s->results.proc,
                              BLOCK_CHUNK_FIND_NUMBER,
                              s->hq.r.block_num, 0,
                              s->i - s->hq.r.row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->bcq);
      CHECK_ERROR(err);

      WAIT_STATE(0x120); // wait for the chunk query to come back.
    case 0x120:
      // we should have a response for the chunk query.
      if( DEBUG > 8 ) printf("range_results_t %p got chunk query\n", s);
      CHECK_RESPONSE(bcq);

      // now, decide if we need to locate or if we need to 
      // get the chunk.
      {
        int64_t start, end;
        int use_chunk = 0;
        if( bcq->r.first + s->hq.r.row < s->first )
        {
          // the chunk starts before the first row we want.
          // we'll want from our first row to the end of the chunk.
          start = s->first;
          if( bcq->r.last + s->hq.r.row > s->last ) {
            end = s->last;
          } else {
            end = bcq->r.last + s->hq.r.row;
          }
          if( 1 + end - start > s->results.chunk_size ) {
            end = start + s->results.chunk_size - 1;
          }
          s->i = 1 + end; // get the next i
        } else if( bcq->r.last + s->hq.r.row > s->last ) {
          // the chunk starts at the start of our chunk
          // the chunk extends beyond the end of our range.
          // we'll want from the start of the chunk to 
          // the end of our range.
          start = bcq->r.first + s->hq.r.row;
          end = s->last;
          if( 1 + end - start > s->results.chunk_size ) {
            end = start + s->results.chunk_size - 1;
          }
          s->i = 1 + end; // get the next i
        } else {
          // we can just get the data from the chunk.
          use_chunk = 1;
          // set the next i to the start of the next chunk.
          start = bcq->r.first + s->hq.r.row;
          end = bcq->r.last + s->hq.r.row;
          s->i = 1 + end;
        }

        if( DEBUG > 8 ) {
          printf("range_results_t %p looking for [%" PRIi64 ", %" PRIi64 "]\n", s, start, end);
        }

        cleanup_block_chunk_query(bcq);

        if( use_chunk ) {
          if( DEBUG > 8 ) printf("range_results_t %p doing chunk docs query\n", s);
          // create the chunk request
          setup_block_chunk_query(&s->bcq, s->results.proc.entry.loc,
                                  0x2, // state
                                  & s->results.proc,
                                  BLOCK_CHUNK_REQUEST_DOCUMENTS,
                                  s->hq.r.block_num, s->bcq.r.chunk_number,
                                  0, 0);
          err = schedule_query(ss, (query_entry_t*) &s->bcq);
          CHECK_ERROR(err);
          WAIT_STATE(0x130);
        } else {
          if( DEBUG > 8 ) printf("range_results_t %p doing locate range query\n", s);
          err = setup_locate_range(&s->locate_range,
                                   &s->results.proc,
                                   s->results.proc.entry.loc,
                                   0 /*beforectx*/, 0 /*afterCtx*/, 1 /*dolocate*/, 
                                   start, end );
          CHECK_ERROR(err);
          err = schedule_query(ss, (query_entry_t*) &s->locate_range);
          CHECK_ERROR(err);
          WAIT_STATE(0x300);
        }

      }
      // code not reachable.
      break;
    case 0x130:
      // we just got the bcq results
      if( DEBUG > 8 ) printf("range_results_t %p got chunk docs query\n", s);
      CHECK_RESPONSE(bcq);
      
      // return the results from bcq.
      results_move(&s->results.results, &bcq->r.results);

      cleanup_block_chunk_query(bcq);
      // return the results.
      s->results.proc.entry.state = 0x400;
      break; // go to state 0x400 where we return.

    case 0x200:
      if( DEBUG > 8 ) printf("range_results_t %p doing locate range 200 \n", s);
      {
        int64_t end;

        end = s->i + s->results.chunk_size - 1;
        if( end > s->last ) end = s->last;

        // start a locate_range query for a chunk.
        err = setup_locate_range(&s->locate_range,
                                 &s->results.proc,
                                 s->results.proc.entry.loc,
                                 0 /*beforectx*/, 0 /*afterCtx*/, 1 /*dolocate*/, 
                                 s->i, end );
        CHECK_ERROR(err);
        err = schedule_query(ss, (query_entry_t*) &s->locate_range);
        CHECK_ERROR(err);
        // update i
        s->i = 1 + end;
        WAIT_STATE(0x300);
      }
    case 0x300:
      if( DEBUG > 8 ) printf("range_results_t %p got locate range\n", s);
      // we just got the locate range results.
      CHECK_RESPONSE(locate_range);

      // save the results.
      {
        error_t err;
        int64_t* results;
        long num_results;

        // 1 - check for errors.
        for( long i = 0; i < locate_range->num_queries; i++ ) {
          context_query_t* cq = (context_query_t*) ith_query(locate_range, i);
          CHECK_RESPONSE( cq );
        }

        // 2 - save the results in a result_t
        results = malloc(sizeof(int64_t)*locate_range->num_queries);
        if( ! results ) RETURN_ERROR(ERR_MEM);

        num_results = locate_range->num_queries;
        for( long i = 0; i < locate_range->num_queries; i++ ) {
          context_query_t* cq = (context_query_t*) ith_query(locate_range, i);
          // store the offset for now.
          results[i] = cq->offset;
        }
        // cleanup the parallel query.
        cleanup_locate_range(locate_range);

        // if we're looking for documents, vs document offsets
        // lookup all of the results; we're looking for document numbers.

        // schedule parallel query to get the document numbers.
        err = setup_parallel_query(&s->locate_range, &s->results.proc,
                                   s->results.proc.entry.loc,
                                   sizeof(header_loc_query_t), num_results);
        CHECK_ERROR(err);
        for( long i = 0; i < num_results; i++ ) {
          setup_header_loc_query(
              (header_loc_query_t*) ith_query(&s->locate_range, i),
              s->results.proc.entry.loc,
              i,
              &s->locate_range.proc, 
              HDR_LOC_RESOLVE_LOCATION,
              results[i], 0);
        }
        free(results);

        // schedule the parallel query
        err = schedule_query(ss, (query_entry_t*) &s->locate_range);
        CHECK_ERROR(err);
        WAIT_STATE(0x310);
      }
    case 0x310:
      CHECK_RESPONSE(locate_range);

      if( results_type(&s->results.results) == RESULT_TYPE_DOCUMENTS ) {
        int64_t* results;
        long num_results;

        num_results = locate_range->num_queries;
        results = malloc(sizeof(int64_t) * num_results);
        if( ! results ) RETURN_ERROR(ERR_MEM);

        for( long i = 0; i < num_results; i++ ) {
          header_loc_query_t* loc_q = (header_loc_query_t*) ith_query(locate_range, i);
          results[i] = loc_q->r.loc.doc;
        }

        err = results_create_sort(&s->results.results, num_results, results);
        CHECK_ERROR(err);

        free(results);
      } else {
        // handling offsets
        location_info_t* results;
        long num_results;

        num_results = locate_range->num_queries;
        results = malloc(sizeof(location_info_t) * num_results);
        if( ! results ) RETURN_ERROR(ERR_MEM);

        for( long i = 0; i < num_results; i++ ) {
          header_loc_query_t* loc_q = (header_loc_query_t*) ith_query(locate_range, i);
          results[i] = loc_q->r.loc;
        }
        err = results_create_sort_locations(&s->results.results, num_results, results);
        CHECK_ERROR(err);
        free(results);
      }

      for( long i = 0; i < locate_range->num_queries; i++ ) {
          header_loc_query_t* loc_q = (header_loc_query_t*) ith_query(locate_range, i);
          cleanup_header_loc_query(loc_q);
      }
      // cleanup the query we just did.
      cleanup_parallel_query(locate_range);

      // we've got the results! go to state 400.
      s->results.proc.entry.state = 0x400;
      break; // while loop goes to that state.
    case 0x400:
      if( s->i > s->last ) {
        // when called back - start over
        s->results.proc.entry.state = 0x0;
        s->results.is_last_chunk = 1;
      } else {
        // when called back - get the next chunk
        s->results.proc.entry.state = 0x10;
      }
      // deliver the results.
      if( DEBUG > 8 ) printf("range_results_t %p delivering results\n", s);
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
  }
  }

}

error_t setup_string_results_query(
                             string_results_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int64_t chunk_size, result_type_t result_type,
                             int plen, alpha_t* pat)
{
  error_t err;

  memset(q, 0, sizeof(string_results_query_t));

  err = setup_results_query( &q->results,
                       QUERY_TYPE_STRING_RESULTS, loc,
                       requestor, do_string_results_query,
                       chunk_size, result_type );
  if( err ) return err;


  err = setup_string_query( &q->string_query, &q->results.proc,
                            loc, plen, pat);

  if( err ) return err;

  return ERR_NOERR;
}

void cleanup_string_results_query( string_results_query_t* q)
{
  assert( q->results.proc.entry.type == QUERY_TYPE_STRING_RESULTS );
  cleanup_string_query( & q->string_query );
  cleanup_range_to_results_query( &q->rtrq );

  cleanup_results_query(&q->results);
}


void do_string_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  string_results_query_t* s = NULL;
  range_to_results_query_t* rtrq = NULL;
  string_query_t* string_query = NULL;
  error_t err;


  switch ( mode ) {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_STRING_RESULTS );
      s = (string_results_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_STRING ) {
        string_query = (string_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_RANGE_TO_RESULTS ) {
        rtrq = (range_to_results_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_STRING_RESULTS );
      s = (string_results_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

  /** The normal algorithm:
      [first,last] = do_string_query;
      results = range_to_results;
      */

  switch (s->results.proc.entry.state) {
    case 0x0: // initial setup
      if( DEBUG > 8 ) printf("string_results_t %p starting\n", s);
      // run the string query.
      err = schedule_query(ss, (query_entry_t*) &s->string_query);
      CHECK_ERROR(err);
      WAIT_STATE(0x1);
    case 0x1:
      if( DEBUG > 8 ) printf("string_results_t %p done string query\n", s);
      // we should have a string query response.
      CHECK_RESPONSE(string_query);
      // proceed to getting a chunk with range_to_results.
      // fall through
    case 0x5: // setup the range_to_results query.
      err = setup_range_to_results_query( &s->rtrq,
               &s->results.proc, s->results.proc.entry.loc,
               s->results.chunk_size, results_type(&s->results.results),
               s->string_query.first, s->string_query.last);
      CHECK_ERROR(err);
      // fall through
    case 0x10: // get a chunk
      if( DEBUG > 8 ) printf("string_results_t %p getting chunk\n", s);
      err = schedule_query(ss, (query_entry_t*) &s->rtrq);
      CHECK_ERROR(err);
      WAIT_STATE(0x20);
    case 0x20:
      // we should have a chunk
      if( DEBUG > 8 ) printf("string_results_t %p got chunk\n", s);
      CHECK_RESPONSE( rtrq );

      // fall through
    case 0x30:
      // use the results.
      if( s->rtrq.results.is_last_chunk ) {
        s->results.proc.entry.state = 0x5; // repeat the other query.
        s->results.is_last_chunk = 1;
      } else {
        s->results.proc.entry.state = 0x10;
        s->results.is_last_chunk = 0;
      }
      results_move( &s->results.results, &s->rtrq.results.results);
      if( DEBUG > 8 ) printf("string_results_t %p delivering results\n", s);
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
  }
}

error_t setup_regexp_results_query( regexp_results_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int64_t chunk_size, result_type_t result_type,
                             struct ast_node* ast_node)
{
  error_t err;

  memset(q, 0, sizeof(regexp_results_query_t));

  err = setup_results_query( &q->results,
                       QUERY_TYPE_REGEXP_RESULTS, loc,
                       requestor, do_regexp_results_query,
                       chunk_size, result_type );
  if( err ) return err;


  if( ast_node->type != AST_NODE_REGEXP ) {
    return ERR_PARAM;
  }

  err = setup_regexp_query( &q->regexp, &q->results.proc, loc, ast_node);
  if( err ) return err;

  q->rtrq.results.proc.entry.type = 0;

  return ERR_NOERR;
}

void cleanup_regexp_results_query( regexp_results_query_t* q)
{
  assert( q->results.proc.entry.type == QUERY_TYPE_REGEXP_RESULTS );
  cleanup_regexp_query( & q->regexp );
  if( q->rtrq.results.proc.entry.type ) cleanup_range_to_results_query( &q->rtrq );

  cleanup_results_query(&q->results);
}


void do_regexp_results_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  regexp_results_query_t* s = NULL;
  range_to_results_query_t* rtrq = NULL;
  regexp_query_t* regexp_query = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      // Called this way to start the job
      assert( qe->type == QUERY_TYPE_REGEXP_RESULTS );
      s = (regexp_results_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_REGEXP ) {
        regexp_query = (regexp_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_RANGE_TO_RESULTS ) {
        rtrq = (range_to_results_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_REGEXP_RESULTS );
      s = (regexp_results_query_t*) qe->parent;
      break;
    default:
      assert(0);
  }

  /** The normal algorithm:

      do the regular expression query -> gives num_ranges
      sort the results

      cur = 0;
      results = { };
      while(1) {
        if cur >= num_ranges
          set that we're on the last chunk
          return results
        if results.size >= chunk_size
          return results

        chunk_reader = range_to_results_reader(results[cur].range)
        chunk = chunk_reader.next
        while(1) {
          if( results >= chunk_size ) 
            return results;
          results = results UNION chunk
          if chunk is the last chunk  {
            cur++
            break; // go to outer while loop start to get new chunk
          }
        }
      }
    */
        
  while(1) {
  switch (s->results.proc.entry.state) {
    case 0x0: // initial setup
      if( DEBUG > 8 ) printf("regexp_results_t %p starting\n", s);
      // run the string query.
      err = schedule_query(ss, (query_entry_t*) &s->regexp);
      CHECK_ERROR( err );
      WAIT_STATE(0x1);
    case 0x1:
      if( DEBUG > 8 ) printf("regexp_results_t %p done regexp query\n", s);
      // we should have a regexp query response.
      CHECK_RESPONSE(regexp_query);

      // sort the results. -- now done in do-regexp-query
      //regexp_result_list_sort(&regexp_query->results);
      // fall through
    case 0x2: // entry point for restarting the search.
      s->cur = 0;
      s->results.proc.entry.state = 0x10;
      // fall through
    case 0x10: //entry point for getting another chunk after changing cur.
      // clear the results
      results_clear_set_type(&s->results.results,
                             results_type(&s->results.results));
      s->num_results = 0;
      s->results.proc.entry.state = 0x20;
      break; // loop around.
    case 0x15: // entry point for getting another chunk without changing cur.
      results_clear_set_type(&s->results.results,
                             results_type(&s->results.results));
      s->num_results = 0;
      s->results.proc.entry.state = 0x30;
      break; // go to state 0x30
    case 0x20: // outer while loop.
      if( s->cur >= s->regexp.results.num_results ) {
        // we've returned the last chunk.
        s->results.is_last_chunk = 1;
        // restart at 0x2
        s->results.proc.entry.state = 0x2;
        RETURN_RESULT;
      }
      if( s->num_results >= s->results.chunk_size ) {
        // we've got a whole chunk.
        s->results.is_last_chunk = 0;
        // start at case 0x10.
        s->results.proc.entry.state = 0x10;
        RETURN_RESULT;
      }
      err = setup_range_to_results_query( &s->rtrq, &s->results.proc,
                                      s->results.proc.entry.loc,
                                      s->results.chunk_size,
                                      results_type(&s->results.results),
                                      s->regexp.results.results[s->cur].first,
                                      s->regexp.results.results[s->cur].last);
      CHECK_ERROR(err);
      // fall through
    case 0x30: // inner while loop
      if( s->num_results >= s->results.chunk_size ) {
        // we've got a whole chunk.
        s->results.is_last_chunk = 0;
        // start at case 0x15.
        s->results.proc.entry.state = 0x15;
        RETURN_RESULT;
      }
      err = schedule_query(ss, (query_entry_t*) &s->rtrq);
      CHECK_ERROR( err );
      WAIT_STATE(0x31);
    case 0x31:
      // we should have a range-to-results
      CHECK_RESPONSE(rtrq);
      // union it with our results.
      s->num_results += results_num_results(&rtrq->results.results);
      err = unionResults(&s->results.results,
                         &rtrq->results.results,
                         &s->results.results);
      CHECK_ERROR( err );
      if( rtrq->results.is_last_chunk ) {
        s->cur++;
        s->results.proc.entry.state = 0x20; // start of outer while loop
        break;
      } else {
        s->results.proc.entry.state = 0x30; // start of inner while loop
        break;
      }
    default:
      RETURN_ERROR(ERR_INVALID);
  }
  }
}

error_t create_generic_ast_count_query( process_entry_t** q_ptr,
                                        process_entry_t* requestor,
                                        index_locator_t loc,
                                        struct ast_node* ast_node)
{
  error_t err;
  process_entry_t* ret;
  
  *q_ptr = NULL;
  
  // allocate and setup_... and return in q_ptr
  switch( ast_node->type ) {
    // set ret.
    case AST_NODE_BOOL:
      return ERR_INVALID_STR("Boolean queries not supported for count");
    case AST_NODE_REGEXP:
      ret = calloc(1, sizeof(regexp_query_t));
      if( ! ret ) return ERR_MEM;
      err = setup_regexp_query( (regexp_query_t*) ret,
                                requestor, 
                                loc,
                                ast_node);
      break;
    case AST_NODE_STRING:
      {
        struct string_node* node = (struct string_node*) ast_node;

        ret = calloc(1, sizeof(string_query_t));
        if( ! ret ) return ERR_MEM;
        err = setup_string_query( (string_query_t*) ret,
                                  requestor,
                                  loc,
                                  node->string.len, node->string.chars);
      }
      break;
    default:
      assert(0);
  }
  
  *q_ptr = ret;
  return err;
}

void destroy_generic_ast_count_query( process_entry_t* q)
{
  switch( q->entry.type ) {
    // set ret.
    case QUERY_TYPE_REGEXP:
      cleanup_regexp_query((regexp_query_t*)q);
      break;
    case QUERY_TYPE_STRING:
      cleanup_string_query((string_query_t*)q);
      break;
    default:
      assert(0);

  }
  
  free(q);
  
}


error_t create_generic_ast_query( results_query_t** q_ptr,
                                  process_entry_t* requestor,
                                  index_locator_t loc,
                                  int64_t chunk_size,
                                  result_type_t result_type,
                                  struct ast_node* ast_node)
{
  error_t err;
  results_query_t* ret;
  
  *q_ptr = NULL;
  
  // allocate and setup_... and return in q_ptr
  switch( ast_node->type ) {
    // set ret.
    case AST_NODE_BOOL:
      ret = calloc(1, sizeof(generic_boolean_query_t));
      if( ! ret ) return ERR_MEM;
      err = setup_generic_boolean_query( (generic_boolean_query_t*) ret,
                                         requestor, 
                                         loc,
                                         chunk_size,
                                         (struct boolean_node*) ast_node);
      break;
    case AST_NODE_REGEXP:
      ret = calloc(1, sizeof(regexp_results_query_t));
      if( ! ret ) return ERR_MEM;
      err = setup_regexp_results_query( (regexp_results_query_t*) ret,
                                        requestor, 
                                        loc,
                                        chunk_size,
                                        result_type,
                                        ast_node);
      break;
    case AST_NODE_STRING:
      {
        struct string_node* node = (struct string_node*) ast_node;

        ret = calloc(1, sizeof(string_results_query_t));
        if( ! ret ) return ERR_MEM;
        err = setup_string_results_query( (string_results_query_t*) ret,
                                          requestor,
                                          loc, chunk_size, result_type,
                                          node->string.len, node->string.chars);
      }
      break;
    default:
      assert(0);

  }
  
  *q_ptr = ret;
  return err;
}

void cleanup_generic_results_query ( results_query_t* q)
{
  switch( q->proc.entry.type ) {
    // set ret.
    case QUERY_TYPE_BOOLEAN:
      cleanup_generic_boolean_query((generic_boolean_query_t*)q);
      break;
    case QUERY_TYPE_REGEXP_RESULTS:
      cleanup_regexp_results_query((regexp_results_query_t*)q);
      break;
    case QUERY_TYPE_STRING_RESULTS:
      cleanup_string_results_query((string_results_query_t*)q);
      break;
    default:
      assert(0);
  }
}
  
void destroy_generic_ast_query( results_query_t* q)
{
  cleanup_generic_results_query(q);
  free(q);
}

error_t setup_generic_boolean_query( generic_boolean_query_t* q,
                                     process_entry_t* requestor, 
                                     index_locator_t loc,
                                     int64_t chunk_size,
                                     struct boolean_node* ast_node)
{
  error_t err;
  query_callback callback;
  result_type_t result_type;

  memset(q, 0, sizeof(generic_boolean_query_t));
  
  // depending on the type of the AST node, set callback and result_type
  //  appropriately.
  
  q->ast_node = ast_node;
  switch(ast_node->nodeType)
  {
    case BOOL_AND:
      callback = do_and_query;
      result_type = RESULT_TYPE_DOCUMENTS;
      break;
    case BOOL_OR:
      callback = do_or_query;
      result_type = RESULT_TYPE_DOCUMENTS;
      break;
    case BOOL_NOT:
      callback = do_not_query;
      result_type = RESULT_TYPE_DOCUMENTS;
      break;
    case BOOL_THEN:
      callback = do_ThenWithin_query;
      result_type = RESULT_TYPE_DOC_OFFSETS;
      break;
    case BOOL_WITHIN:
      callback = do_ThenWithin_query;
      result_type = RESULT_TYPE_DOC_OFFSETS;
      break;
    default:
      assert(0);
  }

  err = setup_results_query( &q->results_query,
                       QUERY_TYPE_BOOLEAN, loc,
                       requestor, callback,
                       chunk_size, result_type);
  if( err ) return err;
 

  // recursively construct the queries for 
  // left and right based on the ast_node.
  err = create_generic_ast_query(&q->left, (process_entry_t*)q, loc, chunk_size, result_type, ast_node->left);
  if( err ) return err;
  
  err = create_generic_ast_query(&q->right, (process_entry_t*)q, loc,
      chunk_size, result_type, ast_node->right);
  if( err ) return err;
  

  return ERR_NOERR;
}

void cleanup_generic_boolean_query(generic_boolean_query_t* q)
{
  assert( q->results_query.proc.entry.type == QUERY_TYPE_BOOLEAN );
  //free_ast_node(ast_node);

  // recursively free queries for left and right
  // and free left and right
  destroy_generic_ast_query(q->left);
  destroy_generic_ast_query(q->right);

  cleanup_results_query(&q->results_query);
}



void do_and_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  generic_boolean_query_t* s = NULL;
  results_query_t* r = NULL;
  error_t error;

  switch ( mode ) 
  {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_BOOLEAN );
      s = (generic_boolean_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( is_result_query(qe->type) ) {
        r = (results_query_t*) qe;
      } else assert(0);
      s = (generic_boolean_query_t*) qe->parent;
      break;
    default:
      assert(0);
      break;
  }

  /* The normal algorithm:
   * Acuiqre a list of documents for the matches of the left and right
   * sides of the expression, and order them.  Go through the left side list
   * looking for matches in the right side list until you pass the current
   * document # in the right list, then advance in the left list. If there
   * is a match, advance both.
   
   for each chunk1 of E1 results  {
      for each chunk2 of E2 results  {
         sort(chunk1)
         sort(chunk2)
         result1 = chunk1[0]
         result2 = chunk2[0]
         while result1 within chunk1 and result2 within chunk2  {
             if result1 < result2  {
                  increment result1  }
             else if result1 > result2  {
               increment result2  }
             else if result1 == result2  {
               add result to result listing
               increment result1 & result2 until new result  }
         }
      }
    }
   */

  while (1) 
  {
    long* myState = &s->results_query.proc.entry.state;
    switch( *myState ) 
    {
      case 0x0:
        //Schedule the two queries (left & right)
        s->left->request_state = 0x010;
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->left );
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x100);
      case 0x1:
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x110);
      case 0x100:
      case 0x101:
      case 0x110:
        *myState |= r->request_state;
        if( *myState != 0x111 )
        {
          WAIT;
        }
        /* Fall through.. */
      case 0x200:
      {
        error = intersectResults(&s->left->results, &s->right->results,&s->results_query.results);
        CHECK_ERROR(error);
        if(s->right->is_last_chunk == 1)
        {
          *myState = 0x0; // the get both chunks option..
        
          if(s->left->is_last_chunk == 1)
          {
            // we're done
            s->results_query.is_last_chunk = 1;
          }
        } 
        else 
        {
          *myState = 0x1; // get the next chunk only from the right
        }
        
        // return with the chunk we found
        RETURN_RESULT;
      }
      default:
        RETURN_ERROR(ERR_INVALID);
    
    }
  }

}

void do_or_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  generic_boolean_query_t* s = NULL;
  results_query_t* r = NULL;
  error_t error;

  switch ( mode ) 
  {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_BOOLEAN );
      s = (generic_boolean_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( is_result_query(qe->type) ) {
        r = (results_query_t*) qe;
      } else assert(0);
      s = (generic_boolean_query_t*) qe->parent;
      break;
    default:
      assert(0);
      break;
  }

  /* The normal algorithm:
   * Get a list of matching documents for both the left and right side of 
   * the boolean expression.  Go through both lists in parallel comparing
   * for matches.
   */

#define ERROR(xxx) {s->results_query.proc.entry.err_code = err_code(xxx); return;}

  while (1) 
  {
    long* myState = &s->results_query.proc.entry.state;
    switch( *myState ) 
    {
      case 0x0:
        //Schedule the two queries (left & right)
        s->left->request_state = 0x010;
        s->right->request_state = 0x001;
        error = schedule_query(ss, (query_entry_t*)s->left );
        CHECK_ERROR( error );
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x100);
      case 0x1: //Return left chunks (no more data on right)
        schedule_query(ss, (query_entry_t*)s->left );
        WAIT_STATE(0x240);
      case 0x2: //Return right chunks (no more data on left)
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x250);
      case 0x100:
      case 0x101:
      case 0x110:
        *myState |= r->request_state;
        if( *myState != 0x111 )
        {
          WAIT;
        }
        /* Fall through.. */
      case 0x200:
      
        error = unionResults(&s->left->results, 
                             &s->right->results,
                             &s->results_query.results);
        CHECK_ERROR(error);
        *myState = 0x300;
        break; // go to state 300
      case 0x240:
        // got left chunks
        results_move(&s->results_query.results, &s->left->results);
        *myState = 0x300;
        break; // go to state 300
      case 0x250:
        // got right chunks
        results_move(&s->results_query.results, &s->right->results);
        *myState = 0x300;
        break; // go to state 300
      case 0x300:
        if(s->right->is_last_chunk == 1)
        {
          if(s->left->is_last_chunk == 1)
          {
            // we're done
            s->results_query.is_last_chunk = 1;
          }
          else
          {
            //Out of data on the right, return left chunks next time
            *myState = 0x1;
          }
        } 
        else 
        {
          if(s->results_query.is_last_chunk == 1)
          {
            //Out of data on the left, return right chunks next time
            *myState = 0x2;
          }
        }
        // return with the chunk we found
        RETURN_RESULT;
      default:
        RETURN_ERROR(ERR_INVALID);
    
    }
  }

}



void do_not_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  generic_boolean_query_t* s = NULL;
  results_query_t* r = NULL;
  error_t error;

  switch ( mode ) 
  {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_BOOLEAN );
      s = (generic_boolean_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( is_result_query(qe->type) ) {
        r = (results_query_t*) qe;
      } else assert(0);
      s = (generic_boolean_query_t*) qe->parent;
      break;
    default:
      assert(0);
      break;
  }

  /* The normal algorithm:
   * output <- chunk from left
   *  for each chunk on the right
   *    output = output - chunk on right
   * recursive call (output)
   */


  while (1) 
  {
    long* myState = &s->results_query.proc.entry.state;
    switch( *myState ) 
    {
      case 0x0:
        //Schedule the two queries (left & right)
        s->left->request_state = 0x010;
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->left );
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x100);
      case 0x1:
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x210);
      case 0x100:
      case 0x101:
      case 0x110:
        *myState |= r->request_state;
        if( *myState != 0x111 )
        {
          WAIT;
        }
        /* Fall through.. */
      case 0x200:
      {
        results_move(&s->results_query.results, &s->left->results);
        *myState = 0x300;
        break;
      }
      case 0x210:
      {
        assert( r->request_state == 0x001 );
        *myState = 0x300;
        /* Fall through.. */
      }
      case 0x300:
      {
        error = subtractResults(&s->results_query.results, 
                                &s->right->results,
                                &s->results_query.results);
        CHECK_ERROR(error);
        
        if(s->right->is_last_chunk == 1)
        {
          *myState = 0x0; // the get both chunks option..
        
          if(s->left->is_last_chunk == 1)
          {
            // we're done
            s->results_query.is_last_chunk = 1;
          }
          // return with the chunk we found
          RETURN_RESULT;
        } 
        else 
        {
          *myState = 0x1; // get the next chunk only from the right
          break; // goes back to state 0x1 in the while loop
        }
      }
      default:
        RETURN_ERROR(ERR_INVALID);
    
    }
  }

}

void do_ThenWithin_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  generic_boolean_query_t* s = NULL;
  results_query_t* r = NULL;
  error_t error;
  int dist;

  switch ( mode ) 
  {
    case QUERY_MODE_START:
      assert( qe->type == QUERY_TYPE_BOOLEAN );
      s = (generic_boolean_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( is_result_query(qe->type) ) {
        r = (results_query_t*) qe;
      } else assert(0);
      s = (generic_boolean_query_t*) qe->parent;
      break;
    default:
      assert(0);
      break;
  }
  dist = s->ast_node->distance;

  /* The normal algorithm:
   *  for each chunk on left
   *    for each chunk on right
   *        if  0 < rightElementLoc - leftElementLoc < distance
   *          add document to result
   *        else if  0 > rightElementLoc - leftElementLoc > distance
   *          add document ro result
   */

  while (1) 
  {
    long* myState = &s->results_query.proc.entry.state;
    switch( *myState ) 
    {
      case 0x0:
        //Schedule the two queries (left & right)
        s->left->request_state = 0x010;
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->left );
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x100);
      case 0x1:
        s->right->request_state = 0x001;
        schedule_query(ss, (query_entry_t*)s->right );
        WAIT_STATE(0x110);
      case 0x100:
      case 0x101:
      case 0x110:
        *myState |= r->request_state;
        if( *myState != 0x111 )
        {
          WAIT;
        }
        /* Fall through.. */
      case 0x200:
      {
        if(s->ast_node->nodeType == BOOL_THEN)
        {
          error = thenResults(&s->left->results, 
                              &s->right->results,
                              &s->results_query.results,
                              dist);
        }
        else
        {
          error = withinResults(&s->left->results, 
                              &s->right->results,
                              &s->results_query.results,
                              dist);
        }
        
        CHECK_ERROR(error);
        if(s->right->is_last_chunk == 1)
        {
          if(s->left->is_last_chunk == 1)
          {
            // we're done
            s->results_query.is_last_chunk = 1;
          }
          *myState = 0x0; // start over with both
                          // or get left and right, when we're on last right.
        } else {
          *myState = 0x1;
        }
        // return with the chunk we found
        RETURN_RESULT;
      }
      default:
        RETURN_ERROR(ERR_INVALID);
    
    }
  }

}

#if SUPPORT_INDEX_MERGE
void setup_backward_merge_query(backward_merge_query_t* s,
                                process_entry_t* requestor, 
                                index_locator_t src_loc,
                                index_locator_t dst_loc,
                                int64_t mark_period,
                                int64_t c_src_seof,
                                int64_t c_dst_seof,
                                int64_t src_doc_num,
                                int64_t src_doc_len,
                                int64_t dst_num_docs,
                                int64_t dst_total_len )
{
  error_t err;

  memset(s, 0, sizeof(backward_merge_query_t));

  err = setup_process_entry(&s->proc, QUERY_TYPE_BACKWARD_MERGE,
                      null_index_locator(), 0, requestor,
                      do_backward_merge_query);
  if( err ) return err;

  s->mark_period = mark_period;
  s->doc = src_doc_num + dst_num_docs;

  s->src_hq.leaf.entry.loc = src_loc;
  s->dst_hq.leaf.entry.loc = dst_loc;
  s->src_bq.leaf.entry.loc = src_loc;
  s->dst_bq.leaf.entry.loc = dst_loc;
  s->dst_update.leaf.entry.loc = dst_loc;

  s->dst_total_len = dst_total_len;

  s->src_row = c_src_seof + src_doc_num;
  s->dst_row = c_dst_seof + dst_num_docs - 1;

  s->src_doc_len = src_doc_len;
  s->offset = src_doc_len - 1;
}

void do_backward_merge_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  backward_merge_query_t* s = NULL;
  block_query_t* bq = NULL;
  header_occs_query_t* hq = NULL;
  block_update_query_t* update = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      assert(qe->type == QUERY_TYPE_BACKWARD_MERGE);
      s = (backward_merge_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK ) {
        bq = (block_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK_UPDATE ) {
        update = (block_update_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_BACKWARD_MERGE );
      s = (backward_merge_query_t*) qe->parent;
      break;
  }

  /*

     The normal algorithm:

    backward_merge(source index,
                   destination index,
                   source document number) {
      dst_row = Cdst['#'] + dst_number_of_documents - 1;
      src_row = Csrc['#'] + doc;
      do {
        ch = Lsrc[src_row];
        // update the destination index:
        add ch below dst_row in the destination index
        src_row = Csrc[ch] + Occsrc(ch, src_row) - 1;
        dst_row = Cdst[ch] + Occdst(ch, dst_row) - 1;
      } while( ch != '#' );
    }
  */

  while( 1 ) {
  switch( s->proc.entry.state ) {
    case 0x0:
      /* Assume that we have already set up:
          - doc_num
          - offset (length of the document - 1)
          - dst_row = C['#'] + dst_number_of_documents - 1
          - src_row = C['#'] + doc
          - dst_hq, dst_bq, and dst_update have index_loc set to dst index
          - src_hq and src_bq have index_loc set to src index
          */
      // initialize things.
      s->proc.entry.state = 0x100;
      // fall through
    case 0x100:
      // begin do-while loop.

      // get Lsrc[src_row]
      // first, find the block containing src_row
      // this will set s->src_hq.r.block_num and s->src_hq.r.row
      setup_header_occs_query(&s->src_hq, s->src_hq.leaf.entry.loc,
                              0x200, &s->proc, 
                              HDR_BSEARCH_BLOCK_ROWS,
                              0, 0, s->src_row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->src_hq);
      CHECK_ERROR(err);
      WAIT_STATE(0x200);
    case 0x200:
      CHECK_RESPONSE(hq);
      // get Lsrc[src_row]
      // now find the character in that block
      // this will set s->src_bq.r.ch and s->src_bq.r.occs
      setup_block_query(&s->src_bq, s->src_bq.leaf.entry.loc,
                        0x300, &s->proc,
                        BLOCK_REQUEST_CHAR | BLOCK_REQUEST_OCCS,
                        s->src_hq.r.block_num, 
                        0, s->src_row - s->src_hq.r.row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->src_bq);
      CHECK_ERROR(err);
      WAIT_STATE(0x300);
    case 0x300:
      CHECK_RESPONSE(bq);
      // now we have L[src_row]. Set s->ch.
      s->ch = s->src_bq.r.ch;


      // request C[ch] from both indexes.
      // from src - we already know the block number and the occurences
      // within that block.
      setup_header_occs_query(&s->src_hq, s->src_hq.leaf.entry.loc,
                              0x401, &s->proc,
                              HDR_REQUEST_C|HDR_REQUEST_BLOCK_OCCS|HDR_BACK,
                              s->src_hq.r.block_num, s->ch, s->src_row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->src_hq);
      CHECK_ERROR(err);

      // from dst. We also need to find the block number from dst
      // at this time.
      setup_header_occs_query(&s->dst_hq, s->dst_hq.leaf.entry.loc,
                              0x410, &s->proc,
                              HDR_BSEARCH_BLOCK_ROWS|
                              HDR_REQUEST_C|HDR_REQUEST_BLOCK_OCCS|HDR_BACK,
                              0, s->ch, s->dst_row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->dst_hq);
      CHECK_ERROR(err);
                              
      WAIT_STATE(0x400);
    case 0x400:
    case 0x401:
    case 0x410:
      // wait for both responses
      CHECK_RESPONSE(hq);
      s->proc.entry.state |= hq->leaf.entry.state;
      if( s->proc.entry.state != 0x411 ) WAIT; // wait for the other.
      // fall through
    case 0x500:
      // make the destination block request
      // from dst:
      setup_block_query(&s->dst_bq, s->dst_bq.leaf.entry.loc,
                        0x600, &s->proc,
                        BLOCK_REQUEST_OCCS,
                        s->dst_hq.r.block_num, 
                        s->ch, s->dst_row - s->dst_hq.r.row, 0);
      err = schedule_query(ss, (query_entry_t*) &s->dst_bq);
      CHECK_ERROR(err);
      WAIT_STATE(0x600);
    case 0x600:
      CHECK_RESPONSE(bq);

      // Add an update entry to the destination index.
      {
        int64_t offset_to_use;

        if( should_mark( s->mark_period, s->offset, s->src_doc_len) ) {
          offset_to_use = s->dst_total_len + s->offset;
        } else {
          offset_to_use = -1;
        }
        if( DEBUG > 4 ) {
          printf("Saving update: dst_block %i dst_row %i dst_row_in_block %i ch %i src_row %i offset %i doc %i\n", (int) s->dst_hq.r.block_num, (int) s->dst_row, (int) (s->dst_row - s->dst_hq.r.row), s->ch, (int) s->src_row, (int) offset_to_use, (int) s->doc);
        }
        setup_block_update_query(&s->dst_update, s->dst_update.leaf.entry.loc,
                                 0x700, &s->proc, 
                                 BLOCK_UPDATE_DATA,
                                 s->dst_hq.r.block_num,
                                 s->dst_row - s->dst_hq.r.row,
                                 s->ch, s->src_row, offset_to_use, s->doc);
        err = schedule_query(ss, (query_entry_t*) &s->dst_update);
        CHECK_ERROR(err);
      }
      WAIT_STATE(0x700);
    case 0x700:
      CHECK_RESPONSE(update);

      // update src_row and dst_row
      s->src_row = s->src_hq.r.occs + s->src_bq.r.occs_in_block - 1;
      if( DEBUG > 4 ) {
          printf("dst_row update: hq_occs %i occs_in_block %i\n", (int) s->dst_hq.r.occs, (int) s->dst_bq.r.occs_in_block);
      }
      s->dst_row = s->dst_hq.r.occs + s->dst_bq.r.occs_in_block - 1;
      // update offset.
      s->offset--;

      // fall through
    case 0x800:
      // return to 0x100 if we're not done.
      if( s->ch > ESCAPE_CODE_SEOF ) {
        s->proc.entry.state = 0x100;
        break; // loop back.
      }
      if( s->offset != -1 ) RETURN_ERROR(ERR_INVALID);
      // otherwise, we deliver our results.
      RETURN_RESULT;

    default:
      RETURN_ERROR(ERR_INVALID);
  }
  }
}
void setup_index_merge_query(index_merge_query_t* s,
                                process_entry_t* requestor, 
                                index_locator_t src_loc,
                                index_locator_t dst_loc,
                                index_locator_t out_loc,
                                index_block_param_t* index_params,
                                double ratio,
                                int start_uncompressed,
                                int max_uncompressed)
{
  error_t err;
  
  memset(s, 0, sizeof(index_merge_query_t));

  err = setup_process_entry(&s->proc, QUERY_TYPE_INDEX_MERGE,
                      null_index_locator(), 0, requestor,
                      do_index_merge_query);
  if( err ) return err;

  s->src_loc = src_loc;
  s->dst_loc = dst_loc;
  s->out_loc = out_loc;
  s->mq.r.out_loc = out_loc;
  s->mq.r.src_loc = src_loc;
  
  s->ratio = ratio;
  s->start_uncompressed = start_uncompressed;
  s->max_uncompressed = max_uncompressed;

  memcpy(&s->index_params, index_params, sizeof(index_block_param_t));

  memset(&s->backward_merges, 0, sizeof(parallel_query_t));
}

void do_index_merge_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  index_merge_query_t* s = NULL;
  header_occs_query_t* hoq = NULL;
  header_loc_query_t* hlq = NULL;
  block_update_manage_query_t* mq = NULL;
  parallel_query_t* pq = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      assert(qe->type == QUERY_TYPE_INDEX_MERGE);
      s = (index_merge_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_OCCS ) {
        hoq = (header_occs_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_HEADER_LOC ) {
        hlq = (header_loc_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_BLOCK_UPDATE_MANAGE ) {
        mq = (block_update_manage_query_t*) qe;
      } else if( qe->type == QUERY_TYPE_PARALLEL_QUERY ) {
        pq = (parallel_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_INDEX_MERGE );
      s = (index_merge_query_t*) qe->parent;
      break;
  }

  /*

     The normal algorithm:

     dst_num_blocks = the number of blocks in the dst index

     for( i = 0; i < dst_num_blocks; i++ ) {
       create destination update block i.
     }

     src_num_docs = Csrc[SEOF+1]
     dst_num_docs = Cdst[SEOF+1]
     dst_total_len = total length of dst index

     for( i = 0; i < src_num_docs; i++ ) {
       length = length of src document i
       run backward_merge with this document
     }

     Call the appropriate functions to do the final index merging.
     (that is, call into index.c, such as merge blocks)
  */

  while( 1 ) {
  switch( s->proc.entry.state ) {
    case 0x0:
      // start out.
      // get the number of blocks in the index.
      setup_header_occs_query(&s->hoq, s->dst_loc,
                              0x1, &s->proc, 
                              HDR_REQUEST_NUM_BLOCKS,
                              0, 0, 0, 0);
      err = schedule_query(ss, (query_entry_t*) &s->hoq);
      CHECK_ERROR(err);
      WAIT_STATE(0x1);
    case 0x1:
      CHECK_RESPONSE(hoq);
      s->dst_num_blocks = hoq->r.block_num;
      // get the size in total of the src index.
      setup_header_loc_query(&s->hlq, s->src_loc, 0x2, &s->proc,
                             HDR_LOC_REQUEST_L_SIZE, 
                             0, 0);
      err = schedule_query(ss, (query_entry_t*) &s->hlq);
      CHECK_ERROR(err);
      WAIT_STATE(0x2);
    case 0x2:
      CHECK_RESPONSE(hlq);
      s->src_total_len = hlq->r.doc_len;
      // get the total size of the dst index.
      setup_header_loc_query(&s->hlq, s->dst_loc, 0x3, &s->proc,
                             HDR_LOC_REQUEST_L_SIZE,
                             0, 0);
      err = schedule_query(ss, (query_entry_t*) &s->hlq);
      CHECK_ERROR(err);
      WAIT_STATE(0x3);
    case 0x3:
      CHECK_RESPONSE(hlq);
      s->dst_total_len = hlq->r.doc_len;

      // go through each the destination blocks creating the update block.
      s->i = 0;
      // fall through.
    case 0x10:
      if( s->i >= s->dst_num_blocks ) {
        // done for loop
        s->proc.entry.state = 0x100;
        break; // go to that state.
      }
      // make the request
      setup_block_update_manage_query(&s->mq, s->dst_loc, 0x20, &s->proc,
                                      BLOCK_UPDATE_CREATE, s->i,
                                      s->ratio, s->start_uncompressed,
                                      s->max_uncompressed,
                                      num_bits(s->src_total_len+s->dst_total_len),
                                      s->src_loc, s->out_loc,
                                      & s->index_params);
      err = schedule_query(ss, (query_entry_t*) &s->mq);
      CHECK_ERROR(err);
      WAIT_STATE(0x20);
    case 0x20:
      CHECK_RESPONSE(mq);
      // go back to the for loop.
      s->i++;
      s->proc.entry.state = 0x10;
      break; // loop around - go to state 10.
    case 0x100:
      // get the src number of documents
      setup_header_occs_query(&s->hoq, s->src_loc,
                              0x200, &s->proc,
                              HDR_REQUEST_C,
                              0, 1+ESCAPE_CODE_SEOF, 0, 0);
      err = schedule_query(ss, (query_entry_t*) &s->hoq);
      CHECK_ERROR(err);
      WAIT_STATE(0x200);
    case 0x200:
      CHECK_RESPONSE(hoq);
      s->src_num_docs = hoq->r.occs;
      // fall through
    case 0x300:
      // get the dst number of documents
      setup_header_occs_query(&s->hoq, s->dst_loc,
                              0x400, &s->proc,
                              HDR_REQUEST_C,
                              0, 1+ESCAPE_CODE_SEOF, 0, 0);
      err = schedule_query(ss, (query_entry_t*) &s->hoq);
      CHECK_ERROR(err);
      WAIT_STATE(0x400);
    case 0x400:
      CHECK_RESPONSE(hoq);
      s->dst_num_docs = hoq->r.occs;
      // fall through
    case 0x500:
      // set up the parallel query for the src documents.
      err = setup_parallel_query(&s->backward_merges, &s->proc,
                                 null_index_locator(),
                                 sizeof(backward_merge_query_t),
                                 s->src_num_docs);
      CHECK_ERROR(err);

      // now we need to initialize each one of the parallel queries.
      // To do that, we need to get each document length.
      s->i = 0;
      // fall through
    case 0x600:
      // for loop begins.
      if( s->i >= s->src_num_docs ) {
        s->proc.entry.state = 0x1000;
        break; // end the for loop.
      }
      // fall through
    case 0x700:
      // body of the for loop
      setup_header_loc_query( &s->hlq, s->src_loc, 0x800, &s->proc,
                              HDR_LOC_REQUEST_DOC_LEN, 
                              0, s->i);
      err = schedule_query(ss, (query_entry_t*) &s->hlq);
      CHECK_ERROR(err);
      WAIT_STATE(0x800);
    case 0x800:
      // got result.
      CHECK_RESPONSE(hlq);
      // we should have the document length.
      {
        backward_merge_query_t* bqm = (backward_merge_query_t*)
                                      ith_query(&s->backward_merges, s->i);
        setup_backward_merge_query(bqm, &s->backward_merges.proc,
                                   s->src_loc, s->dst_loc,
                                   s->mq.r.index_params.mark_period,
                                   0, 0, // C[SEOF] is always 0.
                                   s->i, hlq->r.doc_len,
                                   s->dst_num_docs, s->dst_total_len);
      }
      // fall through
    case 0x900:
      // increment i and go back to for loop start.
      s->i++;
      s->proc.entry.state = 0x600;
      break; // loop around.
    case 0x1000:
      // end of for loop. All parallel queries have been set up.
      // run the parallel queries
      schedule_query(ss, (query_entry_t*) &s->backward_merges);
      WAIT_STATE(0x1100); // wait for the parallel queries to complete.
    case 0x1100:
      CHECK_RESPONSE(pq);

      setup_block_update_manage_query(&s->mq, s->dst_loc, 0x5000, &s->proc,
                                      BLOCK_UPDATE_FINISH, 0,
                                      s->ratio, s->start_uncompressed,
                                      s->max_uncompressed,
                                      num_bits(s->src_total_len),
                                      s->src_loc, s->out_loc,
                                      & s->index_params);
      err = schedule_query(ss, (query_entry_t*) &s->mq);
      CHECK_ERROR(err);
      WAIT_STATE(0x5000); // wait for the final merging to complete
      // fall through
    case 0x5000:
      CHECK_RESPONSE(mq);
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
  }
  }
}

#endif

error_t setup_extract_document_query(extract_document_query_t* s,
                                process_entry_t* requestor, 
                                index_locator_t loc,
                                int64_t doc_num)
{
  error_t err;

  memset(s, 0, sizeof(extract_document_query_t));

  err = setup_process_entry(&s->proc, QUERY_TYPE_EXTRACT_DOCUMENT,
                      loc, 0, requestor,
                      do_extract_document_query);
  if( err ) return err;

  s->doc_num = doc_num;

  return ERR_NOERR;
}

void cleanup_extract_document_query(extract_document_query_t* s)
{
  assert( s->proc.entry.type == QUERY_TYPE_EXTRACT_DOCUMENT );
  free(s->content);
  s->content = NULL;
  cleanup_process_entry(&s->proc);
}

void do_extract_document_query(server_state_t* ss, query_mode_t mode, query_entry_t* qe)
{
  extract_document_query_t* s = NULL;
  header_loc_query_t* hq = NULL;
  context_query_t* ctx = NULL;
  error_t err;

  switch ( mode ) {
    case QUERY_MODE_START:
      assert(qe->type == QUERY_TYPE_EXTRACT_DOCUMENT );
      s = (extract_document_query_t*) qe;
      break;
    case QUERY_MODE_RESULT:
      if( qe->type == QUERY_TYPE_HEADER_LOC ) {
        hq = (header_loc_query_t*) qe;
      } else if ( qe->type == QUERY_TYPE_CONTEXT ) {
        ctx = (context_query_t*) qe;
      } else assert(0);
      assert( qe->parent->entry.type == QUERY_TYPE_EXTRACT_DOCUMENT );
      s = (extract_document_query_t*) qe->parent;
      break;
  }

  /*

     The normal algorithm:

     Get the length of the document
     and the EOF row (header query).
     Make a context query for the entire document.
  */

  switch( s->proc.entry.state ) {
    case 0x0:
      // initialize things.
      s->proc.entry.state = 0x100;
      // fall through
    case 0x100:
      // get the length of the document.
      setup_header_loc_query(&s->hq, s->proc.entry.loc, 
                             0x200, &s->proc,
                             HDR_LOC_REQUEST_DOC_LEN |
                             HDR_LOC_REQUEST_DOC_EOF_ROW,
                             0, s->doc_num);
      err = schedule_query(ss, (query_entry_t*) &s->hq);
      CHECK_ERROR(err);
      WAIT_STATE(0x200);
    case 0x200:
      // we should have a header query response.
      CHECK_RESPONSE(hq);

      s->doc_len = s->hq.r.doc_len;

      // setup the context query.
      err = setup_context_query(&s->ctx, &s->proc, s->proc.entry.loc,
                                s->hq.r.offset,/* the EOF row */
                                s->doc_len-1, 1, LOCATE_NONE);
      CHECK_ERROR(err);
      err = schedule_query(ss, (query_entry_t*) &s->ctx);
      CHECK_ERROR(err);
      WAIT_STATE(0x300);
    case 0x300:
      CHECK_RESPONSE(ctx);

      s->content = s->ctx.context;
      s->ctx.context = NULL;

      cleanup_context_query(&s->ctx);
      // we've got the context! return the result.
      RETURN_RESULT;
    default:
      RETURN_ERROR(ERR_INVALID);
  }
}


void cleanup_generic(query_entry_t* e)
{
  switch( e->type ) {
    case QUERY_TYPE_STRING:
      cleanup_string_query((string_query_t*) e);
      break;
    case QUERY_TYPE_REGEXP:
      cleanup_regexp_query((regexp_query_t*) e);
      break;
    case QUERY_TYPE_PARALLEL_QUERY:
      {
        parallel_query_t* pq = (parallel_query_t*) e;
        for( long i = 0; i < pq->num_queries; i++ ) {
          cleanup_generic(ith_query(pq, i));
        }
      }
      break;
    case QUERY_TYPE_BOOLEAN:
    case QUERY_TYPE_REGEXP_RESULTS:
    case QUERY_TYPE_STRING_RESULTS:
      cleanup_generic_results_query ((results_query_t*) e);
      break;
    case QUERY_TYPE_RANGE_TO_RESULTS:
      cleanup_range_to_results_query((range_to_results_query_t*) e);
      break;
    default:
      assert(0);
  }
}

int size_generic( query_type_t type )
{
  switch( type ) {
    case QUERY_TYPE_STRING:
      return sizeof(string_query_t);
    case QUERY_TYPE_PARALLEL_QUERY:
      return sizeof(parallel_query_t);
    default:
      assert(0);
  }
}



