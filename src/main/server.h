/*
  (*) 2006-2014 Michael Ferguson <michaelferguson@acm.org>

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

  femto/src/main/server.h
*/
#ifndef _SERVER_H_
#define _SERVER_H_ 1

#include <pthread.h>

#include "sysqueue.h"
#include "systree.h"
#include "index.h"
#include "nfa.h"
#include "results.h"
#include "ast.h"
#include "queue_map.h"
#include "index_merge.h"

typedef struct {
  int64_t block_id;
  index_locator_t index_loc;
} block_locator_t;

typedef struct {
  index_block_type_t type;
  union {
    header_block_t header_block;
    data_block_t data_block;
#if SUPPORT_INDEX_MERGE
    update_block_t update_block;
#endif
  } u;
} server_block_t;

/*
typedef struct {
  int64_t block_number;
  int row_in_block;
} block_row_t;
*/

typedef enum {
  // leaf queries
  QUERY_TYPE_UNDER_LEAF = 1, // marker
    QUERY_TYPE_HEADER_OCCS,
    QUERY_TYPE_HEADER_LOC,
    QUERY_TYPE_BLOCK,
    QUERY_TYPE_BLOCK_CHUNK,
#if SUPPORT_INDEX_MERGE
    QUERY_TYPE_BLOCK_UPDATE_MANAGE,
    QUERY_TYPE_BLOCK_UPDATE,
#endif
    // when adding more leaf queries, update leaf_entry_size
  QUERY_TYPE_OVER_LEAF,// marker
  // process queries
  QUERY_TYPE_UNDER_PROC, // marker
    QUERY_TYPE_SIGNAL, // pthread signal when done.
    QUERY_TYPE_STRING,
    QUERY_TYPE_BACKWARD_SEARCH,
    //QUERY_TYPE_FORWARD_SEARCH,
    QUERY_TYPE_BACK,
    QUERY_TYPE_FORWARD,
    QUERY_TYPE_CONTEXT,
    QUERY_TYPE_REGEXP,
    QUERY_TYPE_PARALLEL_QUERY,
    QUERY_TYPE_LOCATE,
    QUERY_TYPE_GET_URL,
    QUERY_TYPE_EXTRACT_DOCUMENT,
#if SUPPORT_INDEX_MERGE
    QUERY_TYPE_BACKWARD_MERGE,
    QUERY_TYPE_INDEX_MERGE,
#endif

    // results queries (which are also process queries)
    QUERY_TYPE_UNDER_RESULTS, // marker
      QUERY_TYPE_RANGE_TO_RESULTS,
      QUERY_TYPE_STRING_RESULTS,
      QUERY_TYPE_REGEXP_RESULTS,
      QUERY_TYPE_BOOLEAN,
      //QUERY_TYPE_GENERIC, // integrate with femto.h/femto.c
    QUERY_TYPE_OVER_RESULTS, // marker

  QUERY_TYPE_OVER_PROC, // marker
} query_type_t;

static inline int is_result_query(query_type_t t) 
{
  return (QUERY_TYPE_UNDER_RESULTS < t && t < QUERY_TYPE_OVER_RESULTS);
}
static inline int is_leaf_query(query_type_t t)
{
  return (QUERY_TYPE_UNDER_LEAF < t && t < QUERY_TYPE_OVER_LEAF);
}
static inline int is_process_query(query_type_t t)
{
  return (QUERY_TYPE_UNDER_PROC < t && t < QUERY_TYPE_OVER_PROC );
}

typedef enum {
  QUERY_MODE_START,
  QUERY_MODE_RESULT
} query_mode_t;

// forward structure declarations.
struct server_state;
struct process_entry;

typedef struct query_entry {
  // Shared response info
  unsigned char err_code;
  // Shared request info
  query_type_t type;
  long state; // can store a sequence number or some state #
  index_locator_t loc; // which index is this for?
  struct process_entry* parent;
  // Next entry in a per-thread work list
  LIST_ENTRY(query_entry) entries;
} query_entry_t;

typedef void (*query_callback)(struct server_state* /*server_state_t*/,
                               query_mode_t,
                               struct query_entry* query);

typedef struct process_entry { 
  query_entry_t entry;


  pthread_mutex_t lock; // protects the following elements
                        // always lock a parent process before
                        // a child.

  int setup; // if 0, this has not been set up yet.

  int num_pending_requests; // the number of pending requests..


  int canceled; // is the query canceled?
                 // This is not really protected by any lock at all
                 // but to set it, one should use .lock.

  int done; // set when return_result is called.

  // called when a result comes in with the result (and 
  // result->parent equal to the requestor)
  // or with the requestor when starting the task.
  // lock is held while we're in callback (note -- callback can
  // call the callback for a parent, but not for a child).
  query_callback callback;
} process_entry_t;

typedef enum {
  REQ_INITED = 1,
  REQ_BEGUN = 2,
  REQ_SIGNALLED = 4, // completion has been signalled
  REQ_DONEWAITING = 8, // got signal
  REQ_CANCELED = 16,
} femto_request_state_t;

struct femto_request {
  process_entry_t proc;
  // (we expect callback to signal callback_cond and for
  //  some other thread to possibly be waiting on callback_cond).
  pthread_cond_t callback_cond;
  query_entry_t* query;
  int free_query; // whether or not to cleanup&free the query in femto_destroy_request 
  femto_request_state_t state; // 0 none, 1 inited, 2 begun, 4 cancelled, 8 completed;
  int user_type;
  void* user_ptr;
  struct ast_node* ast_to_free; // not used in query, but will be freed
                                // when request is freed.
};

struct leaf_entry;
typedef RB_ENTRY(leaf_entry) rb_node_t;

typedef struct leaf_entry {
  query_entry_t entry;

  int64_t block_id;
  RB_ENTRY(leaf_entry) rb_node;
} leaf_entry_t;



typedef RB_HEAD(reqtree, leaf_entry) rb_root_t;

typedef LIST_HEAD(query_list_head, query_entry) query_queue_head_t;

typedef struct {
  query_queue_head_t head;
} query_queue_t;

static inline void query_queue_init(query_queue_t* q)
{
  LIST_INIT(&q->head);
}

static inline void query_queue_push(query_queue_t* q, 
                                    struct query_entry* ent)
{
  LIST_INSERT_HEAD(&q->head, ent, entries);
}

// returns 1 if any elements were stolen.
static inline int query_queue_steal(query_queue_t* queue,
    query_queue_head_t* head)
{
  memcpy(head, &queue->head, sizeof(query_queue_head_t));
  memset(&queue->head, 0, sizeof(query_queue_head_t));
  return (head->lh_first != NULL);
}

static inline void query_queue_remove(query_queue_t* queue,
                                     query_entry_t* ent)
{
  LIST_REMOVE(ent, entries);
}

static inline query_entry_t*
query_queue_pop(query_queue_t* q)
{
  query_entry_t* ret = NULL;

  ret = q->head.lh_first;
  if( ret )
  {
    LIST_REMOVE(ret, entries);
  }
  return ret;
}

static inline void query_queue_destroy(query_queue_t* q) 
{
}

typedef struct {
  leaf_entry_t leaf;
  header_occs_request_type_t type;
  header_occs_request_t r;
} header_occs_query_t;

typedef struct {
  leaf_entry_t leaf;
  header_loc_request_type_t type;
  header_loc_request_t r;
} header_loc_query_t;

typedef struct {
  leaf_entry_t leaf;
  block_request_type_t type;
  block_request_t r;
} block_query_t;


typedef struct {
  leaf_entry_t leaf;
  block_chunk_request_type_t type;
  block_chunk_request_t r;
} block_chunk_query_t;

#if SUPPORT_INDEX_MERGE
typedef struct {
  leaf_entry_t leaf;
  block_update_manage_type_t type;
  block_update_manage_t r;
} block_update_manage_query_t;

typedef struct {
  leaf_entry_t leaf;
  block_update_type_t type;
  block_update_t r;
} block_update_query_t;
#endif

typedef struct {
  process_entry_t proc;
  int plen;
  alpha_t* pat;

  // Needed during processing
  int i; // (also a result with too_few_matches)
  header_occs_query_t hq1;
  header_occs_query_t hq2;
  block_query_t bq1;
  block_query_t bq2;
  header_loc_query_t loc_query; // to read n in case patlen == 0

  // Result section
  int64_t first;
  int64_t last;
  // Save the range of rows matching pat[plen-prev_len .. plen]
  // where prev_len marks is the last position with a larger
  // number of matches than what we are returning in [first,last].
  int64_t prev_first;
  int64_t prev_last;
  int prev_len;
} string_query_t;

typedef struct {
  process_entry_t proc;
  int64_t first_in;
  int64_t last_in;
  alpha_t ch;
  // Needed during processing
  header_occs_query_t hq1;
  header_occs_query_t hq2;
  block_query_t bq1;
  block_query_t bq2;

  // Result section
  int64_t first;
  int64_t last;
} backward_search_query_t;

/* see comment in server.c - forward search is not yet implemented.
typedef struct {
  process_entry_t proc;
  int64_t first_in;
  int64_t last_in;
  alpha_t ch;
  // Needed during processing
  header_occs_query_t hq1;
  header_occs_query_t hq2;
  block_query_t bq1;
  block_query_t bq2;

  // Result section
  int64_t first;
  int64_t last;
} forward_search_query_t;
*/

typedef enum {
  LOCATE_NONE,
  LOCATE_WEAK, // get location information only if we see it
  LOCATE_STRONG // wait until we get the location information before 
                // returning
} locate_type_t;

typedef struct {
  process_entry_t proc;
  
  int64_t row; // in & out; row number
  int64_t offset; // output retrieved offset
  alpha_t chr; // output the retrieved character
  locate_type_t doLocate;  // input

  // intermediate values
  //int64_t block_number; // intermediate - block number
  block_query_t bq;
  header_occs_query_t hq;
} back_query_t;

typedef struct {
  process_entry_t proc;
  
  int64_t row; // in and out
  int64_t offset; // output offset
  alpha_t chr; // output retrieved char
  locate_type_t doLocate; // input

  // Used during processing
  int64_t occ;
  //int64_t block_number;
  block_query_t bq;
  header_occs_query_t hq;
} forward_query_t;

typedef struct {
  process_entry_t proc;
  back_query_t backward;
  forward_query_t forward;
  alpha_t* context;
  locate_type_t doLocate;
  int beforeCtxLen;
  int afterCtxLen;
  int forward_i;
  int backward_i;
  int64_t offset;
} context_query_t;

typedef struct {
  process_entry_t proc; 
  context_query_t ctx;
  header_loc_query_t hq;
  int64_t doc_num;
  int64_t doc_len;
  alpha_t* content;
} extract_document_query_t;

typedef struct {
  process_entry_t proc;
  int query_size; // (maximum) size of each of the queries
  long num_queries;
  void* queries; // the queries
  // Request info
  // Used during processing
  // 
} parallel_query_t;

static inline query_entry_t* ith_query(parallel_query_t* ctx, long i)
{
  return (query_entry_t*) ((unsigned char*) ctx->queries + i*ctx->query_size);
}

typedef struct {
  process_entry_t proc;
  int64_t doc_id;
  // on return, url is malloced
  int info_len;
  unsigned char* info;
  // If requested, on return, document length is here.
  int64_t doc_len;
  // used during processing.
  //string_query_t string_query; // to search for the right row
  //back_query_t back; // to step back to decode the URL.
  header_loc_query_t hq;
  int64_t row;
} get_doc_info_query_t;

#if SUPPORT_INDEX_MERGE
typedef struct {
  process_entry_t proc;
  // settings
  int64_t mark_period;
  header_occs_query_t dst_hq;
  header_occs_query_t src_hq;
  block_query_t dst_bq;
  block_query_t src_bq;
  block_update_query_t dst_update;
  int64_t src_doc_len;
  int64_t doc; // the destination doc number.
  int64_t dst_total_len;
  int64_t offset;
  int64_t dst_row;
  int64_t src_row;
  alpha_t ch;
} backward_merge_query_t;

typedef struct {
  process_entry_t proc;
  // request infos
  double ratio;
  int start_uncompressed;
  int max_uncompressed;
  index_locator_t src_loc;
  index_locator_t dst_loc;
  index_locator_t out_loc;
  index_block_param_t index_params;
  // used during the computation
  header_occs_query_t hoq;
  header_loc_query_t hlq;
  block_update_manage_query_t mq;
  int64_t dst_total_len;
  int64_t dst_num_docs;
  int64_t dst_num_blocks;
  int64_t src_total_len;
  int64_t src_num_docs;
  int64_t i;
  parallel_query_t backward_merges;

} index_merge_query_t;
#endif

typedef struct {
  process_entry_t proc;
  string_query_t count; // string query
  parallel_query_t locate_range; // context queries
  int max_occs;
} locate_query_t;

typedef struct {
  int64_t first;
  int64_t last;
  int cost;
  int match_len;
  alpha_t* match;
  int64_t prev_first;
  int64_t prev_last;
  int prev_len;
} regexp_result_t;

//void regexp_result_free(regexp_result_t* v);

typedef struct {
  process_entry_t proc;
  parallel_query_t queries; // locate strings...

  regexp_result_t adding_left[ALPHA_SIZE];
  regexp_result_t adding_right[ALPHA_SIZE];
} string_rows_all_t;

typedef struct {
  int num_results;
  regexp_result_t* results;
} regexp_result_list_t;

error_t regexp_result_list_append(regexp_result_list_t* list, regexp_result_t* new_result);
void regexp_result_list_sort(regexp_result_list_t* list);

typedef struct {
  header_occs_query_t hq1;
  header_occs_query_t hq2;
  block_query_t bq1;
  block_query_t bq2;
} regexp_query_part_t;

typedef struct {
  process_entry_t proc;
  // the NFA we're simulating
  nfa_description_t* nfa;
  // The total error weight to allow
  nfa_errcnt_tmp_t maxerr;
  // only allow error weights up to 254
  // so we can store #errors in a byte.
  int max_nonresults; // max number to store in nonresults.

  // the results are stored here
  regexp_result_list_t results;
  // A couple of non-results are stored here.
  //  (up to max_nonresults). Note - this array
  //  is only allocated once, not appended to.
  regexp_result_list_t nonresults;
  // temporary values needed during processing
    // allocated in setup
    queue_map_t queue;
    uint64_t *r_c; //reachable_characters
    nfa_errcnt_t* nfa_states; // error count per state.
    nfa_errcnt_t* tmp_states; // tmp space

    header_loc_query_t loc_query;

    int num_iterations;

    int64_t first;
    int64_t last;
    regexp_result_t new_range[ALPHA_SIZE];
    int num_hq1_requested;
    int num_hq1_got;
    int num_hq2_requested;
    int num_hq2_got;
    int num_bq1_requested;
    int num_bq1_got;
    int num_bq2_requested;
    int num_bq2_got;
    regexp_query_part_t parts[ALPHA_SIZE];
} regexp_query_t;

typedef struct {
  process_entry_t proc;
  int64_t chunk_size; // on request, the requested approximate size of chunks
  // The "sub-classes" will keep track of the 
  // current position in the search
  unsigned char is_last_chunk; // on return, 1 if this is the last chunk.
                               // on call, if this is set, we start over.
  int request_state; // used by the CALLER
  results_t results;
} results_query_t;

typedef struct {
  results_query_t results;
  // output - the matching documents (or offsets)
  // input - first and last range.
  int64_t first;
  int64_t last;
  // used during processing
  int64_t i; // current row number.
  header_occs_query_t hq;
  block_query_t bq;
  block_chunk_query_t bcq;
  parallel_query_t locate_range; // context queries
} range_to_results_query_t;

struct generic_boolean_query {
  results_query_t results_query;
  results_query_t* left;
  results_query_t* right;
  struct boolean_node* ast_node;
};

typedef struct generic_boolean_query generic_boolean_query_t;

typedef struct {
  results_query_t results;
  range_to_results_query_t rtrq;
  string_query_t string_query;
  int64_t num_results;
} string_results_query_t;

typedef struct {
  results_query_t results;
  range_to_results_query_t rtrq;
  regexp_query_t regexp;
  int cur;
  int64_t num_results; // the number of results in our results set.
} regexp_results_query_t;




typedef struct {
  //int mapped_block_num;
  index_locator_t loc;
  int block_size;
  int64_t block_num; // 0 is the header block.
  //index_struct_t* index;
} mapped_block_t;

typedef struct {
  int num_threads;
  int block_cache_size;
  int bucket_cache_size;
  int verbose;
} server_settings_t;

typedef struct {
  int64_t block_requests;
  int64_t block_faults;
} server_statistics_t;

// forward declare..
struct server_state;

typedef struct shared_server_state {
  server_settings_t settings; // read-only after server start

  path_translator_t path_to_id; // has its own lock

  size_t enqueued_counter; // used for round-robin enqueuing

  // read-only after server start
  int num_threads;
  struct server_state* threads;
} shared_server_state_t;

// server state for a server serving a single index.
typedef struct server_state {
  shared_server_state_t* shared_state;

  pthread_mutex_t lock; // protecting these structures
  pthread_cond_t wait_here_for_requests;

  int thread_number;
  pthread_t thread;

  int running; // 1 if we're to keep this thread running.

  // the block cache is now local to worker...
  cache_t block_cache; // block cache! here the ID numbers are
                       // "mapped block numbers" 

  // The current request.
  //
  // leaf queries
  long num_leaf_queries;
  rb_root_t leaf_queries;

  // some statistics...
  server_statistics_t stats;

  // the following structures are NOT PROTECTED BY LOCK
  // instead, they can only be modified by the thread owning
  // this queue!

  // This queue stores all new process queries in LIFO order
  query_queue_t new_process_queries;

  // This queue stores all new leaf queries in LIFO order
  query_queue_t new_leaf_queries;
  
  // This queue stores results right before they are delivered
  // we do it this way because otherwise we are holding the lock
  // and using resources in the child in the stack trace of the
  // parent, which might delete the child!
  query_queue_t ready_with_results;

} server_state_t;

error_t set_default_server_settings(server_settings_t* settings);
error_t start_server(shared_server_state_t** sst, server_settings_t* settings);
void stop_server(shared_server_state_t* s);

error_t setup_parallel_count(parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int npats, int* plen, alpha_t** pats);

void cleanup_parallel_count(parallel_query_t* ctx );
error_t setup_parallel_locate(parallel_query_t* ctx,
                              process_entry_t* requestor,
                              index_locator_t loc,
                              int npats, int* plen, alpha_t** pats, 
                              int max_occs_each);
void cleanup_parallel_locate(parallel_query_t* ctx);
error_t setup_locate_range(parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int beforeCtxLen, int afterCtxLen, int doLocate,
                             int64_t first, int64_t last);
void cleanup_locate_range(parallel_query_t* q );

error_t setup_backward_search_query(
        backward_search_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t first, int64_t last, alpha_t ch);
void cleanup_backward_search_query(backward_search_query_t* q );

// error_t resolve_offset(server_state_t* ss, index_locator_t loc, int64_t offset, location_info_t* occ);

error_t schedule_query(server_state_t* ss, query_entry_t* q);
// reqs must be an array, each element is reqsize, each must be castable to
// process_entry_t.
error_t server_schedule_queries(shared_server_state_t* shared, void* reqs, int nreqs, int reqsize);
error_t server_cancel_query(shared_server_state_t* shared, process_entry_t* q);

void setup_block_query(block_query_t* q,
                       // leaf entry params
                       index_locator_t loc,
                       int state,
                       process_entry_t* requestor,
                       // request params
                       block_request_type_t type,
                       int64_t block_number,
                       int ch, int row_in_block, int occs_in_block);
void setup_header_loc_query(
                     header_loc_query_t* q,
                     // params for the leaf entry
                     index_locator_t loc,
                     int state,
                     process_entry_t* requestor,
                     // request params
                     header_loc_request_type_t type,
                     int64_t offset, int64_t doc);
void cleanup_header_loc_query(header_loc_query_t* q);
void setup_header_occs_query(
                     header_occs_query_t* q,
                     // query entry params
                     index_locator_t loc,
                     int state,
                     process_entry_t* requestor,
                     // request params
                     header_occs_request_type_t type,
                     int64_t block_num, int ch, int64_t row, int64_t occs);
error_t setup_extract_document_query(extract_document_query_t* s,
                                process_entry_t* requestor, 
                                index_locator_t loc,
                                int64_t doc_num);
void cleanup_extract_document_query(extract_document_query_t* s);

#if SUPPORT_INDEX_MERGE
void setup_index_merge_query(index_merge_query_t* s,
                                process_entry_t* requestor, 
                                index_locator_t src_loc,
                                index_locator_t dst_loc,
                                index_locator_t out_loc,
                                index_block_param_t* index_params,
                                double ratio,
                                int start_uncompressed,
                                int max_uncompressed);
#endif

error_t setup_string_query(
        string_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int plen, 
        alpha_t* pat);
error_t setup_string_query_take(
        string_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int plen, 
        alpha_t* pat);


error_t setup_parallel_query(parallel_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int query_size, int num_queries);
error_t setup_parallel_query_take(parallel_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int query_size, int num_queries,
                             void* queries);
void cleanup_parallel_query(parallel_query_t* q);

error_t setup_string_results_query(
                             string_results_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int64_t chunk_size, result_type_t result_type,
                             int plen, alpha_t* pat);
void cleanup_string_results_query( string_results_query_t* q);

error_t create_generic_ast_count_query( process_entry_t** q_ptr,
                                        process_entry_t* requestor,
                                        index_locator_t loc,
                                        struct ast_node* ast_node);
void destroy_generic_ast_count_query( process_entry_t* q);

error_t create_generic_ast_query( results_query_t** q_ptr,
                                  process_entry_t* requestor,
				  index_locator_t loc,
                                  int64_t chunk_size,
                                  result_type_t result_type,
				  struct ast_node* ast_node);
void destroy_generic_ast_query( results_query_t* q);
void cleanup_generic_results_query( results_query_t* q);

error_t setup_range_to_results_query(
                             range_to_results_query_t* q,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             int64_t chunk_size, result_type_t result_type,
                             int64_t first, int64_t last);

void cleanup_range_to_results_query(range_to_results_query_t* q );

// take_this_nfa will have free_nfa_description and free() called in cleanup_regexp_query
error_t setup_regexp_query_take_nfa(
        regexp_query_t* q,
	process_entry_t* requestor, 
        index_locator_t loc,
	nfa_description_t* take_this_nfa,
        int max_nonresults);
error_t setup_regexp_query(
        regexp_query_t* q,
	process_entry_t* requestor, 
        index_locator_t loc,
	struct ast_node* ast_node,
        int max_nonresults);
void cleanup_regexp_query(regexp_query_t* q );

error_t setup_get_doc_info_query(
        get_doc_info_query_t* q,
        process_entry_t* requestor, 
        index_locator_t loc,
        int64_t doc_id,
        int get_doc_len);
void cleanup_get_doc_info_query(get_doc_info_query_t* q);

error_t setup_get_doc_info_results( parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             results_t* results);
error_t setup_get_doc_info_doc_len_results( parallel_query_t* ctx,
                             process_entry_t* requestor,
                             index_locator_t loc,
                             results_t* results);
void cleanup_get_doc_info_results(parallel_query_t* ctx );
error_t get_doc_info_results( parallel_query_t* ctx, 
                       int* num, long** lensOut, unsigned char*** infosOut );
error_t get_doc_info_doc_len_results( parallel_query_t* ctx, 
                       int* num, long** lensOut, unsigned char*** infosOut,
                       int64_t** doc_lensOut );

error_t setup_string_rows_all_query(
          parallel_query_t* ctx,
          process_entry_t* requestor,
          index_locator_t loc,
          int plen,
          alpha_t* pat);
error_t setup_string_rows_addleftright_query(
          parallel_query_t* ctx,
          process_entry_t* requestor,
          index_locator_t loc,
          int plen,
          alpha_t* pat,
          int adding_left);
void cleanup_string_rows_addleftright_query(parallel_query_t* ctx);

void cleanup_generic(query_entry_t* pe);
int size_generic(query_type_t type);

static inline
int leaf_entry_size( query_type_t type )
{
  switch( type ) {
    case QUERY_TYPE_BLOCK:
      return sizeof(block_query_t);
    case QUERY_TYPE_BLOCK_CHUNK:
      return sizeof(block_chunk_query_t);
    case QUERY_TYPE_HEADER_OCCS:
      return sizeof(header_occs_query_t);
    case QUERY_TYPE_HEADER_LOC:
      return sizeof(header_loc_query_t);
#if SUPPORT_INDEX_MERGE
    case QUERY_TYPE_BLOCK_UPDATE:
      return sizeof(block_update_query_t);
    case QUERY_TYPE_BLOCK_UPDATE_MANAGE:
      return sizeof(block_update_manage_query_t);
#endif
    default:
      assert(0);
      return sizeof(leaf_entry_t);
  }
}

static inline
int max_leaf_entry_size( void )
{
  int max = 0;
  int tmp;
  for( int i = QUERY_TYPE_UNDER_LEAF+1; i < QUERY_TYPE_OVER_LEAF; i++ ) {
    tmp = leaf_entry_size( (query_type_t) i );
    if( tmp > max ) max = tmp;
  }
  return max;
}

static inline
int block_query_cmp(const block_query_t* a, const block_query_t* b)
{
  if( a->r.row_in_block < b->r.row_in_block ) return -1;
  else if( a->r.row_in_block > b->r.row_in_block ) return 1;

  if( a->r.ch < b->r.ch ) return -1;
  if( a->r.ch > b->r.ch ) return 1;

  if( a->r.occs_in_block < b->r.occs_in_block ) return -1;
  if( a->r.occs_in_block > b->r.occs_in_block ) return 1;

  if( a->type < b->type ) return -1;
  if( a->type > b->type ) return -1;

  return memcmp(&a->r, &b->r, sizeof(block_request_t));
}


static inline
int compare_leaf_queries(const leaf_entry_t* a, const leaf_entry_t* b)
{
  int cmp = index_locator_cmp(a->entry.loc, b->entry.loc);

  if( cmp ) return cmp;

  if( a->block_id < b->block_id ) return -1;
  else if( a->block_id > b->block_id ) return 1;

  if( a->entry.type < b->entry.type ) return -1;
  else if( a->entry.type > b->entry.type ) return 1;

  // do additional sorting for QUERY_TYPE_BLOCK.
  if( a->entry.type == QUERY_TYPE_BLOCK ) {
    return block_query_cmp((const block_query_t*) a, (const block_query_t*) b);
  } else {
    const unsigned char* ap = (const unsigned char*) a;
    const unsigned char* bp = (const unsigned char*) b;
    ap += sizeof(leaf_entry_t);
    bp += sizeof(leaf_entry_t);
    return memcmp(ap, bp, leaf_entry_size(a->entry.type) - sizeof(leaf_entry_t));
  }
}

static inline
void clear_leaf_response(leaf_entry_t* dst)
{
  if( dst->entry.type == QUERY_TYPE_BLOCK_CHUNK ) {
    block_chunk_query_t* chunk = (block_chunk_query_t*) dst;
    results_destroy(&chunk->r.results);
  } else if( dst->entry.type == QUERY_TYPE_HEADER_LOC ) {
    header_loc_query_t* hq = (header_loc_query_t*) dst;
    if( hq->r.doc_info ) free(hq->r.doc_info);
  }
}

// copies only the query part of the leaf entry
// (not the state or the callback ptr. or the parent)
static inline
void copy_leaf_query(leaf_entry_t* dst, const leaf_entry_t* src)
{
  unsigned char* dstp = (unsigned char*) dst;
  const unsigned char* srcp = (const unsigned char*) src;
 
  dst->block_id = src->block_id;

  dstp += sizeof(leaf_entry_t);
  srcp += sizeof(leaf_entry_t);
  memcpy(dstp, srcp, leaf_entry_size(src->entry.type) - sizeof(leaf_entry_t));
}

static inline
error_t copy_leaf_response(leaf_entry_t* dst, const leaf_entry_t* src)
{
  copy_leaf_query(dst, src);

  if( dst->entry.type == QUERY_TYPE_BLOCK_CHUNK ) {
    // need to copy the results too instead of duplicating pointers.
    block_chunk_query_t* dst_chunk = (block_chunk_query_t*) dst;
    block_chunk_query_t* src_chunk = (block_chunk_query_t*) src;
    if( src_chunk->r.results.data ) {
      return results_copy(&dst_chunk->r.results, &src_chunk->r.results);
    }
  } else if( dst->entry.type == QUERY_TYPE_HEADER_LOC ) {
    header_loc_query_t* dst_hq = (header_loc_query_t*) dst;
    header_loc_query_t* src_hq = (header_loc_query_t*) src;
    if( src_hq->r.doc_info ) {
      dst_hq->r.doc_info = (unsigned char*) malloc(src_hq->r.doc_info_len);
      if( ! dst_hq->r.doc_info ) return ERR_MEM;
      memcpy(dst_hq->r.doc_info, src_hq->r.doc_info, src_hq->r.doc_info_len);
    }
  }
  return ERR_NOERR;
}

static inline
int sort_leaf_entries_by_row_ascending(const leaf_entry_t* a, const leaf_entry_t* b)
{
  int cmp = compare_leaf_queries(a, b);
  if( cmp ) return cmp;

  if( (intptr_t) a < (intptr_t) b ) return -1;
  else if( (intptr_t) a > (intptr_t) b ) return 1;
  else return 0;
}



RB_GENERATE_STATIC(reqtree, leaf_entry, rb_node, sort_leaf_entries_by_row_ascending);

static inline void rb_insert_leaf_entry(server_state_t* ss,
                                        leaf_entry_t* insert)
{
  leaf_entry_t* found;
  //printf("Inserting bq %p node %p with key %i %i %i %i\n", bq, &bq->rb_node, bq->entry.mapped_block_num, bq->r.row_in_block, bq->entry.type, bq->r.ch);
  found = RB_INSERT(reqtree, &ss->leaf_queries, insert); 
  if(EXTRA_CHECKS) {
    assert(found == NULL); // we should have no duplicates.
  }
}

static inline void rb_delete_leaf_entry(server_state_t* ss,
                                       leaf_entry_t* bq)
{
  //printf("Deleting bq %p node %p with key %i %i %i %i\n", bq, &bq->rb_node, bq->entry.mapped_block_num, bq->r.row_in_block, bq->entry.type, bq->r.ch);
  RB_REMOVE(reqtree, &ss->leaf_queries, bq);

  if(EXTRA_CHECKS) {
    //printf("Doing extra check: ");
    leaf_entry_t* n;
    RB_FOREACH(n, reqtree, &ss->leaf_queries) {
      //leaf_entry_t* query = rb_leaf_entry(n);
      //printf("%p ", query);
      if( n == bq ) {
        printf("ERROR - node not erased!\n");
        assert(0);
      }
    }
    //printf("\n");
  }
  memset(&bq->rb_node, 0, sizeof(rb_node_t));
}

static inline leaf_entry_t* rb_search_leaf_entry(server_state_t* ss,
                                                 leaf_entry_t* query)
{
  return RB_NFIND(reqtree, &ss->leaf_queries, query);
}


static inline leaf_entry_t* rb_next_leaf_entry(server_state_t* ss,
                                               leaf_entry_t* node)
{
  return RB_NEXT(reqtree, &ss->leaf_queries, node);
}

static inline leaf_entry_t* rb_min_leaf_entry(server_state_t* ss)
{
  return RB_MIN(reqtree, &ss->leaf_queries);
}

static inline int rb_not_empty(server_state_t* ss)
{
  return ! RB_EMPTY(&ss->leaf_queries);
}

static inline int rb_in_a_tree(leaf_entry_t* node)
{
  return RB_PARENT(node, rb_node) != 0;
}

#endif
