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

  femto/src/main/sequential_search.h
*/
#ifndef _SEQUENTIAL_SEARCH_H_
#define _SEQUENTIAL_SEARCH_H 1

#include "results.h"
#include "nfa.h"
#include "ast.h"
#include "hashmap.h"

extern error_t seq_compile_regexp_from_ast(void** ret, struct ast_node* ast_node);

// returns NULL if there was an error.
// Runs the matcher, returns the start and end of the match.
// returns 0 for no match, 1 for a match;
// if a match is returned, the start and end offsets within
// the pattern are returned through the pointer arguments.
// If there was a match, we can continue at subject+end_offset
extern int seq_match_regexp(void* regexp,
                 const unsigned char* subject, size_t subject_length,
                 /* OUT -- a match */ size_t* match_offset, size_t* match_len);

void seq_free_regexp(void* regexp);

typedef enum {
  SEQ_REGEXP,
  SEQ_BOOLEAN
} sequential_query_type;

typedef struct sequential_query_s {
  sequential_query_type type;
  results_t results;
  int64_t count;
  struct ast_node* ast_node;
} sequential_query_t;

typedef struct sequential_regexp_query_s {
  sequential_query_t query;
  //nfa_description_t nfa;
  void* matcher;
  hashmap_t matches;

  results_writer_t results_writer;
} sequential_regexp_query_t;

typedef struct sequential_boolean_query_s {
  sequential_query_t query;
  sequential_query_t* left;
  sequential_query_t* right;
} sequential_boolean_query_t;

typedef struct seq_search_match_key_s {
  const unsigned char* data;
  size_t len;
} seq_search_match_key_t;

typedef struct seq_search_match_value_s {
  int64_t num_matches;
} seq_search_match_value_t;

typedef struct seq_search_match_keyvalue_s {
  seq_search_match_key_t* key;
  seq_search_match_value_t* value;
} seq_search_match_keyvalue_t;



// queries that can be run on multiple input files.
typedef struct sequential_search_state_s {
  int n_queries;
  struct ast_node** asts;
  sequential_query_t** queries;
  int n_regexps;
  sequential_regexp_query_t** regexps;

  //nfa_errcnt_t** nfa_states; // error count per state
  //nfa_errcnt_t** tmp_states; // tmp space

  //uint64_t** nfa_states;
  //uint64_t** tmp_states;
  //hashmap_t* matches_hashes; // one hashmap for each regexp
  char *docmatched; // 1 if we've already matched this document..
  int keepmatches;

  result_type_t type;
} sequential_search_state_t;

error_t init_sequential_search( sequential_search_state_t* state, int num_queries, const char** queries, int icase, result_type_t type /* 0 for count */, int wantmatches);
error_t run_sequential_search(sequential_search_state_t* state, int64_t doc_len, const unsigned char* doc_contents, uint64_t doc_num);
// results must be an array of size num_queries and it must be
// initialized to zero.... same is true for counts.
// caller must free matches[i] if there is anything in there
// (but the pointers within that point to data freed in destroy_seq_search)
// finish takes the results, run_sequential search can be run again
// using the same NFA and AST.
error_t finish_sequential_search(sequential_search_state_t* state, results_t* results, int64_t* counts, seq_search_match_keyvalue_t** matches, size_t* nmatches); 

void destroy_sequential_search(sequential_search_state_t* state);

error_t create_sequential_query( sequential_query_t** q, struct ast_node* ast_node, result_type_t type /* 0 for count */);

// For searching data sequentially with an NFA.
//error_t nfa_find_matches (sequential_search_state_t* state, 
//                          int64_t doc_len, const unsigned char* doc_contents, uint64_t doc_num);

#endif
