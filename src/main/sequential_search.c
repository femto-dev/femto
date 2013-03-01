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

  femto/src/main/sequential_search.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include "error.h"
#include "index.h"
#include "nfa.h"
#include "nfa_approx_tool.h"
#include "compile_regexp.h"
#include "query_planning.h"
#include "sequential_search.h"

hash_t hash_seq_search_match(const void* match)
{
  const seq_search_match_key_t* m = (const seq_search_match_key_t*) match;
  return hash_data(m->data, m->len);
}

void free_seq_search_entry(hm_entry_t* entry)
{
  seq_search_match_key_t* k = (seq_search_match_key_t*) entry->key;
  seq_search_match_value_t* v = (seq_search_match_value_t*) entry->value;
  if( k ) {
    free((void*) k->data);
    free(k);
  }
  if( v ) {
    free(v);
  }
  entry->key = NULL;
  entry->value = NULL;
}

int cmp_seq_search_match(const void* a_in, const void* b_in)
{
  const seq_search_match_key_t* a = (const seq_search_match_key_t*) a_in;
  const seq_search_match_key_t* b = (const seq_search_match_key_t*) b_in;
  size_t min_len = (a->len > b->len)?(a->len):(b->len);
  int ret;

  ret = memcmp(a->data, b->data, min_len);
  if( ret == 0 ) {
    if( a->len < b->len ) return -1;
    if( a->len == b->len ) return 0;
    else return 1;
  }
  return ret;
}


// Code to handle sequential queries.
error_t create_sequential_query( sequential_query_t** q, struct ast_node* ast_node, result_type_t type) 
{
  error_t err = ERR_NOERR;

  switch( ast_node->type ) {
    case AST_NODE_BOOL:
      {
        struct boolean_node* boolean_node = (struct boolean_node*) ast_node;
        sequential_boolean_query_t* ret;
        ret = calloc(1, sizeof(sequential_boolean_query_t));
        if( ! ret ) return ERR_MEM;
        ret->query.type = SEQ_BOOLEAN;
        ret->query.ast_node = ast_node;
        if( err ) return err;
        err = create_sequential_query(&ret->left, boolean_node->left, type);
        if( err ) return err;
        err = create_sequential_query(&ret->right, boolean_node->right, type);
        if( err ) return err;
        *q = &ret->query;
      }
      break;
    case AST_NODE_REGEXP:
    case AST_NODE_STRING:
      {
        sequential_regexp_query_t* ret;
        ret = calloc(1, sizeof(sequential_regexp_query_t));
        if( ! ret ) return ERR_MEM;
        ret->query.type = SEQ_REGEXP;
        ret->query.ast_node = ast_node;
        //err = results_writer_create(&ret->results_writer, RESULT_TYPE_DOC_OFFSETS | RESULT_TYPE_REV_OFFSETS );
        // reverse the AST
        //reverse_regexp(ast_node);

        /*
        err = compile_regexp_from_ast(&ret->nfa, ast_node);
        if( err ) return err;
        */
        err = seq_compile_regexp_from_ast(& ret->matcher, ast_node);
        if( err ) return err;

        // create the hashmap.
        err = hashmap_create(& ret->matches, 128, hash_seq_search_match, cmp_seq_search_match);
        if( err ) return err;
        
        *q = &ret->query;
      }
      break;
    default:
      err = ERR_INVALID_STR("Unknown AST node");
  }

  return err;
}

error_t gather_regexps(sequential_query_t* q, int* num_regexps, sequential_regexp_query_t*** regexps) 
{
  error_t err;

  switch( q->type ) {
    case SEQ_BOOLEAN:
      {
        sequential_boolean_query_t* b = (sequential_boolean_query_t*) q;
        err = gather_regexps(b->left, num_regexps, regexps);
        if( err ) return err;
        err = gather_regexps(b->right, num_regexps, regexps);
        if( err ) return err;
      }
      break;
    case SEQ_REGEXP:
      {
        sequential_regexp_query_t* b = (sequential_regexp_query_t*) q;
        err = append_array(num_regexps, regexps, sizeof(sequential_regexp_query_t*), &b);
        if( err ) return err;
      }
      break;
  }

  return ERR_NOERR;
}

void free_sequential_query(sequential_query_t* q)
{
  switch( q->type ) {
    case SEQ_BOOLEAN:
      {
        sequential_boolean_query_t* b = (sequential_boolean_query_t*) q;
        free_sequential_query(b->left);
        free_sequential_query(b->right);
        free(b);
      }
      break;
    case SEQ_REGEXP:
      {
        sequential_regexp_query_t* b = (sequential_regexp_query_t*) q;
        results_writer_destroy( & b->results_writer );
        //free_nfa_description( & b->nfa );
        seq_free_regexp(b->matcher);

        if( b->matches.num > 0 ) {
          // pull out all of the matches and free the keys and values.
          for( hash_t i = 0; i < b->matches.size; i++ ) {
            free_seq_search_entry( & b->matches.map[i] );
          }
        }
        hashmap_destroy( & b->matches );

        free(b);
      }
      break;
  }
}

error_t init_sequential_search( sequential_search_state_t* state,
                                int npatterns,
                                const char** patterns,
                                int icase, result_type_t type, int wantmatches)
{
  int j;
  error_t err;

  state->type = type;
  state->n_queries = npatterns;

  state->n_regexps = 0;
  state->regexps = NULL;

  state->asts = malloc(npatterns*sizeof(struct ast_node*));
  if( ! state->asts ) return ERR_MEM;
  state->queries = malloc(npatterns*sizeof(sequential_query_t*));
  if( ! state->queries ) return ERR_MEM;
  for( j = 0; j < npatterns; j++ ) {
    state->asts[j] = parse_string(strlen(patterns[j]), patterns[j]);
    if( ! state->asts[j] ) {
      return ERR_INVALID_STR("Could not parse pattern");
    }

    //Remove useless extra tokens from the Regular Expression
    streamline_query(state->asts[j]);
    
    if( icase ) {
      icase_ast(&state->asts[j]);
    }

    // Create the query nodes.
    err = create_sequential_query( &state->queries[j], state->asts[j], type);
    if( err ) return err;

    err = gather_regexps( state->queries[j], &state->n_regexps, &state->regexps);
    if( err ) return err;
  }

  state->docmatched = malloc(state->n_regexps);
  state->keepmatches = wantmatches;

  return ERR_NOERR;
}


error_t run_sequential_search( sequential_search_state_t* state, 
                    int64_t doc_len,
                    const unsigned char* doc_contents,
                    uint64_t doc_num )
{
  error_t err;
  int got;
  int j;
  size_t chunk = 1*1024*1024; // 1 MB chunk.
  size_t overlap = 1024; // handle extra length of up to 1024.
  result_type_t type = state->type;
  int keepmatches = state->keepmatches;

  memset(state->docmatched, 0, state->n_regexps);

  // start results writers for all of our regexps.
  for( j = 0; j < state->n_regexps; j++ ) {
    if( type != 0 ) {
      err = results_writer_create(&state->regexps[j]->results_writer, type);
      if( err ) return err;
    }
  }

  for( int64_t i = 0; i < doc_len; i += chunk ) {
    // scan chunk+overlap bytes with each regular expression.

    // Run all of the regexps on the input.
    for( j = 0; j < state->n_regexps; j++ ) {
      sequential_regexp_query_t* s = state->regexps[j];
      size_t cur = 0;
      size_t end = chunk + overlap;
      size_t end_no_overlap = chunk;
      size_t match_start, match_len;
      if( i + end > doc_len ) end = doc_len - i;
      if( i + end_no_overlap > doc_len ) end_no_overlap = doc_len - i;

      if( type == RESULT_TYPE_DOCUMENTS && state->docmatched[j] ) {
        break; // no matching necessary.
      }

      while( cur < end ) {
        const unsigned char* base = doc_contents + i + cur;
        size_t len = end - cur;
        size_t len_no_overlap = end_no_overlap - cur;

        got = seq_match_regexp(s->matcher,
                           base,
                           len,
                           &match_start, &match_len);
        if( got && match_start < len_no_overlap ) {
          // report the result.
          s->query.count++;
          
          if( type == RESULT_TYPE_DOCUMENTS ) {
            
            state->docmatched[j] = 1;
            
            err = results_writer_append(&s->results_writer, doc_num, 0);
            if( err ) return err;
            // once the 1st match is found for a document, we're done!
            return ERR_NOERR;
          } else if( type == RESULT_TYPE_DOC_OFFSETS ) {
            err = results_writer_append(&s->results_writer, doc_num, i + cur + match_start);
            if( err ) return err;
          }
          
          // if we're doing the matches, add to the matches.
          if( keepmatches ) {
            // look for the match in our hashmap.
            seq_search_match_key_t search_key;
            hm_entry_t entry;

            search_key.data = base + match_start;
            search_key.len = match_len;
            entry.key = &search_key;
            entry.value = NULL;

            if( hashmap_retrieve(& s->matches, &entry) ) {
              seq_search_match_value_t* v = (seq_search_match_value_t*) entry.value;
              v->num_matches++;
            } else {
              seq_search_match_key_t* key = malloc(sizeof(seq_search_match_key_t));
              seq_search_match_value_t* v = malloc(sizeof(seq_search_match_value_t));
              unsigned char* data = malloc(search_key.len);
              
              if( ! key ) return ERR_MEM;
              if( ! v ) return ERR_MEM;
              if( ! data ) return ERR_MEM;

              v->num_matches = 1;
              memcpy(data, search_key.data, search_key.len);

              key->data = data;
              key->len = search_key.len;


              // make room for more entries..
              err = hashmap_resize(& s->matches);
              if( err ) return err;

              entry.key = key;
              entry.value = v;
              err = hashmap_insert(& s->matches, &entry);
              if( err ) return err;
            }
          }

          // advance by one.
          cur += match_start + 1;
        } else {
          cur = end;
          // no more matches for this pattern.
          break; 
        } 
      }
    }
  }

  // Boolean query processing will happen in finish query.
  return ERR_NOERR;
}

error_t process_results( sequential_query_t* q )
{
  error_t err;

  switch( q->type ) {
    case SEQ_BOOLEAN:
      {
        sequential_boolean_query_t* b = (sequential_boolean_query_t*) q;
        struct boolean_node* ast_node = (struct boolean_node*) q->ast_node;
        err = process_results(b->left);
        if( err ) return err;
        err = process_results(b->right);
        if( err ) return err;

        // now apply the appropriate Boolean operation.
        switch( ast_node->nodeType ) {
          case BOOL_AND:
            err = intersectResults(&b->left->results,
                                   &b->right->results, 
                                   &b->query.results);
            break;
          case BOOL_OR:
            err = unionResults(&b->left->results,
                               &b->right->results, 
                               &b->query.results);
            break;
          case BOOL_NOT:
            err = subtractResults(&b->left->results,
                                  &b->right->results, 
                                  &b->query.results);
            break;
          case BOOL_THEN:
            err = thenResults(&b->left->results,
                              &b->right->results, 
                              &b->query.results, 
                              ast_node->distance);
            break;
          case BOOL_WITHIN:
            err = withinResults(&b->left->results,
                                &b->right->results, 
                                &b->query.results, 
                                ast_node->distance);
            break;
          default:
            err = ERR_INVALID_STR("Unknown AST node");
        }

        if( err ) return err;

        // free up results from the tree...
        results_destroy(&b->left->results);
        results_destroy(&b->right->results);
      }
      break;
    case SEQ_REGEXP:
      // these already have results ready for us...
      // Don't need to do anything.
      break;
  }

  return ERR_NOERR;
}

error_t finish_sequential_search( sequential_search_state_t* state, results_t* results, int64_t* counts, seq_search_match_keyvalue_t** matches, size_t* nmatches)
{
  int j;
  error_t err;

  // Finish the results writers for all of the regular expression queries.
  // next, merge the results from the different queries...
  // finish the results writers and save the results.
  for( j = 0; j < state->n_regexps; j++ ) {
    err = results_writer_finish(&state->regexps[j]->results_writer,
                                &state->regexps[j]->query.results);
    if( err ) return err;
  }

  // Now go through the query tree moving the results up it...
  for( j = 0; j < state->n_queries; j++ ) {
    err = process_results(state->queries[j]);
    // move the results to the output.
    results_move(&results[j], &state->queries[j]->results);
    // move the counts.
    counts[j] = state->queries[j]->count;
    // move the matches, if it's a regular expression query.
    if( state->queries[j]->type == SEQ_REGEXP ) {
      sequential_regexp_query_t* b = (sequential_regexp_query_t*) state->queries[j];
      if( b->matches.num > 0 ) {
        size_t cur = 0;
        matches[j] = malloc(sizeof(seq_search_match_keyvalue_t) * b->matches.num);
        if( ! matches[j] ) return ERR_MEM;

        nmatches[j] = b->matches.num;
        
        // pull out all of the matches and free the keys and values.
        for( hash_t i = 0; i < b->matches.size; i++ ) {
          if( b->matches.map[i].key ) {
            matches[j][cur].key = b->matches.map[i].key;
            matches[j][cur].value = b->matches.map[i].value;
            cur++;
          }
        }
      }
    }
  }

  return ERR_NOERR;
}

void destroy_sequential_search( sequential_search_state_t* state )
{
  int j;

  free(state->docmatched);

  for( j = 0; j < state->n_queries; j++ ) {
    free_sequential_query(state->queries[j]);
    free_ast_node(state->asts[j]);
  }
  free(state->asts);
  free(state->queries);
}

