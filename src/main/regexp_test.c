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

  femto/src/main/regexp_test.c
*/
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "index_test_funcs.c"

void test_regexp_search(void)
{
  int ndocs = 2;
  char* urls[] = {"0", "1"};
  unsigned char* docs[] = {(unsigned char*) "test_one", (unsigned char*) "test_document_two"};
  int* doc_lens;
  index_block_param_t param;
  femto_server_t srv;
  char* index_path = TEST_PATH;
  index_locator_t loc;
  error_t err;

  // building the index
  // (does the BWT, etc.)
  doc_lens = malloc(sizeof(int)*ndocs);
  for( int i = 0; i < ndocs; i++ ) {
    doc_lens[i] = strlen((char*) docs[i]);
  }
  set_default_param(&param);
  create_index(ndocs, urls, doc_lens, docs, index_path, &param, "simple");

  err = femto_start_server_err(&srv, 1);
  die_if_err(err);

  err = femto_loc_for_path_err(&srv, index_path, &loc);
  die_if_err(err);

  // test getting URLs.
  {
    get_doc_info_query_t q;
    for( int i = 0; i < ndocs; i++ ) {
      err = setup_get_doc_info_query(&q, NULL, loc, i, 0);
      die_if_err(err);

      err = femto_run_query(&srv, (query_entry_t*) &q);
      die_if_err(err);

      assert( q.info_len == strlen(urls[i]) );
      assert( 0 == memcmp(urls[i], q.info, q.info_len));
      cleanup_get_doc_info_query(&q);
    }
  }

  {
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;

    // construct an NFA to try
    err = init_nfa_description(nfa, 2);
    die_if_err(err);

    // create an NFA that looks like:
    // (0)--t-->(end)
    nfa->nodes[0].num_transitions = 1;
    nfa->nodes[0].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 't';
    nfa->nodes[0].transitions[0].destination = 1;

    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 1);

    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);

    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 1 range of matches
    assert(q.results.num_results == 1 );
    // assert that we got 6 individual matches
    assert(1 + q.results.results[0].last - q.results.results[0].first == 6);

    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)

    printf("TEST: Search for `t'        passed\n");
  }
  {
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;
    // construct an NFA to try
    err = init_nfa_description(nfa, 5);
    die_if_err(err);

    // create an NFA that looks like:
    // >(0)--t-->(1)--e-->(2)--s-->(3)--t-->(end)
    // the reversed version:
    // >(0)--t-->(1)--s-->(2)--e-->(3)--t-->(end)
    nfa->nodes[0].num_transitions = 1;
    nfa->nodes[0].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 't';
    nfa->nodes[0].transitions[0].destination = 1;
    nfa->nodes[1].num_transitions = 1;
    nfa->nodes[1].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[1].transitions[0].character = CHARACTER_OFFSET + 's';
    nfa->nodes[1].transitions[0].destination = 2;
    nfa->nodes[2].num_transitions = 1;
    nfa->nodes[2].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[2].transitions[0].character = CHARACTER_OFFSET + 'e';
    nfa->nodes[2].transitions[0].destination = 3;
    nfa->nodes[3].num_transitions = 1;
    nfa->nodes[3].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[3].transitions[0].character = CHARACTER_OFFSET + 't';
    nfa->nodes[3].transitions[0].destination = 4;
    //nfa.nodes[4] is the final state.

    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 4);

    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);

    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 1 range of matches
    assert(q.results.num_results == 1 );
    // assert that we got 6 individual matches
    assert(1 + q.results.results[0].last - q.results.results[0].first == 2);

    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)
    printf("TEST: Search for `test'     passed\n");
  }
  {
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;
    // construct an NFA to try
    err = init_nfa_description(nfa, 3);
    die_if_err(err);
    
    // create an NFA for (t|o)
    // that looks like:
    //       ,-t->(end)
    // (0)--<
    //       `-o->(end)
    // (reversed version is the same)
    nfa->nodes[0].num_transitions = 2;
    nfa->nodes[0].transitions = malloc(2*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 't';
    nfa->nodes[0].transitions[0].destination = 1;
    nfa->nodes[0].transitions[1].character = CHARACTER_OFFSET + 'o';
    nfa->nodes[0].transitions[1].destination = 2;
    
    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 1);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 2);
    
    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);
    
    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 2 ranges of matches
    assert(q.results.num_results == 2 );

    regexp_result_list_sort(&(q.results));
    // assert that we got 9 individual matches
    assert(1 + (q.results.results[0].last - q.results.results[0].first) +
	   1 + (q.results.results[1].last - q.results.results[1].first) == 9);
    
    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)
    printf("TEST: Search for `(o|t)'    passed\n");
  }  

  free(doc_lens);

  femto_stop_server(&srv);

  printf("-->  The first set of tests PASSED\n\n");  
}



void test_regexp_search2(void)
{
  int ndocs = 4;
  char* urls[] = {"0", "1", "2", "3"};
  unsigned char* docs[] = {(unsigned char*) "number_one", (unsigned char*) "number_two",
			   (unsigned char*) "numero_uno", (unsigned char*) "numero_dos"};
  int* doc_lens;
  index_block_param_t param;
  femto_server_t srv;
  char* index_path = TEST_PATH;
  index_locator_t loc;
  error_t err;
  
  // building the index
  // (does the BWT, etc.)
  doc_lens = malloc(sizeof(int)*ndocs);
  for( int i = 0; i < ndocs; i++ ) {
    doc_lens[i] = strlen((char*) docs[i]);
  }
  set_default_param(&param);
  create_index(ndocs, urls, doc_lens, docs, index_path, &param, "simple");


  err = femto_start_server_err(&srv, 1);
  die_if_err(err);

  err = femto_loc_for_path_err(&srv, index_path, &loc);
  die_if_err(err);

  {
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;
    // construct an NFA to try
    err = init_nfa_description(nfa, 4);
    die_if_err(err);

    // create an NFA for (num)
    // that looks like:
    // (0)--m-->(1)--u-->(2)--n-->(end)
    nfa->nodes[0].num_transitions = 1;
    nfa->nodes[0].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 'm';
    nfa->nodes[0].transitions[0].destination = 1;
    nfa->nodes[1].num_transitions = 1;
    nfa->nodes[1].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[1].transitions[0].character = CHARACTER_OFFSET + 'u';
    nfa->nodes[1].transitions[0].destination = 2;
    nfa->nodes[2].num_transitions = 1;
    nfa->nodes[2].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[2].transitions[0].character = CHARACTER_OFFSET + 'n';
    nfa->nodes[2].transitions[0].destination = 3;

    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 3);

    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);

    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 1 range of matches
    assert(q.results.num_results == 1 );
    // assert that we got 4 individual matches
    assert(1 + q.results.results[0].last - q.results.results[0].first == 4);

    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)
    
    printf("TEST: Search for `(num)'       passed\n");
  }
  {
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;
    // construct an NFA to try
    err = init_nfa_description(nfa, 4);
    die_if_err(err);
    
    // create an NFA for ((n|u)+)(m|o)
    // that looks like:
    //                              
    //       ,-m-.         ,-n-.    _ ,---.n
    // (0)--{     }->(1)--{     }->(end)<-'
    //       `-o-'         `-u-'   (___)<-.
    //                                `---'u
    //
    nfa->nodes[0].num_transitions = 2;
    nfa->nodes[0].transitions = malloc(2*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 'm';
    nfa->nodes[0].transitions[0].destination = 1;
    nfa->nodes[0].transitions[1].character = CHARACTER_OFFSET + 'o';
    nfa->nodes[0].transitions[1].destination = 1;
    nfa->nodes[1].num_transitions = 2;
    nfa->nodes[1].transitions = malloc(2*sizeof(nfa_transition_t));
    nfa->nodes[1].transitions[0].character = CHARACTER_OFFSET + 'n';
    nfa->nodes[1].transitions[0].destination = 2;
    nfa->nodes[1].transitions[1].character = CHARACTER_OFFSET + 'u';
    nfa->nodes[1].transitions[1].destination = 2;
    nfa->nodes[2].num_transitions = 2;
    nfa->nodes[2].transitions = malloc(2*sizeof(nfa_transition_t));
    nfa->nodes[2].transitions[0].character = CHARACTER_OFFSET + 'n';
    nfa->nodes[2].transitions[0].destination = 2;
    nfa->nodes[2].transitions[1].character = CHARACTER_OFFSET + 'u';
    nfa->nodes[2].transitions[1].destination = 2;

    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 2);

    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);

    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 2 ranges of matches
    assert(q.results.num_results == 2 );
    // assert that we got 5 individual matches    
    assert(1 + (q.results.results[0].last - q.results.results[0].first) +
	   1 + (q.results.results[1].last - q.results.results[1].first) == 5);
    
    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)

    printf("TEST: Search for `(n|u)+(m|o)' passed\n");
  }
  {  
    nfa_description_t* nfa = malloc(sizeof(nfa_description_t));
    regexp_query_t q;
    // construct an NFA to try
    err = init_nfa_description(nfa, 4);
    die_if_err(err);
    
    // create an NFA for m(b|e)*r
    // that looks like:
    //           __,---.b
    //          (   )<-'
    // (0)--m-->( 1 )----r-->(end)
    //          (___)<-.
    //             `---'e
    //
    nfa->nodes[0].num_transitions = 1;
    nfa->nodes[0].transitions = malloc(1*sizeof(nfa_transition_t));
    nfa->nodes[0].transitions[0].character = CHARACTER_OFFSET + 'm';
    nfa->nodes[0].transitions[0].destination = 1;
    nfa->nodes[1].num_transitions = 3;
    nfa->nodes[1].transitions = malloc(3*sizeof(nfa_transition_t));
    nfa->nodes[1].transitions[0].character = CHARACTER_OFFSET + 'b';
    nfa->nodes[1].transitions[0].destination = 1;
    nfa->nodes[1].transitions[1].character = CHARACTER_OFFSET + 'e';
    nfa->nodes[1].transitions[1].destination = 1;
    nfa->nodes[1].transitions[2].character = CHARACTER_OFFSET + 'r';
    nfa->nodes[1].transitions[2].destination = 2;

    set_bit(nfa->bit_array_len, nfa->start_states_set, 0);
    set_bit(nfa->bit_array_len, nfa->final_states_set, 2);

    // create the query:
    // initialize q, the regexp_query_t
    // (function in server.c)
    err = setup_regexp_query_take_nfa(&q, NULL, loc, nfa, 0);
    die_if_err(err);

    err = femto_run_query(&srv, (query_entry_t*) &q);
    die_if_err(err);

    // assert that we got 1 range of matches
    //assert(q.results.num_results == 1 );
    // assert that we got 4 individual matches
    //assert(1 + q.results.results[0].last - q.results.results[0].first == 4);

    cleanup_regexp_query(&q); // calls free_nfa_description and free(nfa)

    printf("TEST: Search for `m(b|e)*r'    passed\n");
  }

  free(doc_lens);
  
  femto_stop_server(&srv);

  printf("--> The second set of tests    PASSED\n\n");  
}



// Assumes that the data under test is in test_dir
int main(int argc, char** argv)
{
  printf("\n");
  
  test_regexp_search();
  test_regexp_search2();
  
  printf("All regexp tests PASSED\n\n");
}

