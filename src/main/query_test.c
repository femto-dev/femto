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

  femto/src/main/query_test.c
*/
#include "index_test_funcs.c"

                   
void test_boolean_search(void)
{
  int ndocs = 3;
  char* urls[] = {"0", "1", "2"};
  unsigned char* docs[] = {(unsigned char*) "this is test one", 
                           (unsigned char*) "test document two", 
                           (unsigned char*) "test, this is a document named number three"};
  int* doc_lens;
  index_block_param_t param;
  femto_server_t srv;
  char* index_path = TEST_PATH;
  index_locator_t loc;
  error_t err;

  doc_lens = malloc(sizeof(int)*ndocs);
  for( int i = 0; i < ndocs; i++ ) {
    doc_lens[i] = strlen((char*) docs[i]);
  }
  set_default_param(&param);
  // create an example index
  create_index(ndocs, urls, doc_lens, docs, index_path, &param, "simple");
  
  err = femto_start_server_err(&srv, 1);
  die_if_err(err);

  err = femto_loc_for_path_err(&srv, index_path, &loc);
  die_if_err(err);

  {
    char* text = "one";
    int num_expect = 1;
    location_info_t expect[] = {{0,0}};
    string_t string;
    struct string_node* string_node;
    results_query_t* q = NULL;
 
    string = construct_c_string(text);    

    string_node = string_node_new(string);

    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) string_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) string_node);

    destroy_generic_ast_query(q);

    printf("Passed Test 1\n");
  }

  {
    char* left = "test";
    char* right = "one";
    int num_expect = 1;
    location_info_t expect[] = {{0,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
     
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    

    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);

    b_node = boolean_node_new( BOOL_AND, 0 );
    boolean_node_set_children( b_node,
                              (struct ast_node*) left_node,
                              (struct ast_node*) right_node);

    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);
    printf("Passed Test 2\n");
  }
  {
    char* left = "is";
    char* right = "document";
    int num_expect = 1;
    location_info_t expect[] = {{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_AND, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 3\n");
  }
  {
    char* sub_left = "test";
    char* sub_right = "document";
    char* right = "named";
    int num_expect = 1;
    location_info_t expect[] = {{2,0}};
    
    string_t sub_left_string;
    string_t sub_right_string;
    string_t right_string;
    
    struct string_node* sub_left_node;
    struct string_node* sub_right_node;
    struct string_node* right_node;
    struct boolean_node* left;
    struct boolean_node* b_node;
    
    results_query_t* q = NULL;
    
    sub_left_string = construct_c_string(sub_left);    
    sub_right_string = construct_c_string(sub_right);    
    right_string = construct_c_string(right);    
    
    sub_left_node = string_node_new(sub_left_string);
    sub_right_node = string_node_new(sub_right_string);
    right_node = string_node_new(right_string);
    
    
    left = boolean_node_new( BOOL_AND, 0 );
    boolean_node_set_children( left, 
                               (struct ast_node*) sub_left_node,
                               (struct ast_node*) sub_right_node );
                               
    b_node = boolean_node_new( BOOL_AND, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 4\n");
  }
  {
    char* left = "one";
    char* right = "two";
    int num_expect = 2;
    location_info_t expect[] = {{0,0},{1,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_OR, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 5\n");
  }
  {
    char* sub_left = "one";
    char* sub_right = "two";
    char* right = "three";
    int num_expect = 0;
    location_info_t expect[] = {};
    
    string_t sub_left_string;
    string_t sub_right_string;
    string_t right_string;
    
    struct string_node* sub_left_node;
    struct string_node* sub_right_node;
    struct string_node* right_node;
    struct boolean_node* left;
    struct boolean_node* b_node;
    
    results_query_t* q = NULL;
    
    sub_left_string = construct_c_string(sub_left);    
    sub_right_string = construct_c_string(sub_right);    
    right_string = construct_c_string(right);    
    
    sub_left_node = string_node_new(sub_left_string);
    sub_right_node = string_node_new(sub_right_string);
    right_node = string_node_new(right_string);
    
    
    left = boolean_node_new( BOOL_OR, 0 );
    boolean_node_set_children( left, 
                               (struct ast_node*) sub_left_node,
                               (struct ast_node*) sub_right_node );
                               
    b_node = boolean_node_new( BOOL_AND, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 6\n");
  }
  {
    char* left = "document";
    char* right = "three";
    int num_expect = 1;
    location_info_t expect[] = {{1,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_NOT, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 7\n");
  }
  {
    char* left = "test";
    char* right = "bob";
    int num_expect = 3;
    location_info_t expect[] = {{0,0},{1,0},{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_NOT, 0 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOCUMENTS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 8\n");
  }
  {
    char* left = "test";
    char* right = "this";
    int num_expect = 1;
    location_info_t expect[] = {{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_THEN, 6 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 9\n");
  }
  {
    char* left = "one";
    char* right = "test";
    int num_expect = 0;
    location_info_t expect[] = {};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_THEN, 20 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 10\n");
  }
  {
    char* left = "test";
    char* right = "this";
    int num_expect = 1;
    location_info_t expect[] = {{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_WITHIN, 6 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 11\n");
  }
  {
    char* left = "test";
    char* right = "this";
    int num_expect = 1;
    location_info_t expect[] = {{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_WITHIN, 7 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 12\n");
  }
  {
    char* left = "test";
    char* right = "this";
    int num_expect = 2;
    location_info_t expect[] = {{0,0},{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_WITHIN, 8 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 13\n");
  }
  {
    char* left = "test";
    char* right = "hi";
    int num_expect = 2;
    location_info_t expect[] = {{0,1},{2,0}};
    string_t left_string;
    string_t right_string;
    struct string_node* left_node;
    struct string_node* right_node;
    struct boolean_node* b_node;
    results_query_t* q = NULL;
    
    left_string = construct_c_string(left);    
    right_string = construct_c_string(right);    
    
    left_node = string_node_new(left_string);
    right_node = string_node_new(right_string);
    
    b_node = boolean_node_new( BOOL_WITHIN, 7 );
    boolean_node_set_children( b_node,
                               (struct ast_node*) left_node,
                               (struct ast_node*) right_node);
                               
    // set up a query
    create_generic_ast_query(&q, NULL, loc, 100, RESULT_TYPE_DOC_OFFSETS,
                             (struct ast_node*) b_node);

    // call "run query"
    err = femto_run_query(&srv, (query_entry_t*) q);
    die_if_err(err);

    // now, check the results we got out of it.
    check_results( &q->results, num_expect, expect );

    free_ast_node((struct ast_node*) b_node);
    destroy_generic_ast_query(q);

    printf("Passed Test 14\n");
  }
  
  free(doc_lens);
  femto_stop_server(&srv);
}

int main(int argc, char** argv)
{
  test_boolean_search();

  printf("All query tests PASSED\n");
}

