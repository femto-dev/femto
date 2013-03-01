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

  femto/src/main/nfa_test.c
*/
#include "nfa.h"
#include "string.h"
#include <assert.h>

void test_collapsing(void)
{
  error_t err;
  // step 0: test upper_triangular_index
  {
    assert(upper_triangular_index(1,0,1)==0);

    assert(upper_triangular_index(2,0,2)==0);
    assert(upper_triangular_index(2,0,1)==1);
    assert(upper_triangular_index(2,1,2)==2);

    assert(upper_triangular_index(3,0,3)==0);
    assert(upper_triangular_index(3,0,2)==1);
    assert(upper_triangular_index(3,1,3)==2);
    assert(upper_triangular_index(3,0,1)==3);
    assert(upper_triangular_index(3,1,2)==4);
    assert(upper_triangular_index(3,2,3)==5);

    assert(upper_triangular_index(4,0,4)==0);
    assert(upper_triangular_index(4,0,3)==1);
    assert(upper_triangular_index(4,1,4)==2);
    assert(upper_triangular_index(4,0,2)==3);
    assert(upper_triangular_index(4,1,3)==4);
    assert(upper_triangular_index(4,2,4)==5);
    assert(upper_triangular_index(4,0,1)==6);
    assert(upper_triangular_index(4,1,2)==7);
    assert(upper_triangular_index(4,2,3)==8);
    assert(upper_triangular_index(4,3,4)==9);
  }
  // step 1: test renumbering all transitions.
  {
    nfa_description_t nfa;
    err = init_nfa_description(&nfa, 3);
    set_bit(nfa.bit_array_len, nfa.start_states_set, 0);
    set_bit(nfa.bit_array_len, nfa.start_states_set, 2);
    set_bit(nfa.bit_array_len, nfa.final_states_set, 2);
    nfa.nodes[0].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa.nodes[0].num_transitions = 2;
    nfa.nodes[0].transitions[0].destination = 1;
    nfa.nodes[0].transitions[0].character = CHARACTER_OFFSET + 'a';
    nfa.nodes[0].transitions[1].destination = 1;
    nfa.nodes[0].transitions[1].character = CHARACTER_OFFSET + 'a';
    nfa.nodes[1].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa.nodes[1].num_transitions = 2;
    nfa.nodes[1].transitions[0].destination = 1;
    nfa.nodes[1].transitions[0].character = CHARACTER_OFFSET + 'b';
    nfa.nodes[1].transitions[1].destination = 2;
    nfa.nodes[1].transitions[1].character = CHARACTER_OFFSET + 'b';
    nfa.nodes[2].transitions = malloc (3*sizeof(nfa_transition_t));
    nfa.nodes[2].num_transitions = 3;
    nfa.nodes[2].transitions[0].destination = 0;
    nfa.nodes[2].transitions[0].character = CHARACTER_OFFSET + 'b';
    nfa.nodes[2].transitions[1].destination = 1;
    nfa.nodes[2].transitions[1].character = CHARACTER_OFFSET + 'b';
    nfa.nodes[2].transitions[2].destination = 2;
    nfa.nodes[2].transitions[2].character = CHARACTER_OFFSET + 'c';

    // replace all 1s with 0s
    renumber_all_transitions(&nfa, 1, 0);
    assert(nfa.nodes[0].transitions[0].destination == 0);
    assert(nfa.nodes[0].transitions[1].destination == 0);
    assert(nfa.nodes[1].transitions[0].destination == 0);
    assert(nfa.nodes[1].transitions[1].destination == 2);
    assert(nfa.nodes[2].transitions[0].destination == 0);
    assert(nfa.nodes[2].transitions[1].destination == 0);
    assert(nfa.nodes[2].transitions[2].destination == 2);

    printf("TEST: renumber transitions passed!\n");


    err = remove_inacessible_states(&nfa);
    die_if_err(err);

    assert( nfa.num_nodes == 2 );
    assert( nfa.nodes[0].num_transitions == 1 );
    assert( nfa.nodes[0].transitions[0].character == CHARACTER_OFFSET + 'a');
    assert( nfa.nodes[0].transitions[0].destination == 0 );
    assert( nfa.nodes[1].num_transitions == 2 );
    assert( nfa.nodes[1].transitions[0].character == CHARACTER_OFFSET + 'b');
    assert( nfa.nodes[1].transitions[0].destination == 0 );
    assert( nfa.nodes[1].transitions[1].character == CHARACTER_OFFSET + 'c');
    assert( nfa.nodes[1].transitions[1].destination == 1 );

    free_nfa_description(&nfa);

    printf("TEST: remove inaccessible  passed!\n");
  }
  {
    // try out the state-collapsing algorithm.
    // this NFA is 
    // >(0) -- a --> ((1))
    // >(2) -- a --> ((3))
    nfa_description_t nfa;
    err = init_nfa_description(&nfa, 4);
    set_bit(nfa.bit_array_len, nfa.start_states_set, 0);
    set_bit(nfa.bit_array_len, nfa.start_states_set, 2);
    set_bit(nfa.bit_array_len, nfa.final_states_set, 1);
    set_bit(nfa.bit_array_len, nfa.final_states_set, 3);
    nfa.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa.nodes[0].num_transitions = 1;
    nfa.nodes[0].transitions[0].destination = 1;
    nfa.nodes[0].transitions[0].character = CHARACTER_OFFSET + 'a';
    nfa.nodes[2].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa.nodes[2].num_transitions = 1;
    nfa.nodes[2].transitions[0].destination = 3;
    nfa.nodes[2].transitions[0].character = CHARACTER_OFFSET + 'a';

    // collapse the NFA!
    err = collapse_nfa(&nfa);
    die_if_err(err);

    assert(nfa.num_nodes == 2);
    assert(nfa.nodes[0].num_transitions = 1);
    assert(nfa.nodes[0].transitions[0].destination == 1);
    assert(nfa.nodes[0].transitions[0].character == CHARACTER_OFFSET + 'a');
    assert(nfa.nodes[1].num_transitions = 1);
    assert(get_bit(nfa.bit_array_len, nfa.start_states_set, 0) == 1);
    assert(get_bit(nfa.bit_array_len, nfa.start_states_set, 1) == 0);
    assert(get_bit(nfa.bit_array_len, nfa.final_states_set, 0) == 0);
    assert(get_bit(nfa.bit_array_len, nfa.final_states_set, 1) == 1);

    free_nfa_description(&nfa);
  }
}

int main ()
{
  printf("\n");
  error_t theError;
  // An empty NFA
  {
    nfa_description_t nfa0;
    theError = init_nfa_description(&nfa0, 1);
    
    if (theError != ERR_NOERR) { return theError; }
    
    set_bit (1, nfa0.start_states_set, 0);
    set_bit (1, nfa0.final_states_set, 0);
    assert (get_bit (1, nfa0.start_states_set, 0));
    
    uint64_t* array_out0 = allocate_bit_array (1);
    
    get_reachable_states (&nfa0, nfa0.start_states_set, '0', array_out0);

    assert ( bit_array_check_empty (1, array_out0));

    free_bit_array (array_out0);
   
    free_nfa_description(&nfa0);

    printf("TEST: An Empty NFA         passed!\n");
  }


  // A simple NFA
  {
    nfa_description_t nfa1;
    theError = init_nfa_description(&nfa1, 2);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa1.nodes[0].num_transitions = 1;
    nfa1.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa1.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';  
    nfa1.nodes[0].transitions[0].destination = 1;
      
    set_bit (nfa1.bit_array_len, nfa1.start_states_set, 0);
    set_bit (nfa1.bit_array_len, nfa1.final_states_set, 1);

    uint64_t* array_out1 = allocate_bit_array(1);

    get_reachable_states (&nfa1, nfa1.start_states_set,
			  CHARACTER_OFFSET + 'a', array_out1);

    assert ( get_bit(1, array_out1, 1));

    free_bit_array (array_out1);
    free_nfa_description(&nfa1);
            
    printf("TEST: A Simple NFA         passed!\n");
  }

  
  // An NFA of 123
  {
    nfa_description_t nfa2;
    theError = init_nfa_description(&nfa2, 4);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa2.nodes[0].num_transitions = 1;
    nfa2.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa2.nodes[0].transitions[0].character = CHARACTER_OFFSET  + '1';  
    nfa2.nodes[0].transitions[0].destination = 1;

    nfa2.nodes[1].num_transitions = 1;
    nfa2.nodes[1].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa2.nodes[1].transitions[0].character = CHARACTER_OFFSET + '2';
    nfa2.nodes[1].transitions[0].destination = 2;

    nfa2.nodes[2].num_transitions = 1;
    nfa2.nodes[2].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa2.nodes[2].transitions[0].character = CHARACTER_OFFSET + '3';
    nfa2.nodes[2].transitions[0].destination = 3;
    
    set_bit (nfa2.bit_array_len, nfa2.start_states_set, 0);
    set_bit (nfa2.bit_array_len, nfa2.final_states_set, 3);

    uint64_t* array_out2 = allocate_bit_array(1);
    uint64_t* array_in2  = allocate_bit_array(1);

    get_reachable_states (&nfa2, nfa2.start_states_set,
			  CHARACTER_OFFSET + '1', array_out2);
    
    assert ( get_bit(1, array_out2, 1));

    memcpy(array_in2, array_out2, 1*sizeof(uint64_t));
    get_reachable_states (&nfa2, array_in2,
			  CHARACTER_OFFSET + '2', array_out2);
    
    assert ( get_bit(1, array_out2, 2));
    
    memcpy(array_in2, array_out2, 1*sizeof(uint64_t));
    get_reachable_states (&nfa2, array_in2,
			  CHARACTER_OFFSET + '3', array_out2);

    assert ( get_bit(1, array_out2, 3));

    free_bit_array (array_out2);
    free_bit_array (array_in2);
    free_nfa_description(&nfa2);
            
    printf("TEST: An NFA of 123        passed!\n");
  }

  
  // An NFA with OR 1
  {
    nfa_description_t nfa3;
    theError = init_nfa_description(&nfa3, 3);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa3.nodes[0].num_transitions = 2;
    nfa3.nodes[0].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa3.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';
    nfa3.nodes[0].transitions[0].destination = 1;
    nfa3.nodes[0].transitions[1].character = CHARACTER_OFFSET  + 'b';
    nfa3.nodes[0].transitions[1].destination = 2;
      
    set_bit (nfa3.bit_array_len, nfa3.start_states_set, 0);
    set_bit (nfa3.bit_array_len, nfa3.final_states_set, 1);
    set_bit (nfa3.bit_array_len, nfa3.final_states_set, 2);

    uint64_t* array_out3 = allocate_bit_array(1);

    get_reachable_states (&nfa3, nfa3.start_states_set,
			  CHARACTER_OFFSET + 'a', array_out3);

    assert ( get_bit(1, array_out3, 1));

    free_bit_array (array_out3);
    free_nfa_description(&nfa3);
            
    printf("TEST: A first NFA w/ OR    passed!\n");
  }

  
  // An NFA with OR 2
  {
    nfa_description_t nfa4;
    theError = init_nfa_description(&nfa4, 3);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa4.nodes[0].num_transitions = 2;
    nfa4.nodes[0].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa4.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';
    nfa4.nodes[0].transitions[0].destination = 1;
    nfa4.nodes[0].transitions[1].character = CHARACTER_OFFSET  + 'b';
    nfa4.nodes[0].transitions[1].destination = 2;
      
    set_bit (nfa4.bit_array_len, nfa4.start_states_set, 0);
    set_bit (nfa4.bit_array_len, nfa4.final_states_set, 1);
    set_bit (nfa4.bit_array_len, nfa4.final_states_set, 2);

    uint64_t* array_out4 = allocate_bit_array(1);

    get_reachable_states (&nfa4, nfa4.start_states_set,
			  CHARACTER_OFFSET + 'b', array_out4);

    assert ( get_bit(1, array_out4, 2));

    free_bit_array (array_out4);
    free_nfa_description(&nfa4);
            
    printf("TEST: Another NFA w/ OR    passed!\n");
  }

  
  // A repeat NFA
  {
    nfa_description_t nfa5;
    theError = init_nfa_description(&nfa5, 1);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa5.nodes[0].num_transitions = 1;
    nfa5.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa5.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';
    nfa5.nodes[0].transitions[0].destination = 0;
      
    set_bit (nfa5.bit_array_len, nfa5.start_states_set, 0);
    set_bit (nfa5.bit_array_len, nfa5.final_states_set, 0);

    uint64_t* array_out5 = allocate_bit_array(1);
    uint64_t* array_in5  = allocate_bit_array(1);
      
    get_reachable_states (&nfa5, nfa5.start_states_set,
			  CHARACTER_OFFSET + 'b', array_out5);

    assert ( !get_bit(1, array_out5, 0));

    set_bit (nfa5.bit_array_len, array_out5, 0);
    
    for (int i = 0; i < 10; i++) {
      memcpy(array_in5, array_out5, 1*sizeof(uint64_t));
      get_reachable_states (&nfa5, array_in5,
			    CHARACTER_OFFSET + 'a', array_out5);

      assert ( get_bit(1, array_out5, 0));
    }

    free_bit_array (array_out5);
    free_bit_array (array_in5);
    free_nfa_description(&nfa5);
            
    printf("TEST: A repeating NFA      passed!\n");
  }
  
  // A complex NFA
  {
    nfa_description_t nfa6;
    theError = init_nfa_description(&nfa6, 3);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa6.nodes[0].num_transitions = 1;
    nfa6.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa6.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';
    nfa6.nodes[0].transitions[0].destination = 1;
    nfa6.nodes[1].num_transitions = 2;
    nfa6.nodes[1].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa6.nodes[1].transitions[0].character = CHARACTER_OFFSET  + 'b';
    nfa6.nodes[1].transitions[0].destination = 1;
    nfa6.nodes[1].transitions[1].character = CHARACTER_OFFSET  + 'c';
    nfa6.nodes[1].transitions[1].destination = 2;
      
    set_bit (nfa6.bit_array_len, nfa6.start_states_set, 0);
    set_bit (nfa6.bit_array_len, nfa6.final_states_set, 2);

    uint64_t* array_out6 = allocate_bit_array(1);
    uint64_t* array_in6  = allocate_bit_array(1);

    get_reachable_states (&nfa6, nfa6.start_states_set,
			  CHARACTER_OFFSET + 'a', array_out6);

    assert ( get_bit(1, array_out6, 1));

    memcpy(array_in6, array_out6, 1*sizeof(uint64_t));
    get_reachable_states (&nfa6, array_in6,
			  CHARACTER_OFFSET + 'c', array_out6);

    assert ( get_bit(1, array_out6, 2));
    
    free_bit_array (array_out6);
    free_bit_array (array_in6);
    free_nfa_description(&nfa6);
            
    printf("TEST: A complex NFA        passed!\n");
  }
  

  // complicated NFA
  {
    nfa_description_t nfa7;
    theError = init_nfa_description(&nfa7, 3);
    
    if (theError != ERR_NOERR) { return theError; }
    
    nfa7.nodes[0].num_transitions = 1;
    nfa7.nodes[0].transitions = malloc (1*sizeof(nfa_transition_t));
    nfa7.nodes[0].transitions[0].character = CHARACTER_OFFSET  + 'a';
    nfa7.nodes[0].transitions[0].destination = 1;
    nfa7.nodes[1].num_transitions = 2;
    nfa7.nodes[1].transitions = malloc (2*sizeof(nfa_transition_t));
    nfa7.nodes[1].transitions[0].character = CHARACTER_OFFSET  + 'b';
    nfa7.nodes[1].transitions[0].destination = 1;
    nfa7.nodes[1].transitions[1].character = CHARACTER_OFFSET  + 'c';
    nfa7.nodes[1].transitions[1].destination = 2;
      
    set_bit (nfa7.bit_array_len, nfa7.start_states_set, 0);
    set_bit (nfa7.bit_array_len, nfa7.final_states_set, 2);

    uint64_t* array_out7 = allocate_bit_array(1);
    uint64_t* array_in7  = allocate_bit_array(1);

    get_reachable_states (&nfa7, nfa7.start_states_set,
			  CHARACTER_OFFSET + 'a', array_out7);

    assert ( get_bit(1, array_out7, 1));

    for (int i = 0; i < 10; i++) {
      memcpy(array_in7, array_out7, 1*sizeof(uint64_t));
      get_reachable_states (&nfa7, array_in7,
			    CHARACTER_OFFSET + 'b', array_out7);
      
      assert ( get_bit(1, array_out7, 1));
    }
    
    memcpy(array_in7, array_out7, 1*sizeof(uint64_t));
    get_reachable_states (&nfa7, array_in7,
			  CHARACTER_OFFSET + 'c', array_out7);
    
    assert ( get_bit(1, array_out7, 2));
    
    free_bit_array (array_out7);
    free_bit_array (array_in7);
    free_nfa_description(&nfa7);
            
    printf("TEST: A complicated NFA    passed!\n");
  }

  test_collapsing();

  printf("\nAll NFA tests passed successfully!\n\n");
  
  return 0;
}
