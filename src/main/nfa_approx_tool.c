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

  femto/src/main/nfa_approx_tool.c
*/
#include "index.h"
#include "nfa.h"
#include "nfa_approx_tool.h"
#include "compile_regexp.h"
#include <stdarg.h>
#include <strings.h>

#define DEBUGGER   0 //debug variable (set it to 0 for no messages)
#define ALPHA_NUM  (ALPHA_SIZE + 63)/64

int main (int argc, char** argv)
{
  if (! check_args(argc, argv) ) {
    return 0;
  }
  
  char* text = argv[1];
  char* pattern = argv[2];
  int errors = atoi(argv[3]);
  int SuperTestMode = 0;
  
  if ( (argc == 5) && ( 0 == strcmp("-t", argv[4])) ) {
    SuperTestMode = 1; }

  int num_matches = 0;
  int length = strlen(text);
  
  //create NFA
  thompson_nfa_description_t the_NFA;
  
  //use the regular expression to create the NFA
  error_t error = compile_regexp_from_string_thompson(&the_NFA, pattern);
  die_if_err(error);

  nfa_state state;
  int counter = init_nfa_state ( &the_NFA, &state, errors);
  
  print_thompson_nfa(stdout, &the_NFA);
  
  printf("\nSearching for the pattern \"%s\" in the"
	 " text \"%s\" with %d errors\n\n",
	 pattern, text, errors);

  match matches;
  
  make_matches (&the_NFA, length, text, &state, num_matches, &matches, errors);
  
  free_nfa_state ( &state, counter, errors);
  
  free_thompson_nfa_description(&the_NFA);
  
  
  printf("\n\n");
  return 0;
}


void create_character_node (thompson_nfa_description_t *NFA, int node_num, int ch)
{
  NFA->nodes[node_num].character = ch;
  NFA->nodes[node_num].num_epsilon = 0;
  NFA->nodes[node_num].epsilon_dst = NULL;
}


void create_epsilon_node (thompson_nfa_description_t *NFA, int node_num, int args, ...)
{
  va_list ap;
  int i;
  
  NFA->nodes[node_num].character = INVALID_ALPHA;
  NFA->nodes[node_num].num_epsilon = args;
  
  NFA->nodes[node_num].epsilon_dst = malloc(args*sizeof(int));
  
  va_start(ap, args);
  
  for (i = 0; i < args; i++) {
    NFA->nodes[node_num].epsilon_dst[i] = va_arg(ap, int);
  }
  va_end(ap);
}

void do_epsilon_transitions (thompson_nfa_description_t *NFA,
			     uint64_t *array, int number)
{
  int i, j;
  
  // for each of the nodes
  for (i = 0; i < NFA->num_nodes; i++) {
    // if the state is set and is an epsilon node
    if(  (get_bit(number, array, i) == 1)  &&
	 (NFA->nodes[i].num_epsilon > 0)) {
      //clear the bit 
      clear_bit(number, array, i);
      
      if (DEBUGGER >= 1) {
	printf("\nAFTER BIT #%d HAS BEEN CLEARED:\n", i);
	print_bit_array(number, array, NFA->num_nodes);
	printf("\n\n"); }
      
      //follow through the epsilons, 
      for (j = 0; j < NFA->nodes[i].num_epsilon; j++) {
	//setting each destination state to 1
	set_bit (number, array, NFA->nodes[i].epsilon_dst[j]);
	
	if (DEBUGGER >= 2) {
	  printf("\nAFTER BIT #%d INSERTED:\n",
		 NFA->nodes[i].epsilon_dst[j]);
	  print_bit_array(number, array, NFA->num_nodes);
	  printf("\n\n"); }      }
    }
  }
  if (DEBUGGER >= 1) {
    printf("\nAFTER BIT #%d HAS BEEN COMPLETED:\n", i-1);
    print_bit_array(number, array, NFA->num_nodes);
    printf("\n\n"); }  
}

int check_args (int argc, char** argv)
{
  
  //wrong number of arguments
  if ((argc <  4) || (argc > 5)) {
    printf( "Usage: nfa_tool <text_to_search_through> <\"regular expression\">"
	    " <number of errors> [-t for testing]\n"
	    " searches through the text for a match with the NFA, "
	    "allowing errors\n");
    return 0;
  }
  else {
    if (atoi(argv[3]) < 0 ) {
      printf("ERROR: The number of errors must be greater"
	     " than or equal to 0.\n\n");
      return 0;
    }
    else {
      return 1;
    }
  }
}


int init_nfa_state (thompson_nfa_description_t* nfa, nfa_state* state, int errors)
{
  int i;
  int counter = 0;
  int number = (nfa->num_nodes + 63) / 64;
  state->non_jumps = allocate_bit_array(number);
  state->empty     = allocate_bit_array(number);
  
  state->alpha_array     = allocate_bit_array (ALPHA_NUM);
  state->redirect_array  = malloc(ALPHA_SIZE*sizeof(alpha_t));
  state->error_arrays    = malloc((errors+1)*sizeof(uint64_t*));
  state->previous_arrays = malloc(errors*sizeof(uint64_t*));
  
  //initialize redirect array to INVALID_ALPHA for all elements
  for (i = 0; i < ALPHA_SIZE; i++)  {
    state->redirect_array[i] = INVALID_ALPHA;
  }

  // go through the nodes one-by-one
  for (i = 0; i < nfa->num_nodes; i++) {
    // if it's a character node
    if (nfa->nodes[i].character != INVALID_ALPHA) {
      // set alpha_array to recognize there is an instance of that character
      set_bit (ALPHA_NUM, state->alpha_array, nfa->nodes[i].character);
    }
  }
  
  //assign redirect_array and count unique characters
  for (i = 0; i < ALPHA_SIZE; i++) {
    // if an instance of that character exists
    if (get_bit(ALPHA_NUM, state->alpha_array, i) == 1) {
      state->redirect_array[i] = counter;
      counter++;
    }
  }

  // allocate the proper space for the character_arrays
  state->character_arrays = malloc(counter*sizeof(uint64_t*));

  // create the arrays
  for(i = 0; i < errors+1; i++) {
    uint64_t *temp = allocate_bit_array(number);
    state->error_arrays[i] = temp;
  }
  for(i = 0; i < errors; i++) {
    uint64_t *temp = allocate_bit_array(number);
    state->previous_arrays[i] = temp;
  }  
  // all the character arrays have a 1 in the first cell
  for (i = 0; i < counter; i++) {
    uint64_t *temp = allocate_bit_array(number);
    set_bit (number, temp, 0);
    state->character_arrays[i] = temp;
  }
  
  // make the masks for each of the nodes/characters
  for (i = 0; i < nfa->num_nodes; i++) {
    // if the node is a character node
    if (nfa->nodes[i].character != INVALID_ALPHA) {
      // set the bit for that specific state, in the character array
      // (indicated by nfa.nodes[i].character)
      set_bit (number,
	       state->character_arrays[state->redirect_array[ nfa->nodes[i].
							      character ]],
	       i+1);
    }
  }
  
  // the non jump mask
  set_bit (number, state->non_jumps, 0);
  for (i = 0; i < nfa->num_nodes; i++) {
    //if it's an epsilon node
    if (nfa->nodes[i].num_epsilon > 0) {
      // if the epsilon jump is only consecutive
      if ((nfa->nodes[i].num_epsilon == 1) &&
	  (i+1 == nfa->nodes[i].epsilon_dst[0])) {
	// make the jump valid
	set_bit (number, state->non_jumps, i+1);
      }
    }
    else {  //it's a character node
      // make the jump valid
      set_bit (number, state->non_jumps, i+1);
    }
  }
  return counter;
  
}





int make_matches (thompson_nfa_description_t *NFA, int input_len,
		   unsigned char* input, nfa_state* state,
		   int num_matches, match* matches, int errors) {
  
  int ch, d, j, k;
  int number = (NFA->num_nodes + 63) / 64;
  
  /*************************************************/
  /* -- Set top d+1 bits.                          */
  /* 1. follow epsilon transitions                 */
  /* 2. right shift (with fill)                    */
  /* 3. AND with S{x}                              */
  /* 4. if d > 0 (errors possible)                 */
  /*   --> R[d-1][j-1] OR  R[d-1][j]               */
  /*       ** previous letter with one error less  */
  /*       ** this letter with one error less      */
  /*   --> follow epsilon transitions for result   */
  /*   --> right shift (with fill)                 */
  /*   --> R[d-1][j-1] OR result                   */
  /*   --> result OR original                      */
  /* 5. mask out non-jumps                         */
  /*************************************************/
  
  //go through the text, letter by letter
  for (j = 0; j < input_len; j++) {
    
    //go through each of the error arrays
    for (d = 0; d < errors+1; d++) {
      //set top d+1 bits
      for ( k = 0; k < d+1; k++) {
	set_bit (number, state->error_arrays[d], k);
      }
      
      if (DEBUGGER >= 1) {
	printf("\nAFTER SETTING FIRST TO ONE for %d errors:\n", d);
	print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	printf("\n\n"); }
      
      // follow epsilon transitions for the current error array
      do_epsilon_transitions (NFA, state->error_arrays[d], number);      
      
      if (DEBUGGER >= 1) {
	printf("\nBEFORE SHIFTING:\n");
	print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	printf("\n\n"); }

      //check to see if done
      if ( get_bit (number, state->error_arrays[d], NFA->num_nodes - 1) == 1) {
	printf ("\n A match was found for %d errors, "
		"ending at character number %d!", d, j);
	num_matches++;
      }
      else {
	if (DEBUGGER >= 1) {
	  printf("\nNOW:\n");
	  print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	  printf("\n\n"); }	
      }
      
      //shift each error_array one bit right and fill
      bit_array_right_shift_one(number, state->error_arrays[d]);
      
      if (DEBUGGER >= 1) {
	printf("\nAFTER SHIFTING:\n");
	print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	printf("\n\n"); }
      
      //AND with the current character of the text, if it's a character node
      ch = CHARACTER_OFFSET + (unsigned char)input[j];
      if (state->redirect_array[ch] != INVALID_ALPHA)  {
	bit_array_and(number, state->error_arrays[d],
		      state->character_arrays[state->redirect_array[ch]],
		      state->error_arrays[d]);
      }
      //if it's not a character in the NFA, AND it with an empty bit array
      else {
	bit_array_and(number, state->error_arrays[d],
		      state->empty, state->error_arrays[d]);
      }
      
      if (DEBUGGER >= 1) {
	printf("\nANDED WITH THE CURRENT CHARACTER:  %c\n", input[j]);
	print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	printf("\n\n"); }
      
      
      // if the number of errors (d) is greater than 0 (AKA, allowing errors)
      if (d > 0) {
	// R[d-1][j-1] OR  R[d-1][j]
	uint64_t *result = allocate_bit_array(number);
	bit_array_or(number, result, state->error_arrays[d-1],
		     state->previous_arrays[d-1]);
	
	// follow epsilon transitions for result
	do_epsilon_transitions (NFA, result, number);
	
	// right shift (with fill)
	bit_array_right_shift_one(number, result);
	
	// R[d-1][j] OR result
	bit_array_or(number, result, state->previous_arrays[d-1], result);
	
	// result OR original
	bit_array_or(number, state->error_arrays[d],
		     result, state->error_arrays[d]);
	
      	free(result);  //no longer needed
      }
      
      //mask out the non-jumps
      /*
      bit_array_and(number, state->error_arrays[d],
		    state->non_jumps, state->error_arrays[d]);
      */
      
      if (DEBUGGER >= 1) {
	printf("\nMASKED OUT THE NON-JUMPS:\n");
	print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
	printf("\n\n"); }
            
    } //done going through each of the error arrays
    
    for (d = 0; d < errors; d++) {
      //store the results
      for (k = 0; k < number; k++) {
	state->previous_arrays[d][k] = state->error_arrays[d][k];
      }
    }
  }  //done going through each of the chracters in the text  
  
  // if no matches found, say so
  if (num_matches == 0) {
    printf("\n No matches found");
  }

  //final check
  
  // follow epsilon transitions for the current error array
  do_epsilon_transitions (NFA, state->error_arrays[d], number);
  
  //check to see if done
  if ( get_bit (number, state->error_arrays[d], NFA->num_nodes - 1) == 1) {
    printf ("\n A match was found for %d errors, "
	    "ending at character number %d!", d, j);
    num_matches++;
  }
  else {
    if (DEBUGGER >= 1) {
      printf("\nNOW:\n");
      print_bit_array(number, state->error_arrays[d], NFA->num_nodes);
      printf("\n\n"); }	
  }  
  return num_matches;
}

void free_nfa_state (nfa_state* state, int counter, int errors)
{
  int i;       
  
  // free the various bit arrays
  for (i = 0; i < counter; i++) {
    free_bit_array( state->character_arrays[i] );
  }
  for (i = 0; i < errors+1; i++) {
    free_bit_array( state->error_arrays[i] );
  }
  for (i = 0; i < errors; i++) {
    free_bit_array( state->previous_arrays[i] );
  }
  
  
  free_bit_array(state->alpha_array);
  free_bit_array(state->non_jumps);
  free_bit_array(state->empty);
  free(state->character_arrays);
  free(state->previous_arrays);
  free(state->error_arrays);
}
