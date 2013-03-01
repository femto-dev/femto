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

  femto/src/main/nfa_tool.c
*/
#include "index.h"
#include "nfa.h"
#include <stdarg.h>
#include <strings.h>

#define DEBUGGER   0 //debug variable (set to 0 for no messages)
#define ALPHA_NUM  (ALPHA_SIZE + 63)/64

void create_character_node (nfa_description_t *NFA, int node_num, int ch);
void create_epsilon_node (nfa_description_t *NFA, int node_num, int args, ...);


int main (int argc, char** argv)
{
  //wrong number of arguments
  if (argc <  2) {
    printf( "Usage: nfa_tool <text_to_search_through>\n"
	    " searches through text for a match with the NFA\n");
    return 0;
  }
  
  char* text = argv[1];
  int i, j, k, temp;
  int counter = 0;
  int num_nodes = 14;
  int number = (num_nodes + 63) / 64;
  int length = strlen(text);
  
  //array used to keep track of the different characters in the text
  uint64_t *alpha_array    = allocate_bit_array (ALPHA_NUM);
  //array of the sequential jumps that are not (only) sequential
  uint64_t *non_jumps      = allocate_bit_array(number);
  //array used as the bit array for  character in the text not in the NFA
  uint64_t *empty          = allocate_bit_array(number);
  //single array used for the states the NFA is currently in
  uint64_t *bit_array      = allocate_bit_array(number);
  //array used to redirect the program to the appropriate character array
  alpha_t  *redirect_array = malloc(ALPHA_SIZE*sizeof(alpha_t));
  //array of the bits arrays for each character in the NFA
  uint64_t **bit_arrays;
  
  //create NFA
  nfa_description_t the_NFA;
  the_NFA.num_nodes = num_nodes;
  the_NFA.nodes = malloc(num_nodes*sizeof(nfa_node_t));
  
  // initialize the nodes in bit_array
  // for the reg exp ba*(b|c)*bab
  create_character_node (&the_NFA, 0,  'b');
  create_epsilon_node   (&the_NFA, 1,   2, 2, 4);
  create_character_node (&the_NFA, 2,  'a');
  create_epsilon_node   (&the_NFA, 3,   1, 4);
  create_epsilon_node   (&the_NFA, 4,   2, 2, 5);
  create_epsilon_node   (&the_NFA, 5,   3, 6, 8, 10);
  create_character_node (&the_NFA, 6,  'b');
  create_epsilon_node   (&the_NFA, 7,   1, 10);
  create_character_node (&the_NFA, 8,  'c');
  create_epsilon_node   (&the_NFA, 9,   1, 10);
  create_character_node (&the_NFA, 10, 'b');
  create_character_node (&the_NFA, 11, 'a');
  create_character_node (&the_NFA, 12, 'b');
  create_character_node (&the_NFA, 13,  INVALID_ALPHA);
  
  for (i = 0; i < ALPHA_SIZE; i++)  {
    redirect_array[i] = INVALID_ALPHA;
  }
  
  // go through the nodes one-by-one
  for (i = 0; i < num_nodes; i++) {
    // if it's a character node
    if (the_NFA.nodes[i].character != INVALID_ALPHA) {
      set_bit (ALPHA_NUM, alpha_array, the_NFA.nodes[i].character);  }
  }
  
  //assign redirect_array and count unique characters
  for (i = 0; i < ALPHA_SIZE; i++) {
    if (get_bit(ALPHA_NUM, alpha_array, i) == 1) {
      redirect_array[i] = counter;
      counter++;
    }
  }
  bit_arrays = malloc(counter*sizeof(uint64_t*));
  
  // create the arrays
  for (i = 0; i < counter; i++) {
    uint64_t *temp_array = allocate_bit_array(number);
    set_bit (number, temp_array, 0); // all have a 1 in the first cell
    bit_arrays[i] = temp_array;
  }
  
  // make the masks
  for (i = 0; i < num_nodes; i++) {
    if (the_NFA.nodes[i].character != INVALID_ALPHA) {
      temp = redirect_array[ the_NFA.nodes[i].character ];
      set_bit (number, bit_arrays[temp], i+1);
    }
  }
    
  // the non jump mask
  set_bit (number, non_jumps, 0);
  for (i = 0; i < num_nodes; i++) {
    //if it's an epsilon node
    if (the_NFA.nodes[i].num_epsilon > 0) {
      // if the epsilon jump is consecutive
      if ((the_NFA.nodes[i].num_epsilon == 1) &&
	  (i+1 == the_NFA.nodes[i].epsilon_dst[0])) {
	set_bit (number, non_jumps, i+1); }
    }
    else {  //it's a character node
      set_bit (number, non_jumps, i+1); }
  }

  if (DEBUGGER == 5 || DEBUGGER == -1) {
    for (i = 0; i < counter; i++) {
      printf("\nTHE BIT ARRAY FOR %c\n", i);
      print_bit_array(number, bit_arrays[i], num_nodes);
      printf("\n\n");  }  }

  if (DEBUGGER >= 1) {
    printf("\nTHE BIT ARRAY FOR NON JUMPS\n");
    print_bit_array(number, non_jumps, num_nodes);
    printf("\n\n"); }
  
  ////////////////////////////////////////////////////////////////////
  // ** Set top bit to 1.                                           //
  // 1. epsilon transitions                                         //
  //   --> follow the current states to the end of any epsilon path //
  // 2. shift  (with fill)                                          //
  // 3. AND with S{x} (where 'x' is the character being looked at)  //
  // 4. mask out non-jumps                                          //
  //   --> (consecutive numbered states that aren't jumps)          //
  //   --> AND with 0's in the states where the jumps end           //
  ////////////////////////////////////////////////////////////////////

  //go through the text, letter by letter
  for (i = 0; i < length; i++) {
    set_bit (number, bit_array, 0);
    
    if (DEBUGGER >= 1) {
      printf("\nAFTER SETTING FIRST TO ONE:\n");
      print_bit_array(number, bit_array, num_nodes);
      printf("\n\n"); }
    
    //search for and follow epsilons
    for (j = 0; j < num_nodes; j++) {
      
      // if the state is set and it's an epsilon node
      if ( (get_bit(number, bit_array, j) == 1) &&
	   (the_NFA.nodes[j].num_epsilon > 0)) {
	clear_bit(number, bit_array, j);
	
	if (DEBUGGER >= 1) {
	  printf("\nAFTER BIT #%d HAS BEEN CLEARED:\n", j);
	  print_bit_array(number, bit_array, num_nodes);
	  printf("\n\n"); }
	
	//follow through with epsilons
	for (k = 0; k < the_NFA.nodes[j].num_epsilon; k++) {	  
	  //set each state to 1
	  set_bit (number, bit_array, the_NFA.nodes[j].epsilon_dst[k]);
	  
	  if (DEBUGGER >= 2) {
	    printf("\nAFTER BIT #%d INSERTED:\n",
		   the_NFA.nodes[j].epsilon_dst[k]);
	    print_bit_array(number, bit_array, num_nodes);
	    printf("\n\n"); }	  
	}
	
	if (DEBUGGER >= 1) {
	  printf("\nAFTER BIT #%d HAS BEEN COMPLETED:\n", j);
	  print_bit_array(number, bit_array, num_nodes);
	  printf("\n\n"); }
      }
    }
    
    if (DEBUGGER >= 1) {
      printf("\nBEFORE SHIFTING:\n");
      print_bit_array(number, bit_array, num_nodes);
      printf("\n\n"); }
    
    //shift
    bit_array_right_shift_one( number, bit_array);
    
    if (DEBUGGER >= 1) {
      printf("\nAFTER SHIFTING:\n");
      print_bit_array(number, bit_array, num_nodes);
      printf("\n\n"); }
    
    //AND with the current character
    if (redirect_array[(unsigned char)text[i]] != INVALID_ALPHA)  {
      bit_array_and(number, bit_array,
		    bit_arrays[redirect_array[(unsigned char)text[i]]],
		    bit_array);
    }
    //if it's not a character in the NFA, AND with an empty bit array
    else {
      bit_array_and(number, bit_array, empty, bit_array);
    }

    if (DEBUGGER >= 1) {
      printf("\nANDED WITH THE CURRENT CHARACTER:  %c\n", text[i]);
      print_bit_array(number, bit_array, num_nodes);
      printf("\n\n"); }
    
    //mask out the non-jumps
    bit_array_and(number, bit_array, non_jumps, bit_array);

    if (DEBUGGER >= 1) {
      printf("\nMASKED OUT THE NON-JUMPS:\n");
      print_bit_array(number, bit_array, num_nodes);
      printf("\n\n"); }
    
    //check to see if done
    if ( get_bit (number, bit_array, num_nodes - 1) == 1) {
      printf ("\n A match was found ending at character number %d!", i+1);
    }
    else  {
      if (DEBUGGER >= 1) {
	printf("\nNOW:\n");
	print_bit_array(number, bit_array, num_nodes);
	printf("\n\n"); }
    }
  }  

  // free the various bit arrays
  for (i = 0; i < counter; i++) {
    free_bit_array( bit_arrays[i] );
  }  
  free_bit_array(bit_array);
  free_bit_array(non_jumps);
  free (empty);
  
  printf("\n\n");
  return 0;
}



void create_character_node (nfa_description_t *NFA, int node_num, int ch)
{
  NFA->nodes[node_num].character = ch;
  NFA->nodes[node_num].num_epsilon = 0;
  NFA->nodes[node_num].epsilon_dst = NULL;
}


void create_epsilon_node (nfa_description_t *NFA, int node_num, int args, ...)
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
